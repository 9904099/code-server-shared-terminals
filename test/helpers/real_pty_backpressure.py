#!/usr/bin/env python3
"""Real PTY/Broker backpressure regression harness; emits one non-sensitive JSON summary."""

from __future__ import annotations

import json
import os
import pty
import re
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HEADER = struct.Struct("!cI")
SIZE = struct.Struct("!HH")
MIB = 1024 * 1024
OUTPUT_MARKER = b"__REAL_BROKER_PTY_OUTPUT_RESUMED__"
INPUT_MARKER = b"__REAL_BROKER_PTY_INPUT_RESUMED__"


def encode_frame(kind: bytes, payload: bytes = b"") -> bytes:
    return HEADER.pack(kind, len(payload)) + payload


def receive_frame(connection: socket.socket) -> tuple[bytes, bytes]:
    header = bytearray()
    while len(header) < HEADER.size:
        chunk = connection.recv(HEADER.size - len(header))
        if not chunk:
            raise RuntimeError("protocol connection closed before frame header")
        header.extend(chunk)
    kind, length = HEADER.unpack(header)
    payload = bytearray()
    while len(payload) < length:
        chunk = connection.recv(length - len(payload))
        if not chunk:
            raise RuntimeError("protocol connection closed before frame payload")
        payload.extend(chunk)
    return kind, bytes(payload)


def connect_when_ready(path: str, timeout: float = 5.0) -> socket.socket:
    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            connection.connect(path)
            return connection
        except OSError as error:
            last_error = error
            connection.close()
            time.sleep(0.02)
    raise RuntimeError(f"socket did not become ready: {path}: {last_error}")


def control_status(path: str) -> dict[str, object]:
    connection = connect_when_ready(path)
    connection.settimeout(2.0)
    try:
        connection.sendall(encode_frame(b"S"))
        kind, payload = receive_frame(connection)
        if kind != b"S":
            raise RuntimeError(f"unexpected status frame: {kind!r}")
        return json.loads(payload)
    finally:
        connection.close()


def integer_field(values: dict[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"expected integer status field: {key}")
    return value


def identity_stop(path: str, status: dict[str, object]) -> None:
    expected = {
        "taskId": status["taskId"],
        "brokerPid": status["brokerPid"],
        "brokerStartTicks": status["brokerStartTicks"],
        "instanceNonce": status["instanceNonce"],
        "controlSocketDev": status["controlSocketDev"],
        "controlSocketIno": status["controlSocketIno"],
    }
    connection = connect_when_ready(path)
    connection.settimeout(3.0)
    try:
        connection.sendall(encode_frame(b"K", json.dumps(expected, separators=(",", ":")).encode()))
        kind, payload = receive_frame(connection)
        acknowledgement = json.loads(payload)
        if kind != b"K" or acknowledgement != expected:
            raise RuntimeError("identity-bound stop acknowledgement mismatch")
    finally:
        connection.close()


def wait_for(check, label: str, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise RuntimeError(f"timed out waiting for {label}")


class DataReader:
    def __init__(self, connection: socket.socket) -> None:
        self.connection = connection
        self.bytes = 0
        self.tail = bytearray()
        self.error: BaseException | None = None
        self.closed = False
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        incoming = bytearray()
        try:
            while True:
                chunk = self.connection.recv(65536)
                if not chunk:
                    self.closed = True
                    return
                incoming.extend(chunk)
                while len(incoming) >= HEADER.size:
                    kind, length = HEADER.unpack(incoming[: HEADER.size])
                    if len(incoming) < HEADER.size + length:
                        break
                    payload = bytes(incoming[HEADER.size:HEADER.size + length])
                    del incoming[:HEADER.size + length]
                    if kind == b"O":
                        self.bytes += len(payload)
                        self.tail.extend(payload)
                        del self.tail[:-8192]
        except BaseException as error:  # surfaced to the parent test
            self.error = error

    def contains(self, marker: bytes) -> bool:
        return marker in self.tail



def drain_master(master_fd: int, marker: bytes, timeout: float = 10.0) -> bool:
    os.set_blocking(master_fd, False)
    tail = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        readable, _, _ = select.select([master_fd], [], [], 0.1)
        if not readable:
            continue
        try:
            chunk = os.read(master_fd, 65536)
        except BlockingIOError:
            continue
        except OSError:
            return marker in tail
        if not chunk:
            return marker in tail
        tail.extend(chunk)
        del tail[:-8192]
        if marker in tail:
            return True
    return False


def main() -> int:
    if len(sys.argv) != 2:
        raise RuntimeError("expected the pty_broker.py path")
    broker_script = str(Path(sys.argv[1]).resolve())
    summary: dict[str, object] = {
        "outputBytes": 0,
        "pausedAttachedClients": -1,
        "pausedQueuedClientOutputBytes": -1,
        "slowClientsDelta": -1,
        "attachIdentityStable": False,
        "outputMarkerAfterResume": False,
        "inputMarkerAfterResume": False,
        "attachExitCode": None,
        "brokerExitCode": None,
        "emergencyCleanup": False,
    }
    broker: subprocess.Popen[bytes] | None = None
    attach: subprocess.Popen[bytes] | None = None
    direct: socket.socket | None = None
    reader: DataReader | None = None
    master_fd = -1
    stopped = False
    with tempfile.TemporaryDirectory(prefix="real-pty-backpressure-") as directory:
        data_path = str(Path(directory) / "broker.sock")
        control_path = str(Path(directory) / "broker.control.sock")
        broker = subprocess.Popen(
            [
                sys.executable,
                broker_script,
                "serve",
                "--socket", data_path,
                "--control-socket", control_path,
                "--cwd", directory,
                "--shell", "/bin/sh",
                "--task-id", "real-pty-backpressure",
                "--replay-bytes", "65536",
                "--max-clients", "2",
                "--max-client-output-bytes", str(2 * MIB),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            control_status(control_path)
            direct = connect_when_ready(data_path)
            direct.sendall(encode_frame(b"A"))
            direct.sendall(encode_frame(b"R", SIZE.pack(24, 80)))
            reader = DataReader(direct)
            reader.start()

            master_fd, slave_fd = pty.openpty()
            attach = subprocess.Popen(
                [
                    sys.executable,
                    broker_script,
                    "attach",
                    "--socket", data_path,
                    "--max-output-bytes", str(100 * MIB),
                ],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            os.close(slave_fd)
            attach_pid = attach.pid
            wait_for(
                lambda: integer_field(control_status(control_path), "attachedClients") == 2,
                "two real attached clients",
            )
            before = control_status(control_path)
            direct.sendall(encode_frame(b"I", b"stty -echo\n"))
            time.sleep(0.2)
            reader.tail.clear()
            output_command = (
                b"python3 -c 'import os; os.write(1, b\"x\" * (4 * 1024 * 1024) "
                b"+ b\"__REAL_BROKER_PTY_OUTPUT_RESUMED__\")'\n"
            )
            direct.sendall(encode_frame(b"I", output_command))
            wait_for(lambda: reader.contains(OUTPUT_MARKER), "direct client output marker", timeout=12.0)
            wait_for(
                lambda: integer_field(control_status(control_path), "queuedClientOutputBytes") == 0,
                "broker output queue return to zero",
            )
            paused = control_status(control_path)
            summary["outputBytes"] = reader.bytes
            summary["pausedAttachedClients"] = paused["attachedClients"]
            summary["pausedQueuedClientOutputBytes"] = paused["queuedClientOutputBytes"]
            summary["slowClientsDelta"] = (
                integer_field(paused, "slowClients") - integer_field(before, "slowClients")
            )
            summary["attachIdentityStable"] = attach.pid == attach_pid and attach.poll() is None
            process_status = Path(f"/proc/{attach.pid}/status").read_text()
            rss_match = re.search(r"^VmRSS:\s+(\d+)\s+kB$", process_status, re.MULTILINE)
            if rss_match is None:
                raise RuntimeError("attach VmRSS is missing")
            summary["pausedAttachRssBytes"] = int(rss_match.group(1)) * 1024

            summary["outputMarkerAfterResume"] = drain_master(master_fd, OUTPUT_MARKER)
            direct_tail_before = reader.bytes
            os.write(master_fd, b"printf '__REAL_BROKER_PTY_INPUT_RESUMED__\\n'\n")
            wait_for(
                lambda: reader.contains(INPUT_MARKER) and reader.bytes > direct_tail_before,
                "input round trip after PTY resume",
            )
            summary["inputMarkerAfterResume"] = True

            current = control_status(control_path)
            identity_stop(control_path, current)
            stopped = True
            summary["brokerExitCode"] = broker.wait(timeout=6.0)
            drain_master(master_fd, b"__NO_SUCH_MARKER__", timeout=0.5)
            summary["attachExitCode"] = attach.wait(timeout=6.0)
            if reader.error is not None:
                raise reader.error
            assert broker.stderr is not None
            assert attach.stderr is not None
            broker_stderr = broker.stderr.read().decode("utf-8", "replace")
            attach_stderr = attach.stderr.read().decode("utf-8", "replace")
            summary["brokerStderrEmpty"] = broker_stderr == ""
            summary["attachStderrEmpty"] = attach_stderr == ""
        finally:
            if direct is not None:
                direct.close()
            if master_fd >= 0:
                os.close(master_fd)
            if attach is not None and attach.poll() is None:
                attach.kill()
                attach.wait()
                summary["emergencyCleanup"] = True
            if broker is not None and broker.poll() is None:
                if not stopped:
                    try:
                        identity_stop(control_path, control_status(control_path))
                        stopped = True
                    except BaseException:
                        pass
                try:
                    broker.wait(timeout=6.0)
                except subprocess.TimeoutExpired:
                    broker.kill()
                    broker.wait()
                    summary["emergencyCleanup"] = True

    passed = (
        integer_field(summary, "outputBytes") >= 4 * MIB
        and summary["pausedAttachedClients"] == 2
        and summary["pausedQueuedClientOutputBytes"] == 0
        and summary["slowClientsDelta"] == 0
        and summary["attachIdentityStable"] is True
        and summary["outputMarkerAfterResume"] is True
        and summary["inputMarkerAfterResume"] is True
        and summary["attachExitCode"] == 0
        and summary["brokerExitCode"] == 0
        and summary.get("brokerStderrEmpty") is True
        and summary.get("attachStderrEmpty") is True
        and summary["emergencyCleanup"] is False
    )
    summary["passed"] = passed
    print(json.dumps(summary, separators=(",", ":")))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
