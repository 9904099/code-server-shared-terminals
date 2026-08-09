#!/usr/bin/env python3
"""Measure direct Unix-socket PTY broker latency and bulk throughput."""

from __future__ import annotations

import argparse
import json
import socket
import statistics
import struct
import subprocess
import tempfile
import time
from pathlib import Path

HEADER = struct.Struct("!cI")
SIZE = struct.Struct("!HH")
SCRIPT = Path(__file__).with_name("pty_broker.py")


def frame(kind: bytes, payload: bytes = b"") -> bytes:
    return HEADER.pack(kind, len(payload)) + payload


def receive_frame(connection: socket.socket) -> tuple[bytes, bytes]:
    header = bytearray()
    while len(header) < HEADER.size:
        chunk = connection.recv(HEADER.size - len(header))
        if not chunk:
            raise ConnectionError("broker closed before frame header")
        header.extend(chunk)
    kind, length = HEADER.unpack(header)
    payload = bytearray()
    while len(payload) < length:
        chunk = connection.recv(length - len(payload))
        if not chunk:
            raise ConnectionError("broker closed before frame payload")
        payload.extend(chunk)
    return kind, bytes(payload)


def status(socket_path: str) -> dict[str, int | str]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(2)
        connection.connect(socket_path)
        connection.sendall(frame(b"S"))
        kind, payload = receive_frame(connection)
        if kind != b"S":
            raise RuntimeError(f"unexpected status reply {kind!r}")
        return json.loads(payload)


def stop(socket_path: str, current: dict[str, int | str]) -> dict[str, int | str]:
    expected = {
        "taskId": current["taskId"],
        "brokerPid": current["brokerPid"],
        "brokerStartTicks": current["brokerStartTicks"],
        "instanceNonce": current["instanceNonce"],
        "controlSocketDev": current["controlSocketDev"],
        "controlSocketIno": current["controlSocketIno"],
    }
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(2)
        connection.connect(socket_path)
        connection.sendall(frame(b"K", json.dumps(expected, separators=(",", ":")).encode()))
        kind, payload = receive_frame(connection)
    if kind != b"K":
        raise RuntimeError(f"unexpected stop reply {kind!r}")
    acknowledgement = json.loads(payload)
    if acknowledgement != expected:
        raise RuntimeError("stop acknowledgement identity mismatch")
    return acknowledgement


class Client:
    def __init__(self, socket_path: str) -> None:
        self.connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.connection.settimeout(5)
        self.connection.connect(socket_path)
        self.incoming = bytearray()
        self.output = bytearray()

    def send(self, kind: bytes, payload: bytes = b"") -> None:
        self.connection.sendall(frame(kind, payload))

    def wait_output(self, marker: bytes) -> int:
        start = len(self.output)
        while marker not in self.output[start:]:
            data = self.connection.recv(65536)
            if not data:
                raise ConnectionError("broker closed")
            self.incoming.extend(data)
            while len(self.incoming) >= HEADER.size:
                kind, length = HEADER.unpack(self.incoming[: HEADER.size])
                if length > 1024 * 1024 or len(self.incoming) < HEADER.size + length:
                    break
                payload = bytes(self.incoming[HEADER.size:HEADER.size + length])
                del self.incoming[:HEADER.size + length]
                if kind == b"O":
                    self.output.extend(payload)
        return len(self.output) - start

    def close(self) -> None:
        self.connection.close()


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * quantile)))
    return ordered[index]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--bulk-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--require-cgroup", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.samples < 10 or args.samples > 10000:
        raise ValueError("samples must be between 10 and 10000")
    if args.bulk_bytes < 1024 or args.bulk_bytes > 8 * 1024 * 1024:
        raise ValueError("bulk-bytes must be between 1024 and 8388608")

    with tempfile.TemporaryDirectory(prefix="pty-broker-benchmark-") as directory:
        socket_path = str(Path(directory) / "broker.sock")
        control_socket_path = str(Path(directory) / "broker.control.sock")
        command = [
            "python3", str(SCRIPT), "serve",
            "--socket", socket_path,
            "--control-socket", control_socket_path,
            "--cwd", directory,
            "--shell", "/bin/sh",
            "--task-id", "benchmark",
            "--replay-bytes", "0",
            "--max-clients", "1",
            "--max-client-output-bytes", "16777216",
        ]
        if args.require_cgroup:
            command.append("--require-cgroup")
        broker = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 5
        while not Path(socket_path).exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        client: Client | None = None
        result: dict[str, object] = {}
        stop_error: Exception | None = None
        stop_acknowledged = False
        emergency_cleanup = False
        try:
            client = Client(socket_path)
            client.send(b"A")
            client.send(b"R", SIZE.pack(24, 80))
            client.send(b"I", b"stty -echo; printf '__READY__\\n'\n")
            client.wait_output(b"__READY__")

            latencies_ms: list[float] = []
            for index in range(args.samples):
                marker = f"__PING_{index:05d}__".encode()
                started = time.perf_counter_ns()
                client.send(b"I", b"printf '" + marker + b"\\n'\n")
                client.wait_output(marker)
                latencies_ms.append((time.perf_counter_ns() - started) / 1_000_000)

            bulk_marker = b"__BULK_DONE__"
            command = (
                "python3 -c \"import sys;sys.stdout.write('x'*"
                f"{args.bulk_bytes})\"; printf '{bulk_marker.decode()}\\n'\n"
            ).encode()
            started = time.perf_counter_ns()
            before = len(client.output)
            client.send(b"I", command)
            client.wait_output(bulk_marker)
            elapsed = (time.perf_counter_ns() - started) / 1_000_000_000
            received = len(client.output) - before
            result = {
                "samples": args.samples,
                "latencyMs": {
                    "p50": round(percentile(latencies_ms, 0.50), 3),
                    "p95": round(percentile(latencies_ms, 0.95), 3),
                    "p99": round(percentile(latencies_ms, 0.99), 3),
                    "mean": round(statistics.fmean(latencies_ms), 3),
                    "max": round(max(latencies_ms), 3),
                },
                "bulkRequestedBytes": args.bulk_bytes,
                "bulkReceivedBytes": received,
                "bulkSeconds": round(elapsed, 4),
                "bulkMiBPerSecond": round(received / elapsed / 1024 / 1024, 2),
            }
        finally:
            if client is not None:
                client.close()
            try:
                current = status(control_socket_path)
                result["observedProcessBoundary"] = current.get("processBoundary", "unknown")
                required_boundary = "cgroup-v2" if args.require_cgroup else "subreaper"
                if current.get("processBoundary") != required_boundary:
                    raise RuntimeError("broker process boundary did not match the requested mode")
                stop(control_socket_path, current)
                stop_acknowledged = True
                broker.wait(timeout=5)
            except Exception as error:
                stop_error = error
            if broker.poll() is None:
                emergency_cleanup = True
                broker.terminate()
                try:
                    broker.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    broker.kill()
                    broker.wait(timeout=3)
        if stop_error is not None:
            raise RuntimeError("identity-bound graceful stop failed") from stop_error
        if broker.returncode != 0:
            raise RuntimeError(f"broker exited non-zero: {broker.returncode}")
        result["stopAcknowledged"] = stop_acknowledged
        result["emergencyCleanup"] = emergency_cleanup
        print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
