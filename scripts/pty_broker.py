#!/usr/bin/env python3
"""Small Linux PTY broker used by code-server-shared-terminals."""

from __future__ import annotations

import argparse
import collections
import ctypes
import errno
import fcntl
import json
import os
import pty
import re
import selectors
import secrets
import signal
import socket
import stat
import struct
import sys
import termios
import time
import tty
from dataclasses import dataclass, field
from contextlib import contextmanager
from pathlib import Path

VERSION = "0.3.0"
HEADER = struct.Struct("!cI")
SIZE = struct.Struct("!HH")
MAX_FRAME_BYTES = 1024 * 1024
DEFAULT_MAX_CLIENT_INPUT_BYTES = 256 * 1024
DEFAULT_MAX_CLIENT_OUTPUT_BYTES = 2 * 1024 * 1024
DEFAULT_MAX_PTY_INPUT_BYTES = 256 * 1024
MAX_EVENT_BATCH = 128
MAX_FRAMES_PER_READ = 32
MAX_PENDING_DATA_CLIENTS = 2
MAX_CONTROL_CLIENTS = 8
MAX_CONTROL_OUTPUT_BYTES = 64 * 1024
MAX_ACCEPT_BATCH = 64
MAX_BROKER_BUFFER_BYTES = 64 * 1024 * 1024
LOOP_IDLE_TIMEOUT_SECONDS = 0.05
PR_SET_CHILD_SUBREAPER = 36
AT_FDCWD = -100
RENAME_NOREPLACE = 1



def encode_frame(kind: bytes, payload: bytes = b"") -> bytes:
    return HEADER.pack(kind, len(payload)) + payload


@dataclass(eq=False)
class Client:
    sock: socket.socket
    input_buffer: bytearray = field(default_factory=bytearray)
    output_buffer: "ChunkQueue" = field(default_factory=lambda: ChunkQueue(0))
    attached: bool = False
    rows: int = 24
    columns: int = 80
    close_after_flush: bool = False
    stop_after_flush: bool = False
    control: bool = False
    connected_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    ppid: int
    session: int
    start_ticks: int


class ChunkQueue:
    """A bounded FIFO byte queue without hot-path front deletion."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.chunks: collections.deque[bytes] = collections.deque()
        self.offset = 0
        self.size = 0

    def append(self, data: bytes) -> bool:
        if not data:
            return True
        if self.size + len(data) > self.limit:
            return False
        self.chunks.append(data)
        self.size += len(data)
        return True

    def peek(self) -> memoryview:
        if not self.chunks:
            return memoryview(b"")
        return memoryview(self.chunks[0])[self.offset:]

    def consume(self, count: int) -> None:
        while count > 0 and self.chunks:
            available = len(self.chunks[0]) - self.offset
            consumed = min(count, available)
            self.offset += consumed
            self.size -= consumed
            count -= consumed
            if self.offset == len(self.chunks[0]):
                self.chunks.popleft()
                self.offset = 0

    def clear(self) -> None:
        self.chunks.clear()
        self.offset = 0
        self.size = 0


class ReplayBuffer:
    """A bounded chunk ring; trimming copies only the boundary chunk."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.chunks: collections.deque[bytes] = collections.deque()
        self.size = 0

    def append(self, data: bytes) -> None:
        if self.limit == 0 or not data:
            return
        if len(data) >= self.limit:
            self.chunks.clear()
            self.chunks.append(data[-self.limit:])
            self.size = self.limit
            return
        self.chunks.append(data)
        self.size += len(data)
        overflow = self.size - self.limit
        while overflow > 0 and self.chunks:
            first = self.chunks[0]
            if len(first) <= overflow:
                self.chunks.popleft()
                self.size -= len(first)
                overflow -= len(first)
            else:
                self.chunks[0] = first[overflow:]
                self.size -= overflow
                overflow = 0

    def snapshot(self) -> bytes:
        return b"".join(self.chunks)


class Broker:
    def __init__(
        self,
        socket_path: str,
        cwd: str,
        shell: str,
        task_id: str,
        replay_bytes: int,
        max_clients: int,
        max_client_input_bytes: int,
        max_client_output_bytes: int,
        max_pty_input_bytes: int,
        control_socket_path: str | None = None,
        require_cgroup: bool = False,
        cgroup_mount: str = "/sys/fs/cgroup",
        instance_nonce: str | None = None,
        startup_gate_fd: int | None = None,
    ) -> None:
        self.socket_path = socket_path
        self.control_socket_path = control_socket_path or f"{socket_path}.control"
        self.cwd = cwd
        self.shell = shell
        self.task_id = task_id
        if instance_nonce is not None and not re.fullmatch(r"[a-f0-9]{64}", instance_nonce):
            raise ValueError("Instance nonce must be 64 lowercase hexadecimal characters")
        self.instance_nonce = instance_nonce or secrets.token_hex(32)
        self.startup_gate_fd = startup_gate_fd
        self.replay = ReplayBuffer(replay_bytes)
        self.max_clients = max_clients
        self.max_client_input_bytes = max_client_input_bytes
        self.max_client_output_bytes = max_client_output_bytes
        self.require_cgroup = require_cgroup
        self.cgroup_mount = Path(cgroup_mount)
        self.cgroup_path: Path | None = None
        self.cgroup_relative_path = ""
        self.selector = selectors.DefaultSelector()
        self.data_listener: socket.socket | None = None
        self.control_listener: socket.socket | None = None
        self.master_fd = -1
        self.child_pid = -1
        self.child_start_ticks = -1
        self.child_session = -1
        self.child_reaped = False
        self.broker_pid = os.getpid()
        self.broker_start_ticks = self._process_start_ticks()
        self.socket_identities: dict[str, tuple[int, int]] = {}
        self.clients: set[Client] = set()
        self.pending_clients: set[Client] = set()
        self.controller: Client | None = None
        self.pty_input = ChunkQueue(max_pty_input_bytes)
        self.stopping = False
        self.started_at = time.monotonic()
        self.last_activity = time.time()
        self.last_loop = time.monotonic()
        self.max_event_loop_lag_ms = 0.0
        self.input_bytes = 0
        self.output_bytes = 0
        self.disconnected_clients = 0
        self.rejected_clients = 0
        self.slow_clients = 0
        self.cleanup_ok = True

    def run(self) -> int:
        try:
            self._wait_startup_gate()
            self._enable_subreaper()
            self._prepare_cgroup()
            self._start_shell()
            self._start_listeners()
            signal.signal(signal.SIGTERM, self._request_stop)
            signal.signal(signal.SIGINT, self._request_stop)
            signal.signal(signal.SIGHUP, self._request_stop)
            while not self.stopping:
                requested_timeout = 0 if self.pending_clients else LOOP_IDLE_TIMEOUT_SECONDS
                select_started = time.monotonic()
                selected = self.selector.select(timeout=requested_timeout)
                select_finished = time.monotonic()
                self.max_event_loop_lag_ms = max(
                    self.max_event_loop_lag_ms,
                    max(0.0, select_finished - select_started - requested_timeout) * 1000,
                )
                selected.sort(key=lambda item: self._event_priority(item[0].data))
                for key, events in selected[:MAX_EVENT_BATCH]:
                    if key.data == "data-listener":
                        self._accept(self.data_listener, control=False)
                    elif key.data == "control-listener":
                        self._accept(self.control_listener, control=True)
                    elif key.data == "pty":
                        if events & selectors.EVENT_READ:
                            self._read_pty()
                        if events & selectors.EVENT_WRITE:
                            self._write_pty()
                    else:
                        client = key.data
                        if events & selectors.EVENT_READ:
                            self._read_client(client)
                        if client in self.clients and events & selectors.EVENT_WRITE:
                            self._write_client(client)
                for client in list(self.pending_clients)[:MAX_EVENT_BATCH]:
                    self.pending_clients.discard(client)
                    if client in self.clients:
                        self._process_client_frames(client)
                self._expire_unattached_clients(time.monotonic())
                self._reap_children()
                loop_finished = time.monotonic()
                self.max_event_loop_lag_ms = max(
                    self.max_event_loop_lag_ms,
                    (loop_finished - select_finished) * 1000,
                )
                self.last_loop = loop_finished
                if self.child_reaped:
                    self.stopping = True
        finally:
            self._cleanup()
        return 0 if self.cleanup_ok else 1

    def _wait_startup_gate(self) -> None:
        descriptor = self.startup_gate_fd
        if descriptor is None:
            return
        self.startup_gate_fd = None
        try:
            if os.read(descriptor, 1) != b"1":
                raise RuntimeError("Broker startup gate closed before durable identity publication")
        finally:
            os.close(descriptor)

    def _request_stop(self, _signum: int, _frame: object) -> None:
        self.stopping = True

    @staticmethod
    def _event_priority(data: object) -> int:
        if isinstance(data, Client) and data.control:
            return 0
        if data == "control-listener":
            return 1
        if isinstance(data, Client):
            return 2
        if data == "pty":
            return 3
        return 4

    @staticmethod
    def _enable_subreaper() -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
            error_number = ctypes.get_errno()
            raise OSError(error_number, os.strerror(error_number))

    @staticmethod
    def _ensure_private_directory(path: Path) -> None:
        try:
            info = path.lstat()
        except FileNotFoundError:
            path.mkdir(mode=0o700, parents=True)
            info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise RuntimeError(f"Unix socket directory is not a real directory: {path}")
        if info.st_uid != os.geteuid():
            raise RuntimeError(f"Unix socket directory is not owned by uid {os.geteuid()}: {path}")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise RuntimeError(f"Unix socket directory must not be accessible by group/other: {path}")

    @staticmethod
    def _socket_is_live(path: str) -> bool:
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.settimeout(0.2)
        try:
            probe.connect(path)
            return True
        except FileNotFoundError:
            return False
        except ConnectionRefusedError:
            return False
        finally:
            probe.close()

    @classmethod
    def _prepare_socket_path(cls, path: str) -> None:
        encoded_path = os.fsencode(path)
        if len(encoded_path) >= 104:
            raise ValueError(f"Unix socket path is too long: {path}")
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid():
            raise RuntimeError(f"Refusing to replace untrusted Unix socket path: {path}")
        raise RuntimeError(f"Refusing to reuse an existing Unix socket path: {path}")

    @classmethod
    @contextmanager
    def _socket_namespace_locks(cls, paths: tuple[str, ...]):
        descriptors: list[int] = []
        try:
            directories = sorted({str(Path(path).parent) for path in paths})
            for directory in directories:
                lock_path = str(Path(directory) / ".shared-terminals.socket.lock")
                flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                descriptor = os.open(lock_path, flags, 0o600)
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                    os.close(descriptor)
                    raise RuntimeError(f"Unix socket namespace lock is not trusted: {lock_path}")
                os.fchmod(descriptor, 0o600)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                descriptors.append(descriptor)
            yield
        finally:
            for descriptor in reversed(descriptors):
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    @staticmethod
    def _socket_path_info(path: str) -> os.stat_result:
        errors: list[OSError] = []
        if hasattr(os, "O_PATH"):
            descriptor = -1
            try:
                flags = os.O_PATH | os.O_CLOEXEC
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                descriptor = os.open(path, flags)
                return os.fstat(descriptor)
            except OSError as error:
                errors.append(error)
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
        for inspect in (
            lambda: os.lstat(path),
            lambda: os.stat(path, follow_symlinks=False),
        ):
            try:
                return inspect()
            except OSError as error:
                errors.append(error)
        raise errors[-1]

    def _record_bound_socket_identity(self, path: str) -> None:
        try:
            info = self._socket_path_info(path)
        except OSError as error:
            raise RuntimeError(f"Unable to record bound Unix socket identity: {path}") from error
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid():
            raise RuntimeError(f"Bound Unix socket identity is not trusted: {path}")
        self.socket_identities[path] = (info.st_dev, info.st_ino)

    def _bind_listener(self, path: str, selector_data: str) -> socket.socket:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(path)
            self._record_bound_socket_identity(path)
            os.chmod(path, 0o600)
            listener.listen(128)
            listener.setblocking(False)
            self.selector.register(listener, selectors.EVENT_READ, selector_data)
            return listener
        except Exception:
            try:
                self.selector.unregister(listener)
            except Exception:
                pass
            listener.close()
            self._unlink_owned_socket_locked(path)
            raise

    def _start_listeners(self) -> None:
        directories = {Path(self.socket_path).parent, Path(self.control_socket_path).parent}
        for directory in directories:
            self._ensure_private_directory(directory)
        with self._socket_namespace_locks((self.socket_path, self.control_socket_path)):
            self._prepare_socket_path(self.socket_path)
            self._prepare_socket_path(self.control_socket_path)
            self.control_listener = self._bind_listener(self.control_socket_path, "control-listener")
            self.data_listener = self._bind_listener(self.socket_path, "data-listener")

    def _start_shell(self) -> None:
        gate_read, gate_write = os.pipe2(os.O_CLOEXEC)
        pid, master_fd = pty.fork()
        if pid == 0:
            os.close(gate_write)
            released = os.read(gate_read, 1)
            os.close(gate_read)
            if released != b"1":
                os._exit(125)
            os.chdir(self.cwd)
            environment = os.environ.copy()
            environment.setdefault("TERM", "xterm-256color")
            environment.setdefault("COLORTERM", "truecolor")
            environment.setdefault("TERM_PROGRAM", "vscode")
            os.execvpe(self.shell, [f"-{os.path.basename(self.shell)}"], environment)
        os.close(gate_read)
        self.child_pid = pid
        self.master_fd = master_fd
        try:
            identity = self._read_process_identity(pid)
            if identity is None:
                raise RuntimeError("Unable to record shell process identity")
            self.child_start_ticks = identity.start_ticks
            self.child_session = identity.session
            if self.require_cgroup:
                self._move_shell_to_cgroup(pid)
            os.set_blocking(master_fd, False)
            self.selector.register(master_fd, selectors.EVENT_READ, "pty")
            os.write(gate_write, b"1")
        finally:
            os.close(gate_write)

    @staticmethod
    def _self_cgroup_relative_path() -> str:
        for line in Path("/proc/self/cgroup").read_text().splitlines():
            if line.startswith("0::"):
                relative = line[3:]
                if relative.startswith("/") and ".." not in Path(relative).parts:
                    return relative
        raise RuntimeError("Unable to determine the current cgroup v2 path")

    def _prepare_cgroup(self) -> None:
        if not self.require_cgroup:
            return
        if not self.task_id or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.task_id):
            raise RuntimeError("A safe task ID is required for cgroup containment")
        if not (self.cgroup_mount / "cgroup.controllers").is_file():
            raise RuntimeError("cgroup v2 is not mounted")
        parent_relative = self._self_cgroup_relative_path()
        parent = self.cgroup_mount / parent_relative.lstrip("/")
        if not parent.is_dir() or not os.access(parent, os.W_OK):
            raise RuntimeError(
                f"Current service cgroup is not delegated to uid {os.geteuid()}: {parent}"
            )
        cgroup_name = f"shared-terminal-{self.task_id}"
        target = parent / cgroup_name
        try:
            target.mkdir()
        except FileExistsError as error:
            raise RuntimeError(f"Refusing to reuse an existing terminal cgroup: {target}") from error
        self.cgroup_path = target
        self.cgroup_relative_path = f"{parent_relative.rstrip('/')}/{cgroup_name}"
        if not (target / "cgroup.procs").is_file() or not (target / "cgroup.kill").is_file():
            raise RuntimeError(f"Delegated cgroup lacks required control files: {target}")

    def _move_shell_to_cgroup(self, pid: int) -> None:
        if self.cgroup_path is None:
            raise RuntimeError("Required terminal cgroup was not prepared")
        with (self.cgroup_path / "cgroup.procs").open("w") as destination:
            destination.write(f"{pid}\n")
        actual = ""
        for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
            if line.startswith("0::"):
                actual = line[3:]
                break
        if actual != self.cgroup_relative_path:
            raise RuntimeError(
                f"Shell did not enter its task cgroup: expected={self.cgroup_relative_path} actual={actual}"
            )

    def _accept(self, listener: socket.socket | None, control: bool) -> None:
        assert listener is not None
        accept_limit = 1 if control else MAX_ACCEPT_BATCH
        for _ in range(accept_limit):
            try:
                connection, _ = listener.accept()
            except BlockingIOError:
                return
            connection.setblocking(False)
            channel_clients = [client for client in self.clients if client.control is control]
            if control:
                if len(channel_clients) >= MAX_CONTROL_CLIENTS:
                    oldest = min(channel_clients, key=lambda client: client.connected_at)
                    self._close_client(oldest)
            elif len(channel_clients) >= self.max_clients + MAX_PENDING_DATA_CLIENTS:
                self.rejected_clients += 1
                connection.close()
                continue
            client = Client(
                connection,
                output_buffer=ChunkQueue(
                    MAX_CONTROL_OUTPUT_BYTES if control else self.max_client_output_bytes
                ),
                control=control,
            )
            self.clients.add(client)
            self.selector.register(connection, selectors.EVENT_READ, client)
            self.last_activity = time.time()

    def _read_pty(self) -> None:
        try:
            data = os.read(self.master_fd, 65536)
        except BlockingIOError:
            return
        except OSError as error:
            if error.errno == errno.EIO:
                self.stopping = True
                return
            raise
        if not data:
            self.stopping = True
            return
        self.replay.append(data)
        self.output_bytes += len(data)
        self.last_activity = time.time()
        for client in list(self.clients):
            if client.attached:
                self._queue(client, b"O", data)

    def _write_pty(self) -> None:
        if self.pty_input.size == 0:
            self._update_pty_events()
            return
        try:
            written = os.write(self.master_fd, self.pty_input.peek())
        except BlockingIOError:
            return
        except OSError as error:
            if error.errno == errno.EIO:
                self.stopping = True
                return
            raise
        self.pty_input.consume(written)
        self._update_pty_events()

    def _read_client(self, client: Client) -> None:
        try:
            data = client.sock.recv(65536)
        except BlockingIOError:
            return
        except ConnectionError:
            self._close_client(client)
            return
        if not data:
            self._close_client(client)
            return
        input_limit = min(self.max_client_input_bytes, 64 * 1024) if client.control else self.max_client_input_bytes
        if len(client.input_buffer) + len(data) > input_limit:
            self.rejected_clients += 1
            self._close_client(client)
            return
        client.input_buffer.extend(data)
        self._process_client_frames(client)

    def _process_client_frames(self, client: Client) -> None:
        frames = 0
        while len(client.input_buffer) >= HEADER.size and frames < MAX_FRAMES_PER_READ:
            kind, length = HEADER.unpack(client.input_buffer[: HEADER.size])
            if length > MAX_FRAME_BYTES:
                self._close_client(client)
                return
            frame_end = HEADER.size + length
            if len(client.input_buffer) < frame_end:
                return
            payload = bytes(client.input_buffer[HEADER.size:frame_end])
            del client.input_buffer[:frame_end]
            self._handle_frame(client, kind, payload)
            frames += 1
            if client not in self.clients:
                return
            if client.close_after_flush:
                client.input_buffer.clear()
                self.pending_clients.discard(client)
                return
        if len(client.input_buffer) >= HEADER.size:
            _kind, length = HEADER.unpack(client.input_buffer[: HEADER.size])
            if length <= MAX_FRAME_BYTES and len(client.input_buffer) >= HEADER.size + length:
                self.pending_clients.add(client)

    def _handle_frame(self, client: Client, kind: bytes, payload: bytes) -> None:
        if client.control:
            self._handle_control_frame(client, kind, payload)
            return
        if kind == b"A" and not payload:
            if not client.attached and sum(
                candidate.attached for candidate in self.clients if not candidate.control
            ) >= self.max_clients:
                self.rejected_clients += 1
                self._close_client(client)
                return
            client.attached = True
            if self.controller is None:
                self.controller = client
                self._apply_size(client)
            replay = self.replay.snapshot()
            if replay:
                self._queue(client, b"O", replay)
            return
        if kind == b"R" and client.attached and len(payload) == SIZE.size:
            rows, columns = SIZE.unpack(payload)
            if rows > 0 and columns > 0:
                client.rows = rows
                client.columns = columns
                if self.controller is client:
                    self._apply_size(client)
            return
        if kind == b"I" and client.attached:
            if payload:
                if not self.pty_input.append(payload):
                    self.rejected_clients += 1
                    self._close_client(client)
                    return
                if self.controller is not client:
                    self.controller = client
                    self._apply_size(client)
                self.input_bytes += len(payload)
                self.last_activity = time.time()
                self._update_pty_events()
            return
        self._close_client(client)

    def _handle_control_frame(self, client: Client, kind: bytes, payload: bytes) -> None:
        if client.close_after_flush:
            return
        if kind == b"P" and not payload:
            client.close_after_flush = True
            self._queue(client, b"P")
            return
        if kind == b"S" and not payload:
            client.close_after_flush = True
            status_payload = json.dumps(self._status(client), separators=(",", ":")).encode()
            self._queue(client, b"S", status_payload)
            return
        if kind == b"K":
            try:
                expected = json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError):
                expected = None
            control_identity = self.socket_identities.get(self.control_socket_path)
            try:
                current_control = os.lstat(self.control_socket_path)
                current_control_identity = (current_control.st_dev, current_control.st_ino)
            except FileNotFoundError:
                current_control_identity = None
            identity_matches = isinstance(expected, dict) \
                and expected.get("taskId") == self.task_id \
                and expected.get("brokerPid") == self.broker_pid \
                and expected.get("brokerStartTicks") == self.broker_start_ticks \
                and isinstance(expected.get("instanceNonce"), str) \
                and secrets.compare_digest(expected["instanceNonce"], self.instance_nonce) \
                and control_identity is not None \
                and current_control_identity == control_identity \
                and expected.get("controlSocketDev") == str(control_identity[0]) \
                and expected.get("controlSocketIno") == str(control_identity[1])
            client.close_after_flush = True
            if not identity_matches:
                self._queue(client, b"E", b"broker identity mismatch")
                return
            assert control_identity is not None
            acknowledgement = json.dumps({
                "taskId": self.task_id,
                "brokerPid": self.broker_pid,
                "brokerStartTicks": self.broker_start_ticks,
                "instanceNonce": self.instance_nonce,
                "controlSocketDev": str(control_identity[0]),
                "controlSocketIno": str(control_identity[1]),
            }, separators=(",", ":")).encode()
            client.stop_after_flush = True
            self._queue(client, b"K", acknowledgement)
            return
        self._close_client(client)

    def _expire_unattached_clients(self, now: float) -> None:
        for client in list(self.clients):
            timeout = 0.5 if client.control else 2.0
            if not client.attached and not client.close_after_flush and now - client.connected_at > timeout:
                self.rejected_clients += 1
                self._close_client(client)

    def _queue(self, client: Client, kind: bytes, payload: bytes = b"") -> None:
        if not client.output_buffer.append(encode_frame(kind, payload)):
            self.slow_clients += 1
            self._close_client(client)
            return
        self._update_client_events(client)

    def _write_client(self, client: Client) -> None:
        if client.output_buffer.size:
            try:
                written = client.sock.send(client.output_buffer.peek())
            except BlockingIOError:
                return
            except ConnectionError:
                self._close_client(client)
                return
            client.output_buffer.consume(written)
        if client.output_buffer.size == 0 and client.close_after_flush:
            if client.stop_after_flush:
                self.stopping = True
            self._close_client(client)
        elif client in self.clients:
            self._update_client_events(client)

    def _apply_size(self, client: Client) -> None:
        if self.master_fd < 0:
            return
        size = struct.pack("HHHH", client.rows, client.columns, 0, 0)
        fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, size)

    def _update_client_events(self, client: Client) -> None:
        if client not in self.clients:
            return
        events = 0 if client.close_after_flush else selectors.EVENT_READ
        if client.output_buffer.size:
            events |= selectors.EVENT_WRITE
        if events == 0:
            self._close_client(client)
            return
        self.selector.modify(client.sock, events, client)

    def _update_pty_events(self) -> None:
        if self.master_fd < 0:
            return
        events = selectors.EVENT_READ
        if self.pty_input.size:
            events |= selectors.EVENT_WRITE
        self.selector.modify(self.master_fd, events, "pty")

    def _close_client(self, client: Client) -> None:
        if client not in self.clients:
            return
        self.clients.remove(client)
        self.pending_clients.discard(client)
        self.disconnected_clients += 1
        try:
            self.selector.unregister(client.sock)
        except Exception:
            pass
        client.sock.close()
        if self.controller is client:
            self.controller = next((candidate for candidate in self.clients if candidate.attached), None)
            if self.controller is not None:
                self._apply_size(self.controller)

    @staticmethod
    def _read_proc_status() -> tuple[int, int]:
        rss_bytes = 0
        threads = 1
        try:
            for line in Path("/proc/self/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    rss_bytes = int(line.split()[1]) * 1024
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
        except (FileNotFoundError, OSError, ValueError):
            pass
        return rss_bytes, threads

    @staticmethod
    def _fd_count() -> int:
        try:
            return len(list(Path("/proc/self/fd").iterdir()))
        except OSError:
            return -1

    @staticmethod
    def _process_start_ticks() -> int:
        try:
            stat_line = Path("/proc/self/stat").read_text()
            command_end = stat_line.rfind(")")
            return int(stat_line[command_end + 2:].split()[19])
        except (FileNotFoundError, OSError, ValueError, IndexError):
            return -1

    @staticmethod
    def _read_process_identity(pid: int) -> ProcessIdentity | None:
        try:
            stat_line = Path(f"/proc/{pid}/stat").read_text()
            command_end = stat_line.rfind(")")
            if command_end < 0:
                return None
            fields = stat_line[command_end + 2:].split()
            return ProcessIdentity(
                pid=pid,
                ppid=int(fields[1]),
                session=int(fields[3]),
                start_ticks=int(fields[19]),
            )
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError, IndexError):
            return None

    def _owned_processes(self) -> dict[int, ProcessIdentity]:
        process_table: dict[int, ProcessIdentity] = {}
        try:
            proc_entries = list(Path("/proc").iterdir())
        except OSError:
            return process_table
        for entry in proc_entries:
            if not entry.name.isdigit():
                continue
            identity = self._read_process_identity(int(entry.name))
            if identity is not None:
                process_table[identity.pid] = identity

        owned: set[int] = set()
        shell = process_table.get(self.child_pid)
        if shell is not None and shell.start_ticks == self.child_start_ticks:
            owned.add(shell.pid)
        for identity in process_table.values():
            if identity.pid == self.broker_pid or identity.start_ticks < self.child_start_ticks:
                continue
            if identity.ppid == self.broker_pid:
                owned.add(identity.pid)
        changed = True
        while changed:
            changed = False
            for identity in process_table.values():
                if identity.pid not in owned and identity.ppid in owned:
                    owned.add(identity.pid)
                    changed = True
        return {pid: process_table[pid] for pid in owned if pid in process_table}

    def _reap_children(self) -> None:
        while True:
            try:
                finished, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if finished <= 0:
                return
            if finished == self.child_pid:
                self.child_reaped = True

    def _signal_owned_processes(self, signum: int) -> None:
        for identity in self._owned_processes().values():
            current = self._read_process_identity(identity.pid)
            if current != identity:
                continue
            try:
                os.kill(identity.pid, signum)
            except ProcessLookupError:
                pass

    def _wait_for_owned_processes(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._reap_children()
            if not self._owned_processes():
                return True
            time.sleep(0.02)
        self._reap_children()
        return not self._owned_processes()

    def _status(self, requester: Client) -> dict[str, object]:
        rss_bytes, threads = self._read_proc_status()
        control_identity = self.socket_identities.get(self.control_socket_path, (-1, -1))
        cgroup_identity = (-1, -1)
        if self.cgroup_path is not None:
            cgroup_info = os.lstat(self.cgroup_path)
            cgroup_identity = (cgroup_info.st_dev, cgroup_info.st_ino)
        try:
            shell_pgid = os.getpgid(self.child_pid) if self.child_pid > 0 else -1
        except ProcessLookupError:
            shell_pgid = -1
        return {
            "version": VERSION,
            "taskId": self.task_id,
            "brokerPid": self.broker_pid,
            "brokerStartTicks": self.broker_start_ticks,
            "instanceNonce": self.instance_nonce,
            "controlSocketDev": str(control_identity[0]),
            "controlSocketIno": str(control_identity[1]),
            "shellPid": self.child_pid,
            "shellPgid": shell_pgid,
            "shellSession": self.child_session,
            "shellStartTicks": self.child_start_ticks,
            "ownedProcesses": len(self._owned_processes()),
            "processBoundary": "cgroup-v2" if self.cgroup_path is not None else "subreaper",
            "cgroupPath": self.cgroup_relative_path,
            "cgroupDev": str(cgroup_identity[0]),
            "cgroupIno": str(cgroup_identity[1]),
            "clients": sum(client is not requester and not client.control for client in self.clients),
            "attachedClients": sum(
                client is not requester and not client.control and client.attached for client in self.clients
            ),
            "replayBytes": self.replay.size,
            "ptyInputBytes": self.pty_input.size,
            "queuedClientOutputBytes": sum(
                client.output_buffer.size for client in self.clients if client is not requester and not client.control
            ),
            "rssBytes": rss_bytes,
            "fdCount": self._fd_count(),
            "threads": threads,
            "inputBytes": self.input_bytes,
            "outputBytes": self.output_bytes,
            "disconnectedClients": self.disconnected_clients,
            "rejectedClients": self.rejected_clients,
            "slowClients": self.slow_clients,
            "maxEventLoopLagMs": round(self.max_event_loop_lag_ms, 3),
            "lastActivityUnixMs": int(self.last_activity * 1000),
            "uptimeMs": int((time.monotonic() - self.started_at) * 1000),
        }

    def _cleanup(self) -> None:
        for client in list(self.clients):
            self._close_client(client)
        for listener in (self.data_listener, self.control_listener):
            if listener is None:
                continue
            try:
                self.selector.unregister(listener)
            except Exception:
                pass
            listener.close()
        self.data_listener = None
        self.control_listener = None
        if self.master_fd >= 0:
            try:
                self.selector.unregister(self.master_fd)
            except Exception:
                pass
            os.close(self.master_fd)
            self.master_fd = -1
        if self.cgroup_path is not None:
            cgroup_ok = self._terminate_cgroup_processes()
            fallback_ok = True if cgroup_ok else self._terminate_owned_processes()
            self.cleanup_ok = cgroup_ok and fallback_ok
        else:
            self.cleanup_ok = self._terminate_owned_processes()
        self.selector.close()
        try:
            with self._socket_namespace_locks((self.socket_path, self.control_socket_path)):
                self._unlink_owned_socket_locked(self.socket_path)
                self._unlink_owned_socket_locked(self.control_socket_path)
        except (OSError, RuntimeError):
            self.cleanup_ok = False

    def _unlink_owned_socket(self, path: str) -> None:
        try:
            with self._socket_namespace_locks((path,)):
                self._unlink_owned_socket_locked(path)
        except (OSError, RuntimeError):
            self.cleanup_ok = False

    def _unlink_owned_socket_locked(self, path: str) -> None:
        expected_identity = self.socket_identities.get(path)
        if expected_identity is None:
            return
        quarantine = f"{path}.remove-{os.getpid()}-{secrets.token_hex(12)}"
        try:
            first = self._socket_path_info(path)
            second = self._socket_path_info(path)
            if (
                stat.S_ISSOCK(first.st_mode)
                and stat.S_ISSOCK(second.st_mode)
                and (first.st_dev, first.st_ino) == expected_identity
                and (second.st_dev, second.st_ino) == expected_identity
            ):
                self._rename_noreplace(path, quarantine)
                isolated = self._socket_path_info(quarantine)
                if stat.S_ISSOCK(isolated.st_mode) and (isolated.st_dev, isolated.st_ino) == expected_identity:
                    os.unlink(quarantine)
                    return
                try:
                    self._rename_noreplace(quarantine, path)
                except OSError:
                    self.cleanup_ok = False
                    return
                self.cleanup_ok = False
        except FileNotFoundError:
            pass
        except OSError:
            self.cleanup_ok = False

    @staticmethod
    def _rename_noreplace(source: str, destination: str) -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None:
            raise OSError(errno.ENOSYS, "renameat2 is unavailable")
        renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        renameat2.restype = ctypes.c_int
        if renameat2(
            AT_FDCWD,
            os.fsencode(source),
            AT_FDCWD,
            os.fsencode(destination),
            RENAME_NOREPLACE,
        ) != 0:
            error_number = ctypes.get_errno()
            raise OSError(error_number, os.strerror(error_number), source, destination)

    def _terminate_owned_processes(self) -> bool:
        if self.child_pid <= 0:
            return True
        for signum, timeout in (
            (signal.SIGHUP, 0.5),
            (signal.SIGTERM, 0.5),
            (signal.SIGKILL, 1.0),
        ):
            if not self._owned_processes():
                self._reap_children()
                return True
            self._signal_owned_processes(signum)
            if self._wait_for_owned_processes(timeout):
                return True
        return not self._owned_processes()

    def _terminate_cgroup_processes(self) -> bool:
        target = self.cgroup_path
        if target is None:
            return not self.require_cgroup
        try:
            events_path = target / "cgroup.events"
            kill_path = target / "cgroup.kill"
            if not events_path.is_file() or not kill_path.is_file():
                target.rmdir()
                self.cgroup_path = None
                return False
            with kill_path.open("w") as control:
                control.write("1\n")
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                self._reap_children()
                events = dict(
                    line.split(maxsplit=1)
                    for line in events_path.read_text().splitlines()
                    if " " in line
                )
                if events.get("populated") == "0":
                    target.rmdir()
                    self.cgroup_path = None
                    return True
                time.sleep(0.02)
        except (FileNotFoundError, OSError, ValueError):
            return False
        return False


def receive_frame(connection: socket.socket) -> tuple[bytes, bytes]:
    header = bytearray()
    while len(header) < HEADER.size:
        chunk = connection.recv(HEADER.size - len(header))
        if not chunk:
            raise ConnectionError("broker closed before frame header")
        header.extend(chunk)
    kind, length = HEADER.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise ConnectionError("broker frame exceeds protocol limit")
    payload = bytearray()
    while len(payload) < length:
        chunk = connection.recv(length - len(payload))
        if not chunk:
            raise ConnectionError("broker closed before frame payload")
        payload.extend(chunk)
    return kind, bytes(payload)


def terminal_size() -> tuple[int, int]:
    try:
        size = os.get_terminal_size(sys.stdin.fileno())
        return size.lines, size.columns
    except OSError:
        return 24, 80


def run_attach(socket_path: str) -> int:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.connect(socket_path)
    connection.sendall(encode_frame(b"A"))
    rows, columns = terminal_size()
    connection.sendall(encode_frame(b"R", SIZE.pack(rows, columns)))
    saved_attributes = None
    if os.isatty(sys.stdin.fileno()):
        saved_attributes = termios.tcgetattr(sys.stdin.fileno())
        tty.setraw(sys.stdin.fileno())
    resized = False

    def mark_resized(_signum: int, _frame: object) -> None:
        nonlocal resized
        resized = True

    previous_winch = signal.signal(signal.SIGWINCH, mark_resized)
    selector = selectors.DefaultSelector()
    selector.register(connection, selectors.EVENT_READ, "socket")
    selector.register(sys.stdin.fileno(), selectors.EVENT_READ, "stdin")
    incoming = bytearray()
    try:
        while True:
            if resized:
                resized = False
                rows, columns = terminal_size()
                connection.sendall(encode_frame(b"R", SIZE.pack(rows, columns)))
            for key, _events in selector.select(timeout=0.25):
                if key.data == "stdin":
                    data = os.read(sys.stdin.fileno(), 65536)
                    if not data:
                        return 0
                    connection.sendall(encode_frame(b"I", data))
                else:
                    data = connection.recv(65536)
                    if not data:
                        return 0
                    incoming.extend(data)
                    if len(incoming) > MAX_FRAME_BYTES + HEADER.size:
                        return 1
                    while len(incoming) >= HEADER.size:
                        kind, length = HEADER.unpack(incoming[: HEADER.size])
                        if length > MAX_FRAME_BYTES:
                            return 1
                        if len(incoming) < HEADER.size + length:
                            break
                        payload = bytes(incoming[HEADER.size:HEADER.size + length])
                        del incoming[: HEADER.size + length]
                        if kind == b"O":
                            view = memoryview(payload)
                            while view:
                                written = os.write(sys.stdout.fileno(), view)
                                view = view[written:]
    finally:
        selector.close()
        connection.close()
        signal.signal(signal.SIGWINCH, previous_winch)
        if saved_attributes is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, saved_attributes)


def run_control(socket_path: str, kind: bytes, payload: bytes = b"", expect_reply: bool = True) -> int:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(1.0)
    try:
        connection.connect(socket_path)
        connection.sendall(encode_frame(kind, payload))
        if expect_reply:
            reply, _ = receive_frame(connection)
            if reply != kind:
                return 1
        return 0
    except (FileNotFoundError, ConnectionError, OSError):
        return 1
    finally:
        connection.close()


def run_stop(
    socket_path: str,
    task_id: str,
    broker_pid: int,
    broker_start_ticks: int,
    instance_nonce: str,
    control_socket_dev: str,
    control_socket_ino: str,
) -> int:
    expected = {
        "taskId": task_id,
        "brokerPid": broker_pid,
        "brokerStartTicks": broker_start_ticks,
        "instanceNonce": instance_nonce,
        "controlSocketDev": control_socket_dev,
        "controlSocketIno": control_socket_ino,
    }
    payload = json.dumps(expected, separators=(",", ":")).encode()
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(1.0)
    try:
        connection.connect(socket_path)
        connection.sendall(encode_frame(b"K", payload))
        reply, acknowledgement_payload = receive_frame(connection)
        if reply != b"K":
            return 1
        acknowledgement = json.loads(acknowledgement_payload)
        if acknowledgement != expected:
            return 1
        sys.stdout.write(json.dumps(acknowledgement, separators=(",", ":")) + "\n")
        return 0
    except (FileNotFoundError, ConnectionError, OSError, json.JSONDecodeError, UnicodeDecodeError):
        return 1
    finally:
        connection.close()


def run_recover(
    data_socket_path: str,
    data_socket_dev: int,
    data_socket_ino: int,
    control_socket_path: str,
    control_socket_dev: int,
    control_socket_ino: int,
    task_id: str,
    broker_pid: int,
    broker_start_ticks: int,
    cgroup_relative_path: str,
    cgroup_dev: int | None,
    cgroup_ino: int | None,
) -> int:
    live_identity = Broker._read_process_identity(broker_pid)
    if live_identity is not None and live_identity.start_ticks == broker_start_ticks:
        raise RuntimeError("Refusing remnant recovery while the registered broker is still alive")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", task_id):
        raise RuntimeError("A safe task ID is required for remnant recovery")

    sockets = (
        (data_socket_path, (data_socket_dev, data_socket_ino)),
        (control_socket_path, (control_socket_dev, control_socket_ino)),
    )
    cgroup_target: Path | None = None
    with Broker._socket_namespace_locks(tuple(path for path, _identity in sockets)):
        present_sockets: list[tuple[str, tuple[int, int]]] = []
        for path, expected_identity in sockets:
            try:
                info = Broker._socket_path_info(path)
            except FileNotFoundError:
                continue
            if (
                not stat.S_ISSOCK(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or (info.st_dev, info.st_ino) != expected_identity
            ):
                raise RuntimeError(f"Refusing to recover drifted Unix socket path: {path}")
            present_sockets.append((path, expected_identity))

        if cgroup_relative_path:
            if cgroup_dev is None or cgroup_ino is None:
                raise RuntimeError("Terminal cgroup recovery identity is incomplete")
            relative = Path(cgroup_relative_path)
            current_relative = Path(Broker._self_cgroup_relative_path())
            if (
                not cgroup_relative_path.startswith("/")
                or ".." in relative.parts
                or relative.name != f"shared-terminal-{task_id}"
                or relative.parent != current_relative
            ):
                raise RuntimeError("Refusing to recover an untrusted terminal cgroup path")
            cgroup_target = Path("/sys/fs/cgroup") / cgroup_relative_path.lstrip("/")
            try:
                cgroup_info = os.lstat(cgroup_target)
            except FileNotFoundError:
                cgroup_target = None
            else:
                if (
                    not stat.S_ISDIR(cgroup_info.st_mode)
                    or cgroup_info.st_uid != os.geteuid()
                    or (cgroup_info.st_dev, cgroup_info.st_ino) != (cgroup_dev, cgroup_ino)
                ):
                    raise RuntimeError("Refusing to recover an untrusted terminal cgroup")
        elif cgroup_dev is not None or cgroup_ino is not None:
            raise RuntimeError("Terminal cgroup path is missing for the supplied recovery identity")

        for path, expected_identity in present_sockets:
            quarantine = f"{path}.remove-{os.getpid()}-{secrets.token_hex(12)}"
            Broker._rename_noreplace(path, quarantine)
            isolated = Broker._socket_path_info(quarantine)
            if not stat.S_ISSOCK(isolated.st_mode) or (isolated.st_dev, isolated.st_ino) != expected_identity:
                try:
                    Broker._rename_noreplace(quarantine, path)
                finally:
                    raise RuntimeError(f"Recovered Unix socket identity drifted after quarantine: {path}")
            os.unlink(quarantine)

    if cgroup_target is not None:
        kill_path = cgroup_target / "cgroup.kill"
        events_path = cgroup_target / "cgroup.events"
        with kill_path.open("w") as control:
            control.write("1\n")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            events = dict(
                line.split(maxsplit=1)
                for line in events_path.read_text().splitlines()
                if " " in line
            )
            if events.get("populated") == "0":
                cgroup_target.rmdir()
                cgroup_target = None
                break
            time.sleep(0.02)
        if cgroup_target is not None:
            raise RuntimeError("Recovered terminal cgroup remained populated")

    acknowledgement = {
        "taskId": task_id,
        "brokerPid": broker_pid,
        "brokerStartTicks": broker_start_ticks,
        "dataSocketDev": str(data_socket_dev),
        "dataSocketIno": str(data_socket_ino),
        "controlSocketDev": str(control_socket_dev),
        "controlSocketIno": str(control_socket_ino),
        "cgroupPath": cgroup_relative_path,
        "cgroupDev": "" if cgroup_dev is None else str(cgroup_dev),
        "cgroupIno": "" if cgroup_ino is None else str(cgroup_ino),
    }
    sys.stdout.write(json.dumps(acknowledgement, separators=(",", ":")) + "\n")
    return 0


def run_status(socket_path: str) -> int:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(1.0)
    try:
        connection.connect(socket_path)
        connection.sendall(encode_frame(b"S"))
        reply, payload = receive_frame(connection)
        if reply != b"S":
            return 1
        sys.stdout.buffer.write(payload + b"\n")
        return 0
    except (FileNotFoundError, ConnectionError, OSError):
        return 1
    finally:
        connection.close()


def run_lock(descriptor: int, timeout_seconds: float) -> int:
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise RuntimeError("Registry lock fd is not a trusted regular file")
        os.fchmod(descriptor, 0o600)
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Timed out waiting for registry lock")
                time.sleep(0.02)
        owner = json.dumps({
            "pid": os.getpid(),
            "startTicks": Broker._process_start_ticks(),
        }, separators=(",", ":")).encode()
        os.ftruncate(descriptor, 0)
        os.write(descriptor, owner + b"\n")
        os.fsync(descriptor)
        sys.stdout.write("LOCKED\n")
        sys.stdout.flush()
        return 0
    finally:
        # The Node parent holds a duplicate of this same open-file-description.
        # Closing only the helper descriptor preserves the flock until the
        # parent's critical section closes its descriptor.
        os.close(descriptor)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=VERSION)
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve")
    serve.add_argument("--socket", required=True)
    serve.add_argument("--control-socket")
    serve.add_argument("--cwd", required=True)
    serve.add_argument("--shell", required=True)
    serve.add_argument("--task-id", default="")
    serve.add_argument("--instance-nonce")
    serve.add_argument("--replay-bytes", type=int, default=512 * 1024)
    serve.add_argument("--max-clients", type=int, default=4)
    serve.add_argument("--max-client-input-bytes", type=int, default=DEFAULT_MAX_CLIENT_INPUT_BYTES)
    serve.add_argument("--max-client-output-bytes", type=int, default=DEFAULT_MAX_CLIENT_OUTPUT_BYTES)
    serve.add_argument("--max-pty-input-bytes", type=int, default=DEFAULT_MAX_PTY_INPUT_BYTES)
    serve.add_argument("--require-cgroup", action="store_true")
    serve.add_argument("--startup-gate-fd", type=int)

    attach = commands.add_parser("attach")
    attach.add_argument("--socket", required=True)

    probe = commands.add_parser("probe")
    probe.add_argument("--socket", required=True)

    stop = commands.add_parser("stop")
    stop.add_argument("--socket", required=True)
    stop.add_argument("--task-id", required=True)
    stop.add_argument("--broker-pid", required=True, type=int)
    stop.add_argument("--broker-start-ticks", required=True, type=int)
    stop.add_argument("--instance-nonce", required=True)
    stop.add_argument("--control-socket-dev", required=True)
    stop.add_argument("--control-socket-ino", required=True)

    recover = commands.add_parser("recover")
    recover.add_argument("--data-socket", required=True)
    recover.add_argument("--data-socket-dev", required=True, type=int)
    recover.add_argument("--data-socket-ino", required=True, type=int)
    recover.add_argument("--control-socket", required=True)
    recover.add_argument("--control-socket-dev", required=True, type=int)
    recover.add_argument("--control-socket-ino", required=True, type=int)
    recover.add_argument("--task-id", required=True)
    recover.add_argument("--broker-pid", required=True, type=int)
    recover.add_argument("--broker-start-ticks", required=True, type=int)
    recover.add_argument("--cgroup-path", default="")
    recover.add_argument("--cgroup-dev", type=int)
    recover.add_argument("--cgroup-ino", type=int)

    status = commands.add_parser("status")
    status.add_argument("--socket", required=True)

    lock = commands.add_parser("lock")
    lock.add_argument("--fd", required=True, type=int)
    lock.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    os.umask(0o077)
    args = parse_args()
    if args.command == "serve":
        bounded = {
            "replay bytes": (args.replay_bytes, 0, MAX_FRAME_BYTES),
            "max clients": (args.max_clients, 1, 64),
            "max client input bytes": (args.max_client_input_bytes, HEADER.size, MAX_FRAME_BYTES + HEADER.size),
            "max client output bytes": (args.max_client_output_bytes, HEADER.size, 16 * MAX_FRAME_BYTES),
            "max PTY input bytes": (args.max_pty_input_bytes, 1, 16 * MAX_FRAME_BYTES),
        }
        for label, (value, minimum, maximum) in bounded.items():
            if value < minimum or value > maximum:
                raise ValueError(f"{label} must be between {minimum} and {maximum}")
        if args.replay_bytes + HEADER.size > args.max_client_output_bytes:
            raise ValueError("replay bytes must fit inside one client output queue")
        buffer_budget = (
            (args.max_clients + MAX_PENDING_DATA_CLIENTS) * args.max_client_input_bytes
            + args.max_clients * args.max_client_output_bytes
            + args.replay_bytes
            + args.max_pty_input_bytes
            + MAX_CONTROL_CLIENTS * 64 * 1024
        )
        if buffer_budget > MAX_BROKER_BUFFER_BYTES:
            raise ValueError(
                f"aggregate broker buffer budget {buffer_budget} exceeds {MAX_BROKER_BUFFER_BYTES} bytes"
            )
        return Broker(
            args.socket,
            args.cwd,
            args.shell,
            args.task_id,
            args.replay_bytes,
            args.max_clients,
            args.max_client_input_bytes,
            args.max_client_output_bytes,
            args.max_pty_input_bytes,
            args.control_socket,
            args.require_cgroup,
            instance_nonce=args.instance_nonce,
            startup_gate_fd=args.startup_gate_fd,
        ).run()
    if args.command == "attach":
        return run_attach(args.socket)
    if args.command == "probe":
        return run_control(args.socket, b"P")
    if args.command == "stop":
        return run_stop(
            args.socket,
            args.task_id,
            args.broker_pid,
            args.broker_start_ticks,
            args.instance_nonce,
            args.control_socket_dev,
            args.control_socket_ino,
        )
    if args.command == "recover":
        return run_recover(
            args.data_socket,
            args.data_socket_dev,
            args.data_socket_ino,
            args.control_socket,
            args.control_socket_dev,
            args.control_socket_ino,
            args.task_id,
            args.broker_pid,
            args.broker_start_ticks,
            args.cgroup_path,
            args.cgroup_dev,
            args.cgroup_ino,
        )
    if args.command == "status":
        return run_status(args.socket)
    if args.command == "lock":
        if args.timeout_seconds <= 0 or args.timeout_seconds > 60:
            raise ValueError("lock timeout must be greater than 0 and at most 60 seconds")
        return run_lock(args.fd, args.timeout_seconds)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
