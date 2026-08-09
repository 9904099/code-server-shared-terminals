#!/usr/bin/env python3
"""Bounded PTY broker soak runner. Emits one JSON summary and no terminal content."""

from __future__ import annotations

import argparse
import json
import os
import socket
import struct
import subprocess
import tempfile
import time
from pathlib import Path

HEADER = struct.Struct("!cI")
SCRIPT = Path(__file__).with_name("pty_broker.py")


def frame(kind: bytes, payload: bytes = b"") -> bytes:
    return HEADER.pack(kind, len(payload)) + payload


def receive_frame(connection: socket.socket) -> tuple[bytes, bytes]:
    data = bytearray()
    while len(data) < HEADER.size:
        chunk = connection.recv(HEADER.size - len(data))
        if not chunk:
            raise ConnectionError("closed before header")
        data.extend(chunk)
    kind, length = HEADER.unpack(data)
    if length > 1024 * 1024:
        raise ConnectionError("oversized status")
    payload = bytearray()
    while len(payload) < length:
        chunk = connection.recv(length - len(payload))
        if not chunk:
            raise ConnectionError("closed before payload")
        payload.extend(chunk)
    return kind, bytes(payload)


def request(socket_path: str, kind: bytes, payload: bytes = b"") -> bytes:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(2)
        connection.connect(socket_path)
        connection.sendall(frame(kind, payload))
        reply, payload = receive_frame(connection)
        if reply != kind:
            raise RuntimeError(f"unexpected reply {reply!r}")
        return payload


def status(socket_path: str) -> dict[str, int | str]:
    return json.loads(request(socket_path, b"S"))


def stop(socket_path: str, current: dict[str, int | str]) -> dict[str, int | str]:
    expected = {
        "taskId": current["taskId"],
        "brokerPid": current["brokerPid"],
        "brokerStartTicks": current["brokerStartTicks"],
        "instanceNonce": current["instanceNonce"],
        "controlSocketDev": current["controlSocketDev"],
        "controlSocketIno": current["controlSocketIno"],
    }
    acknowledgement = json.loads(request(
        socket_path,
        b"K",
        json.dumps(expected, separators=(",", ":")).encode(),
    ))
    if acknowledgement != expected:
        raise RuntimeError("stop acknowledgement identity mismatch")
    return acknowledgement


def attach(socket_path: str, receive_buffer: int | None = None) -> socket.socket:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if receive_buffer is not None:
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
    connection.settimeout(5)
    connection.connect(socket_path)
    connection.sendall(frame(b"A"))
    return connection


def wait_for_output(connection: socket.socket, marker: bytes, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    incoming = bytearray()
    output = bytearray()
    while marker not in output:
        connection.settimeout(max(0.01, deadline - time.monotonic()))
        data = connection.recv(65536)
        if not data:
            raise ConnectionError("fast client closed before hot-output marker")
        incoming.extend(data)
        while len(incoming) >= HEADER.size:
            kind, length = HEADER.unpack(incoming[:HEADER.size])
            if length > 1024 * 1024:
                raise ConnectionError("oversized broker frame")
            if len(incoming) < HEADER.size + length:
                break
            payload = bytes(incoming[HEADER.size:HEADER.size + length])
            del incoming[:HEADER.size + length]
            if kind == b"O":
                output.extend(payload)
        if time.monotonic() >= deadline:
            raise TimeoutError("hot-output marker timeout")
    return len(output)


def slope_per_hour(values: list[int], sample_times: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    if len(values) != len(sample_times):
        raise ValueError("sample values and timestamps must have equal length")
    origin = sample_times[0]
    xs = [sample_time - origin for sample_time in sample_times]
    x_mean = sum(xs) / len(xs)
    y_mean = sum(values) / len(values)
    denominator = sum((value - x_mean) ** 2 for value in xs)
    if denominator == 0:
        return 0.0
    slope_per_second = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values)) / denominator
    return slope_per_second * 3600


def status_retry(socket_path: str, timeout: float = 2.0) -> dict[str, int | str]:
    deadline = time.monotonic() + timeout
    while True:
        try:
            return status(socket_path)
        except (BlockingIOError, ConnectionError, TimeoutError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


def timed_status(socket_path: str) -> tuple[dict[str, int | str], float]:
    started = time.perf_counter_ns()
    current = status_retry(socket_path)
    return current, (time.perf_counter_ns() - started) / 1_000_000


def wait_ready(socket_path: str, deadline: float) -> dict[str, int | str]:
    while time.monotonic() < deadline:
        try:
            return status(socket_path)
        except (FileNotFoundError, ConnectionError, OSError, json.JSONDecodeError):
            time.sleep(0.02)
    raise TimeoutError("broker did not become ready")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--churn-per-interval", type=int, default=10)
    parser.add_argument("--hot-output-bytes", type=int, default=512 * 1024)
    parser.add_argument("--hot-every", type=int, default=10)
    parser.add_argument("--max-control-latency-ms", type=float, default=250.0)
    parser.add_argument("--max-event-loop-lag-ms", type=float, default=250.0)
    parser.add_argument("--require-cgroup", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration < 1 or args.duration > 72 * 3600:
        raise ValueError("duration must be between 1 and 259200 seconds")
    if args.interval <= 0 or args.interval > 60:
        raise ValueError("interval must be between 0 and 60 seconds")
    if args.churn_per_interval < 0 or args.churn_per_interval > 1000:
        raise ValueError("churn-per-interval must be between 0 and 1000")
    if args.hot_output_bytes < 64 * 1024 or args.hot_output_bytes > 8 * 1024 * 1024:
        raise ValueError("hot-output-bytes must be between 65536 and 8388608")
    if args.hot_every < 1 or args.hot_every > 10000:
        raise ValueError("hot-every must be between 1 and 10000")
    if args.max_control_latency_ms <= 0 or args.max_event_loop_lag_ms <= 0:
        raise ValueError("latency thresholds must be positive")

    with tempfile.TemporaryDirectory(prefix="pty-broker-soak-") as directory:
        socket_path = str(Path(directory) / "broker.sock")
        control_socket_path = str(Path(directory) / "broker.control.sock")
        command = [
            "python3", str(SCRIPT), "serve",
            "--socket", socket_path,
            "--control-socket", control_socket_path,
            "--cwd", directory,
            "--shell", "/bin/sh",
            "--task-id", "soak-test",
            "--replay-bytes", "65536",
            "--max-clients", "4",
            "--max-client-input-bytes", "65536",
            "--max-client-output-bytes", "262144",
            "--max-pty-input-bytes", "65536",
        ]
        if args.require_cgroup:
            command.append("--require-cgroup")
        broker = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        samples: list[dict[str, int | str]] = []
        sample_times: list[float] = []
        control_latencies_ms: list[float] = []
        churn = 0
        hot_output_bursts = 0
        fast_client: socket.socket | None = None
        slow_client: socket.socket | None = None
        failures: list[str] = []
        stop_acknowledged = False
        emergency_cleanup = False
        started = time.monotonic()
        try:
            samples.append(wait_ready(control_socket_path, started + 5))
            sample_times.append(time.monotonic())
            deadline = started + args.duration
            iteration = 0
            slow_client = attach(socket_path, receive_buffer=4096)
            while time.monotonic() < deadline:
                if iteration % args.hot_every == 0:
                    if fast_client is not None:
                        fast_client.close()
                    fast_client = attach(socket_path)
                    marker = f"__SOAK_HOT_{iteration:08d}__".encode()
                    command_payload = (
                        "python3 -c \"import os;os.write(1,b'x'*"
                        f"{args.hot_output_bytes})\"; printf '\\n{marker.decode()}\\n'\n"
                    ).encode()
                    fast_client.sendall(frame(b"I", command_payload))
                    wait_for_output(fast_client, marker)
                    hot_output_bursts += 1
                    fast_client.close()
                    fast_client = None
                for _ in range(args.churn_per_interval):
                    try:
                        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                            client.connect(socket_path)
                            client.sendall(frame(b"A"))
                        churn += 1
                    except (BlockingIOError, ConnectionError):
                        pass
                current, latency_ms = timed_status(control_socket_path)
                samples.append(current)
                sample_times.append(time.monotonic())
                control_latencies_ms.append(latency_ms)
                iteration += 1
                time.sleep(min(args.interval, max(0, deadline - time.monotonic())))
            if fast_client is not None:
                fast_client.close()
                fast_client = None
            if slow_client is not None:
                slow_client.close()
                slow_client = None
            for _ in range(5):
                time.sleep(args.interval)
                current, latency_ms = timed_status(control_socket_path)
                samples.append(current)
                sample_times.append(time.monotonic())
                control_latencies_ms.append(latency_ms)
        except Exception as error:
            failures.append(f"soak execution failed: {type(error).__name__}")
        finally:
            if fast_client is not None:
                fast_client.close()
            if slow_client is not None:
                slow_client.close()

        if broker.poll() is None:
            try:
                stop(control_socket_path, status_retry(control_socket_path))
                stop_acknowledged = True
                broker.wait(timeout=5)
            except Exception as error:
                failures.append(f"identity-bound graceful stop failed: {type(error).__name__}")
        if broker.poll() is None:
            emergency_cleanup = True
            broker.terminate()
            try:
                broker.wait(timeout=3)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait(timeout=3)
        if not stop_acknowledged:
            failures.append("identity-bound stop acknowledgement was not received")
        if emergency_cleanup:
            failures.append("emergency process cleanup was required")
        if broker.returncode != 0:
            failures.append("broker exited non-zero")
        if Path(socket_path).exists() or Path(control_socket_path).exists():
            failures.append("broker sockets remained after graceful stop")

        summary: dict[str, object] = {
            "durationSeconds": round(time.monotonic() - started, 3),
            "samples": len(samples),
            "churnConnections": churn,
            "hotOutputBursts": hot_output_bursts,
            "stopAcknowledged": stop_acknowledged,
            "emergencyCleanup": emergency_cleanup,
            "requiredProcessBoundary": "cgroup-v2" if args.require_cgroup else "subreaper",
        }
        if samples:
            baseline = samples[0]
            final = samples[-1]
            rss = [int(sample["rssBytes"]) for sample in samples]
            fds = [int(sample["fdCount"]) for sample in samples]
            threads = [int(sample["threads"]) for sample in samples]
            second_half_start = len(rss) // 2
            second_half = rss[second_half_start:]
            second_half_times = sample_times[second_half_start:]
            rss_slope_per_hour = slope_per_hour(second_half, second_half_times)
            rss_slope_window_seconds = (
                second_half_times[-1] - second_half_times[0]
                if len(second_half_times) > 1 else 0.0
            )
            max_event_loop_lag_ms = max(float(sample["maxEventLoopLagMs"]) for sample in samples)
            control_latency_max_ms = max(control_latencies_ms, default=0.0)
            input_bytes_delta = int(final["inputBytes"]) - int(baseline["inputBytes"])
            output_bytes_delta = int(final["outputBytes"]) - int(baseline["outputBytes"])
            slow_clients_delta = int(final["slowClients"]) - int(baseline["slowClients"])
            required_boundary = "cgroup-v2" if args.require_cgroup else "subreaper"
            summary.update({
                "observedProcessBoundary": baseline.get("processBoundary", "unknown"),
                "rssFirstBytes": rss[0],
                "rssLastBytes": rss[-1],
                "rssPeakBytes": max(rss),
                "rssSecondHalfSlopeBytesPerHour": round(rss_slope_per_hour, 3),
                "rssSlopeWindowSeconds": round(rss_slope_window_seconds, 3),
                "fdFirst": fds[0],
                "fdLast": fds[-1],
                "fdPeak": max(fds),
                "threadPeak": max(threads),
                "clientsFinal": final["clients"],
                "ptyInputBytesFinal": final["ptyInputBytes"],
                "queuedClientOutputBytesFinal": final["queuedClientOutputBytes"],
                "inputBytesDelta": input_bytes_delta,
                "outputBytesDelta": output_bytes_delta,
                "slowClientsDelta": slow_clients_delta,
                "maxEventLoopLagMs": max_event_loop_lag_ms,
                "controlLatencyMaxMs": round(control_latency_max_ms, 3),
            })
            if max(threads) != 1:
                failures.append("thread count exceeded one")
            if baseline.get("processBoundary") != required_boundary:
                failures.append("broker process boundary did not match the requested mode")
            if fds[-1] > fds[0] + 2:
                failures.append("file descriptors did not return to baseline")
            if rss[-1] > rss[0] + 8 * 1024 * 1024:
                failures.append("RSS exceeded bounded growth budget")
            if (
                len(second_half) >= 10
                and rss_slope_window_seconds >= 60
                and rss_slope_per_hour > 4 * 1024 * 1024
            ):
                failures.append("RSS second-half slope exceeded 4 MiB/hour")
            if int(final["ptyInputBytes"]) != 0 or int(final["queuedClientOutputBytes"]) > 262144:
                failures.append("queues did not return to bounded idle state")
            if input_bytes_delta <= 0 or output_bytes_delta < args.hot_output_bytes:
                failures.append("hot PTY input/output workload was not observed")
            if hot_output_bursts == 0 or slow_clients_delta == 0:
                failures.append("slow-consumer isolation workload was not observed")
            if control_latency_max_ms > args.max_control_latency_ms:
                failures.append("control-plane latency exceeded threshold")
            if max_event_loop_lag_ms > args.max_event_loop_lag_ms:
                failures.append("event-loop lag exceeded threshold")
        summary["passed"] = not failures
        summary["failures"] = failures
        print(json.dumps(summary, separators=(",", ":")))
        return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
