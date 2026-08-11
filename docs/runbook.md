# Runbook

## Build and verify

```bash
npm ci
npm test
npm run test:resource
npm run bench:broker
npm run soak:broker -- --duration 600
npm audit --omit=dev
npm run package
npx @vscode/vsce ls
sha256sum code-server-shared-terminals-0.3.1.vsix
```

## Install and validate

1. Confirm the target is Linux code-server and record its version, runtime user, HOME and workspace.
2. Confirm Python 3, unified cgroup v2 and filesystem `flock` support are available. Broker socket paths are random and never reused; a pre-existing path blocks startup instead of being moved or deleted. Keep tmux only if legacy `0.2.x` tasks must remain attachable.
3. Confirm the code-server systemd unit reports `Delegate=yes`. If it does not, prepare a reviewed drop-in containing `[Service]` and `Delegate=yes`; applying it requires `daemon-reload` plus a code-server restart and therefore is a separately approved production change. Do not install `0.3.x` first and hope it falls back: new task creation deliberately fails closed without delegation.
4. Back up an existing extension directory and registry if upgrading.
5. Install the VSIX without restarting code-server.
6. Reload two browser windows.
7. In browser A, run `共享终端: 新建任务` from the Command Palette (or explicitly select **共享终端（快速）** from the terminal profile selector). Confirm browser B receives the same native terminal tab. The ordinary terminal `+` and `Ctrl+Shift+\`` use code-server's normal default profile unless the user explicitly changes it; a resulting `bash` tab is therefore not evidence that synchronization failed.
8. Create a second task and verify the two broker sockets, shell PIDs and `shared-terminal-<task-id>` cgroups are different.
9. Close or reload browser B's whole window, wait beyond the watcher interval, and verify the Registry remains `open: true`, browser A stays attached and a fresh browser B reattaches with exactly one matching shared tab. Confirm the Broker client count does not grow from a duplicate native-terminal restoration.
10. Close browser A's shared terminal tab and verify browser B's matching tab closes while the broker task stays alive.
11. Reopen the task and verify both browsers attach again; then use **Terminate and Delete** and verify both clients exit, the Broker PID, both sockets, task cgroup and Registry entry all disappear. In a container, a persistent `Z` process means PID 1 is not reaping orphans; use an init/reaper and keep the gate blocked even though the task is no longer executable.

## Read-only diagnostics

Use the configured registry path and socket name:

```bash
code-server --list-extensions --show-versions
python3 --version
find /tmp/code-server-shared-terminals-"$(id -u)" -maxdepth 1 -type s -print
# Legacy tasks only:
tmux -L code-server-shared-tasks list-sessions
tmux -L code-server-shared-tasks list-clients
```

The default registry resides under the extension's code-server global storage directory. Do not print terminal contents or secret-bearing environment variables.

For one broker, use its private socket to retrieve non-sensitive bounded status:

```bash
python3 /path/to/pty_broker.py status --socket "$(python3 -c 'import json; print(json.load(open("/private/path/tasks.json"))["tasks"][0]["brokerControlPath"])')"
```

The JSON contains process identity, `processBoundary`, task cgroup path, client/queue counts, RSS, FDs, threads, traffic totals and event-loop lag. It intentionally omits terminal output, environment values and cwd. Production candidates must report `processBoundary: "cgroup-v2"`.

## Resource release gates

- `npm run test:resource` must return clients/FDs/threads to baseline after attach/detach and malformed-client churn.
- `test/attach-backpressure.test.ts` must prove that an attach process keeps draining at least 32 MiB from the Broker while its native PTY stdout is paused, then flushes in order when the renderer resumes. Its overflow case must retain the latest marker, emit the visible truncation warning and stay within the configured/RSS bound.
- `sharedTerminals.maxAttachOutputBytes=104857600` allows 100 MiB per native attach without preallocation. With the default `12` tasks and `4` clients, the enforced theoretical attach-buffer ceiling is 4.8 GiB; increasing task/client ceilings with the same buffer is rejected once the 5 GiB per-user aggregate gate would be exceeded.
- `npm run bench:broker` records direct broker P50/P95/P99 key-to-output latency and bulk throughput. It is a local component measurement, not a substitute for the two-browser user-visible path.
- Run the 10-minute soak for every candidate with `--require-cgroup`; it must exercise hot PTY input/output, a non-reading slow consumer, connection churn, idle recovery, control-plane latency and event-loop lag. Bind 24-hour and 72-hour cgroup-mode soak evidence to the exact candidate SHA before public release or production installation.
- RSS slope uses actual monotonic sample timestamps and is enforced only after at least 60 seconds of second-half evidence; shorter smoke runs still enforce absolute RSS, FD/thread, queue, slow-consumer, latency and cleanup gates but must not extrapolate a few seconds of allocator noise to bytes/hour.
- Soak and benchmark cleanup must receive and verify the full identity-bound stop ACK (`task ID + broker PID/start ticks + nonce + control socket dev/inode`). Any `terminate()`/`SIGKILL` fallback is a failed gate, never a PASS cleanup path.
- Treat an existing instance socket as evidence requiring inspection. Do not delete it merely because connect/probe times out; current Brokers use non-reused random paths and fail closed instead of automatic stale-socket recovery.
- If task creation reports Registry publication failure, verify the authenticated rollback removed the exact Broker, Shell/session descendants, both sockets and task cgroup. Startup publishes a one-time nonce before spawn and holds the Broker on an fd gate until PID/start ticks are durable. If `running` publication fails after full authentication, the extension first persists data/control socket and cgroup path/dev/inode in a `starting` recovery row, then attempts cleanup. An `AggregateError` means cleanup is uncertain and must remain BLOCKED.
- A registry row with `brokerState: "starting"` is a crash-recovery handle, not a disposable stale record. Reload first so the extension can authenticate a live control socket and promote it to `running`. If the exact Broker has exited but recorded resources remain, retry **结束并删除** so the identity-bound `recover` command can remove only matching socket/cgroup inodes. Never hand-delete a path; rows with no surviving PID/socket/cgroup after startup-gate cancellation are pruned, while any surviving or drifted resource retains its row.
- BLOCK on monotonic RSS growth, FD/thread/process count that does not return to baseline, a non-empty idle PTY input queue, an orphan Broker/Shell/socket/cgroup, a `.remove-*` restore-collision artifact, or a slow client stalling another client.
- Do not kill by process name. Termination must pass the TaskStore identity check (`task ID + broker PID + broker start ticks`) and then use the private socket stop frame.
- No-client tasks are retained by design. Remove them only through **结束并删除** after confirming the task is no longer needed.

## Docker compatibility smoke

Build a clean image with Python 3 and start code-server on an unused local port:

```bash
docker build -f test/docker/Dockerfile -t shared-terminals-code-server:test .
docker run --rm -d --name shared-terminals-code-server-test \
  --privileged --cgroupns=private \
  -p 127.0.0.1:18080:8080 \
  -e PASSWORD=shared-terminal-smoke-only \
  shared-terminals-code-server:test
```

Install the packaged VSIX and verify metadata:

```bash
docker cp code-server-shared-terminals-0.3.1.vsix \
  shared-terminals-code-server-test:/tmp/extension.vsix
docker exec shared-terminals-code-server-test \
  code-server --install-extension /tmp/extension.vsix --force
docker exec shared-terminals-code-server-test \
  code-server --list-extensions --show-versions
```

Remove the test container when validation is complete:

```bash
docker rm -f shared-terminals-code-server-test
```

## Rollback

1. Do not stop active PTY broker or legacy tmux processes while tasks must be preserved.
2. Uninstall `9904099.code-server-shared-terminals` or reinstall the previous VSIX.
3. Reload every browser window.
4. Verify ordinary terminals remain functional.
5. Only after confirming no legacy task must be retained, remove its registry row and run:

```bash
tmux -L code-server-shared-tasks kill-server
```
