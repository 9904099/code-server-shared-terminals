# Code Server Shared Terminals

[![CI](https://github.com/9904099/code-server-shared-terminals/actions/workflows/ci.yml/badge.svg)](https://github.com/9904099/code-server-shared-terminals/actions/workflows/ci.yml)

A Linux/code-server extension that maps server-side persistent tasks to native terminal tabs. Shared tasks use a lightweight Python PTY broker, so multiple browser windows can attach without tmux's extra terminal-emulation layer.

## Features

- A **Shared Terminal Tasks** view in Explorer.
- Explicit shared-terminal creation through the **共享终端任务** view, the `共享终端: 新建任务` command, or the **共享终端（快速）** terminal profile. Ordinary terminal `+` tabs remain browser-local and are never silently converted.
- One lightweight server-side PTY broker per new shared task, using only Python's standard library, a private data socket and a separate private control socket.
- New extension-created shells fail closed unless the code-server service delegates cgroup v2. The broker remains in the service cgroup while each shell and every descendant run inside a task-specific child cgroup that is removed only after `cgroup.kill` reports `populated 0`.
- Every Broker receives short, random, non-reused data/control socket paths. Existing paths, including stale sockets, are never auto-replaced; namespace operations are serialized by a persistent mode-`0600` `flock`. Cleanup first isolates the pathname with Linux `renameat2(RENAME_NOREPLACE)`, verifies the recorded bind-time inode in quarantine, and restores rather than deletes any replacement identity.
- Native VS Code terminal tabs in every browser window.
- File watching plus polling for cross-window synchronization.
- Closing a shared terminal tab in any browser closes it in every connected browser; the server-side task stays available for reopening. Closing or reloading the whole browser window only detaches that client and does not publish a shared close. **Terminate and Delete** stops the server task after confirmation.
- Slow clients have independent bounded output queues; one browser cannot block the broker for every other browser.
- A bounded 512 KiB replay buffer restores recent output when another browser attaches. Replay is memory-only and never written to the registry.
- The active input client controls PTY size, preventing two differently sized browsers from continuously fighting over rows and columns.
- The registry stores only task metadata plus Broker PID/start-time, socket dev/inode and task-cgroup path/dev/inode needed for fail-closed cleanup—not terminal output or environment values.
- Input parsing, PTY input, per-client output, attached-client count and total task count all have explicit limits. Malformed or slow clients are disconnected independently.
- A private status frame reports queue sizes, RSS, FDs, thread count, traffic counters and event-loop lag without returning terminal output, environment values or the working directory.
- Runtime paths, user, shell and environment are derived from the current code-server instance instead of `/home/coder`.
- Registry writes use a persistent lock file with a kernel `flock`; the Node process keeps the locked open-file-description for the complete transaction, so lock ownership cannot be stolen by age.
- New Brokers are durably registered as `starting` with a one-time nonce before spawn. An inherited startup gate keeps the Broker from creating cgroups, sockets or a Shell until PID/start ticks are durable; EOF cancels startup if the creator dies. Promotion to `running` requires full process, data/control socket and task-cgroup device/inode verification. If that publication fails, the complete identity is persisted in the `starting` row before cleanup. A replacement Extension Host can recover a live row, or retry identity-bound remnant cleanup after the exact Broker exits; uncertain or replaced resources remain visible instead of being deleted by path.
- Cross-window synchronization removes stale browser-local tabs whose task IDs disappeared from the registry. Attach tabs are marked transient because the Broker—not VS Code terminal restoration—is the persistence layer. Each Extension Host keeps at most one native tab per immutable task ID, so a restoration race cannot retain duplicate attach clients. Closed or programmatically disposed terminal objects leave only weak tombstones, preventing delayed tracking from remapping them without retaining objects when a close event is lost.
- Remote terminal-close events that race with another window's completed deletion converge idempotently without recreating metadata or showing a false missing-task error.
- Listener publication and stop are identity-bound: partial listener initialization removes only the socket inode it created, Registry publication failure uses the same authenticated stop path after Broker identity is known, and TaskStore keeps the row until the Broker PID, both sockets and task cgroup are all gone. Diagnostics/benchmarks require the six-field graceful-stop acknowledgement instead of treating forced cleanup as success.

## Requirements

- Linux code-server compatible with VS Code API `^1.127.0`
- Python 3 available on the code-server host
- Unified cgroup v2 and a code-server systemd service configured with `Delegate=yes`; without delegation, creation of a new `0.3.0` broker task is rejected before the shell starts
- Container deployments need an init/reaper (for example Docker `--init`) so a Broker orphaned by an Extension Host restart is reaped after it exits
- `tmux` 3.x is needed only while opening legacy tasks created by version `0.2.x`
- Node.js 22 for building from source

Install Python 3 before using the extension:

```bash
# Debian / Ubuntu
sudo apt-get update && sudo apt-get install -y python3

# Fedora / RHEL
sudo dnf install -y python3

# Alpine
sudo apk add python3
```

## Install

Install directly from Open VSX in code-server by searching for `code-server-shared-terminals`, or download the VSIX from [GitHub Releases](https://github.com/9904099/code-server-shared-terminals/releases/latest):

```bash
code-server --install-extension code-server-shared-terminals-0.3.0.vsix --force
```

After the first installation **and after every extension update**, activate the installed version in every code-server browser window that was already open:

1. Run `Developer: Reload Window` from the Command Palette in each open window; or
2. Refresh the complete browser page.

Newly opened browser windows already use the current extension version and do not need an additional reload. After activation, create shared tasks from the **共享终端任务** view, run `共享终端: 新建任务`, or explicitly select **共享终端（快速）** from the terminal profile selector. Ordinary terminal `+` tabs stay browser-local by design.

## Build and test

```bash
npm ci
npm test
npm run test:resource
npm run bench:broker
npm run soak:broker -- --duration 600
npm audit --omit=dev
npm run package
```

Artifact: `code-server-shared-terminals-0.3.0.vsix`.

The ordinary local test commands use the subreaper fallback so they can run in unprivileged CI. Release-candidate soak/benchmark evidence must add `--require-cgroup` and run inside the delegated target service or an isolated privileged cgroup-v2 container; the JSON must report `observedProcessBoundary: "cgroup-v2"`.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `sharedTerminals.autoOpen` | `true` | Map registered tasks to native terminal tabs automatically |
| `sharedTerminals.registryPath` | extension global storage | Shared task registry path |
| `sharedTerminals.defaultCwd` | first workspace, then user HOME | Default task working directory |
| `sharedTerminals.pythonPath` | `python3` | Python executable used by the lightweight PTY broker |
| `sharedTerminals.socketDirectory` | private per-user `/tmp` directory | Unix socket directory for broker tasks |
| `sharedTerminals.tmuxPath` | `tmux` | tmux executable for legacy `0.2.x` tasks |
| `sharedTerminals.socketName` | `code-server-shared-tasks` | Dedicated tmux socket for legacy tasks |
| `sharedTerminals.shellPath` | `$SHELL`, then `/bin/sh` | Shell for new tasks |
| `sharedTerminals.environment` | `{}` | Extra environment variables for new tasks |
| `sharedTerminals.maxTasks` | `12` | Maximum registered shared tasks for this operating-system user |
| `sharedTerminals.maxClientsPerTask` | `4` | Maximum attached browser terminals per shared PTY; identity-bound probe/status/stop operations use the separate control socket |
| `sharedTerminals.replayBytes` | `524288` | Memory-only replay bytes retained per terminal |
| `sharedTerminals.maxClientInputBytes` | `262144` | Maximum buffered protocol input per client |
| `sharedTerminals.maxClientOutputBytes` | `2097152` | Maximum queued output per client before slow-client disconnect |
| `sharedTerminals.maxPtyInputBytes` | `262144` | Maximum aggregate PTY input queue per terminal |

Do not put passwords or tokens in workspace settings. Use a secure host-level environment or secret manager.

## Semantics and limits

- Two browsers attached to the same task operate the same PTY. Do not type into the same interactive task concurrently.
- Create separate tasks when users need independent work.
- Broker tasks survive browser and Extension Host disconnects, but not a host reboot or a code-server service stop that kills its whole control group. Registry rows remain visible as stopped; recreating a shell cannot restore commands that were running before process loss.
- `0.3.0` does not silently fall back to process-tree scanning for extension-created tasks. If cgroup delegation is missing or stale, task creation fails before publishing a socket or starting the login shell.
- Existing ordinary terminals and legacy tmux tasks are not silently migrated. Explicitly created shared terminals use the fast broker; legacy tasks continue through tmux until explicitly deleted.
- This extension currently supports Linux/code-server only.
- The task registry is shared by extension hosts that use the same registry path and operating-system user.
- Appearing in the registry does not prove a process is alive. Liveness uses the private socket; termination additionally verifies task ID plus broker PID/start time before sending the stop frame. Linux `Z`/`X` process states are treated as exited, but a persistent Zombie still indicates a missing host/container reaper and blocks release validation.
- No-client terminals are intentionally retained because they may run long jobs. Cleanup is explicit; the extension does not kill tasks merely because every browser disconnected.

## Architecture and operations

- [Architecture](docs/architecture.md)
- [Runbook](docs/runbook.md)

## License

[MIT](LICENSE)
