# Architecture

## Goal

Allow multiple code-server browser windows to discover the same server-side terminal tasks while keeping each task visible as a native terminal tab.

```text
browser A extension host ─┐                         ┌─ browser A attach client
                           ├─ tasks.json ─ PTY broker┼─ browser B attach client
browser B extension host ─┘                         └─ login shell PTY
```

## Components

- `runtime-config.ts` derives HOME, user, PATH and shell from the active code-server extension host. User settings can override paths without rebuilding the extension.
- `TaskStore` serializes registry writes with a persistent mode-`0600` lock file, a kernel `flock` held by the Node process's open-file-description for the complete transaction, and atomic rename. The lock path is never removed or replaced during normal operation.
- Broker creation is a durable `starting -> running` transaction. `TaskStore` writes the task ID, a pre-published one-time nonce and random socket paths before spawn. The Broker blocks on an inherited fd startup gate until PID/start ticks have also been written; creator death closes the gate and aborts before cgroup, listener or Shell allocation. `running` is published only after process, both socket and task-cgroup device/inode identities are verified. If publishing `running` fails, those complete identities are first persisted back into a `starting` recovery row before cleanup begins. If later status publication drifts, the exact PID/start ticks, nonce and twice-read resource identities can still authenticate the stop ACK without trusting the drifted PID. A new Extension Host can recover a live `starting` row from that same identity after the creating host exits; uncertain resources retain their row.
- New tasks own one Python standard-library PTY broker and login shell. The broker listens on separate mode-`0600` data and control Unix sockets inside a mode-`0700` per-user directory.
- The broker stays in the delegated code-server service cgroup. Before the login shell is released from its startup pipe, the broker moves it into a unique `shared-terminal-<task-id>` cgroup v2 child. All later descendants inherit that hard membership boundary, including processes that create a new session or process group.
- Socket paths contain a short random per-Broker instance token and are never reused. Any pre-existing path, including a stale socket, blocks startup and requires explicit operator inspection; the Broker does not probe-and-delete it. Publication and cleanup hold a persistent per-directory namespace `flock`. Cleanup performs consecutive identity checks, atomically moves the current path to a random quarantine name with `renameat2(RENAME_NOREPLACE)`, and deletes only if the quarantined inode is the recorded bind-time identity; a replacement is restored or retained without deletion if restoration collides.
- Every browser uses a transient native terminal tab whose shell process is the packaged attach client. The tab opts out of VS Code terminal persistence/reconnection because the Registry plus Broker are the persistence layer. The client performs byte relay only; it does not add another terminal emulator.
- Output is broadcast through independent bounded client queues. Recent output is retained only in a bounded in-memory replay buffer.
- Protocol parsing, PTY input, per-client output, attached-client count and registered task count are independently bounded. Complete frames left after one event budget are rescheduled without waiting for new socket data.
- The client that most recently supplied input controls PTY size. A resize from another attached browser is remembered but does not resize the shared PTY until that browser becomes active.
- Registry rows without `backend` are treated as `tmux`, preserving attach/delete behavior for `0.2.x` tasks.
- A file watcher and a three-second poller synchronize task facts across extension hosts.

## Consistency semantics

- The task registry is the shared server fact.
- Terminal tabs are browser-local transient objects reconstructed from that fact; each Extension Host keeps at most one mapping per immutable task ID and suppresses any duplicate restored tab before disposal.
- Only an explicit user close of a shared tab writes `open: false`; browser/window shutdown, process exit, extension disposal and unknown close reasons detach locally without publishing shared intent. Every browser disposes its matching local tab after an explicit shared close while the server-side broker or legacy tmux session remains alive.
- Reopening a task writes `open: true`; every browser with automatic mapping enabled reattaches.
- Every synchronization pass also disposes browser-local tabs whose task ID is absent from the registry. Closed and programmatically disposed terminal objects keep weak tombstones so delayed asynchronous tracking cannot remap an invalid tab; a missing VS Code close event still cannot retain terminal objects indefinitely.
- If a remote attach process exits after another window has already deleted the Registry row, its delayed terminal-close event is an idempotent no-op; it cannot recreate the row or surface a false “task does not exist” error.
- Deleting a task waits for its PTY broker to stop before removing the registry row. If Registry publication fails after the authenticated Broker identity is known, creation rollback uses that same stop path and confirms the process, both sockets and task cgroup disappear; it does not hard-kill the Broker and skip descendant cleanup. Post-ACK verification allows five seconds. If the Broker exits while a recorded socket or cgroup remains, a later delete invokes the packaged `recover` command only after confirming the PID/start-ticks identity is gone; it removes only matching socket and cgroup device/inode identities. Any replacement identity fails closed and leaves the durable row. Legacy rows terminate their tmux session.
- Multiple clients may attach to one task, but simultaneous interactive input requires human coordination.

## Security boundaries

- Registry and broker socket files are created with mode `0600`; broker socket directories use `0700`.
- The registry contains metadata only, never terminal output or environment values.
- Process execution uses argument arrays, not shell command strings. Additional environment values are inherited through the spawn environment and are not placed in process arguments or the registry.
- Task directories must be absolute and are checked by the extension before creation.
- Termination requires modal confirmation.
- No network port or standalone WebSocket service is introduced.
- A client that cannot drain its bounded queue is disconnected instead of stalling every attached browser.
- Probe, status and identity-bound stop use the separate control socket, so idle or slow data connections cannot consume control capacity.
- Control-client frames are scheduled ahead of the control listener, and the listener accepts one connection per event-loop turn. Each control connection has a fixed 64 KiB output queue, accepts exactly one request frame and stops reading before flushing its response. This prevents pipelined status responses or a slowloris batch from exceeding the declared memory budget or evicting a complete stop request.
- Additional environment configuration may contain operational data; users must not store secrets in workspace settings.

## Resource model

- One terminal owns one single-threaded Python broker, one task cgroup, one login shell, one PTY FD and two private Unix sockets. The default `maxTasks=12` bounds the linear per-terminal process and RSS cost.
- Hot output and input paths use bounded chunk queues/rings instead of repeatedly deleting the head of large contiguous buffers.
- The status frame exposes only operational counters: broker/shell identity, clients, queue bytes, RSS, FD/thread count, traffic totals, disconnect/rejection counts, event-loop lag and activity time.
- Data and control listeners record their filesystem device/inode immediately after bind using `O_PATH`, `lstat` and non-following `stat` fallbacks. Any later initialization failure closes the FD and isolates/removes only that exact socket identity.
- The task registry binds a new broker to `task ID + PID + /proc start ticks + nonce + data/control socket dev/inode + cgroup path/dev/inode`. Delete fails closed on drift or if the Broker process, either socket or task cgroup remains; legacy tmux rows preserve their existing behavior.
- Browser/Extension Host disconnect does not trigger garbage collection. Explicit termination is required so unattended builds and agent jobs are not killed as false orphans.
- Graceful stop uses `cgroup.kill`, waits for `cgroup.events` to report `populated 0`, and removes the empty task cgroup before the Broker exits. Subreaper/process-tree cleanup remains only an unprivileged test fallback and is not accepted for extension-created release tasks.
