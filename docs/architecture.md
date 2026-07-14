# Architecture

## Goal

Allow multiple code-server browser windows to discover the same server-side terminal tasks while keeping each task visible as a native terminal tab.

```text
browser A extension host ─┐
                           ├─ tasks.json ─ TaskStore ─ dedicated tmux socket
browser B extension host ─┘                         ├─ task A PTY
                                                     └─ task B PTY
```

## Components

- `runtime-config.ts` derives HOME, user, PATH and shell from the active code-server extension host. User settings can override paths without rebuilding the extension.
- `TaskStore` serializes registry writes with a directory lock and atomic rename. Locks older than 30 seconds are treated as abandoned and recovered.
- Every task owns a distinct tmux session with its status bar disabled.
- Every browser uses native `createTerminal` with `tmux attach-session`; tmux remains an invisible persistence backend.
- A file watcher and a three-second poller synchronize task facts across extension hosts.

## Consistency semantics

- The task registry is the shared server fact.
- Terminal tabs are browser-local objects reconstructed from that fact.
- Closing a tab detaches one browser.
- Deleting a task terminates its tmux session and makes other attached tabs exit.
- Multiple clients may attach to one task, but simultaneous interactive input requires human coordination.

## Security boundaries

- Registry files are created with mode `0600`.
- The registry contains metadata only, never terminal output or environment values.
- Process execution uses `execFile` argument arrays, not shell command strings.
- Task directories must be absolute and are checked by the extension before creation.
- Termination requires modal confirmation.
- No network port or standalone WebSocket service is introduced.
- Additional environment configuration may contain operational data; users must not store secrets in workspace settings.
