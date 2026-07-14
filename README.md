# Code Server Shared Terminals

[![CI](https://github.com/9904099/code-server-shared-terminals/actions/workflows/ci.yml/badge.svg)](https://github.com/9904099/code-server-shared-terminals/actions/workflows/ci.yml)

A Linux/code-server extension that maps server-side persistent tasks to native terminal tabs. Multiple browser windows can discover and attach to the same task list without exposing tmux as the visible terminal UI.

## Features

- A **Shared Terminal Tasks** view in Explorer.
- One hidden tmux session per persistent task.
- Native VS Code terminal tabs in every browser window.
- File watching plus polling for cross-window synchronization.
- Closing a shared terminal tab in any browser closes it in every connected browser; the tmux task stays available for reopening. **Terminate and Delete** stops the server task after confirmation.
- The registry stores only task ID, name, working directory, session name and creation time—not terminal output.
- Runtime paths, user, shell and environment are derived from the current code-server instance instead of `/home/coder`.
- Stale registry locks are recovered automatically after 30 seconds.

## Requirements

- Linux code-server compatible with VS Code API `^1.127.0`
- `tmux` 3.x available on the code-server host
- Node.js 22 for building from source

Install tmux before using the extension:

```bash
# Debian / Ubuntu
sudo apt-get update && sudo apt-get install -y tmux

# Fedora / RHEL
sudo dnf install -y tmux

# Alpine
sudo apk add tmux
```

## Install

Install directly from Open VSX in code-server by searching for `code-server-shared-terminals`, or download the VSIX from [GitHub Releases](https://github.com/9904099/code-server-shared-terminals/releases/latest):

```bash
code-server --install-extension code-server-shared-terminals-0.2.2.vsix --force
```

After the first installation **and after every extension update**, activate the installed version in every code-server browser window that was already open:

1. Run `Developer: Reload Window` from the Command Palette in each open window; or
2. Refresh the complete browser page.

Newly opened browser windows already use the current extension version and do not need an additional reload. After activation, use the **共享终端任务** view in Explorer.

## Build and test

```bash
npm ci
npm test
npm audit --omit=dev
npm run package
```

Artifact: `code-server-shared-terminals-0.2.2.vsix`.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `sharedTerminals.autoOpen` | `true` | Map registered tasks to native terminal tabs automatically |
| `sharedTerminals.registryPath` | extension global storage | Shared task registry path |
| `sharedTerminals.defaultCwd` | first workspace, then user HOME | Default task working directory |
| `sharedTerminals.tmuxPath` | `tmux` | tmux executable name or absolute path |
| `sharedTerminals.socketName` | `code-server-shared-tasks` | Dedicated tmux socket name |
| `sharedTerminals.shellPath` | `$SHELL`, then `/bin/sh` | Shell for new tasks |
| `sharedTerminals.environment` | `{}` | Extra environment variables for new tasks |

Do not put passwords or tokens in workspace settings. Use a secure host-level environment or secret manager.

## Semantics and limits

- Two browsers attached to the same task operate the same PTY. Do not type into the same interactive task concurrently.
- Create separate tasks when users need independent work.
- This extension currently supports Linux/code-server only.
- The task registry is shared by extension hosts that use the same registry path and operating-system user.

## Architecture and operations

- [Architecture](docs/architecture.md)
- [Runbook](docs/runbook.md)

## License

[MIT](LICENSE)
