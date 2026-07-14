# Code Server Shared Terminals

[![CI](https://github.com/9904099/code-server-shared-terminals/actions/workflows/ci.yml/badge.svg)](https://github.com/9904099/code-server-shared-terminals/actions/workflows/ci.yml)

A Linux/code-server extension that maps server-side persistent tasks to native terminal tabs. Multiple browser windows can discover and attach to the same task list without exposing tmux as the visible terminal UI.

## Features

- A **Shared Terminal Tasks** view in Explorer.
- One hidden tmux session per persistent task.
- Native VS Code terminal tabs in every browser window.
- File watching plus polling for cross-window synchronization.
- Closing a tab only detaches that browser; **Terminate and Delete** stops the server task after confirmation.
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

## Build and test

```bash
npm ci
npm test
npm audit --omit=dev
npm run package
```

Artifact: `code-server-shared-terminals-0.2.0.vsix`.

## Install from VSIX

```bash
code-server --install-extension code-server-shared-terminals-0.2.0.vsix --force
```

Reload each browser window with `Developer: Reload Window`, then use the **共享终端任务** view in Explorer.

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

## Open VSX publishing

code-server uses [Open VSX](https://open-vsx.org/) rather than Microsoft's marketplace. Publishing requires an Eclipse account, the Open VSX Publisher Agreement, a `9904099` namespace and an Open VSX access token:

```bash
npx ovsx create-namespace 9904099 -p "$OVSX_PAT"
npx ovsx publish code-server-shared-terminals-0.2.0.vsix -p "$OVSX_PAT"
```

Never commit `OVSX_PAT`. See the [Open VSX publishing guide](https://github.com/eclipse-openvsx/openvsx/wiki/Publishing-Extensions).

The repository also includes a manual **Publish to Open VSX** GitHub Actions workflow. Add the token as the repository secret `OVSX_PAT`; select `create_namespace=true` only for the first publication. Later versions publish with the same workflow and `create_namespace=false`.

## Architecture and operations

- [Architecture](docs/architecture.md)
- [Runbook](docs/runbook.md)

## License

[MIT](LICENSE)
