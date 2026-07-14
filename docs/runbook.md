# Runbook

## Build and verify

```bash
npm ci
npm test
npm audit --omit=dev
npm run package
npx @vscode/vsce ls
sha256sum code-server-shared-terminals-0.2.0.vsix
```

## Install and validate

1. Confirm the target is Linux code-server and record its version, runtime user, HOME and workspace.
2. Install tmux with the host package manager.
3. Back up an existing extension directory and registry if upgrading.
4. Install the VSIX without restarting code-server.
5. Reload two browser windows.
6. Create `smoke-a` in browser A and confirm browser B receives the same native terminal tab.
7. Create a second task and verify the two tmux sessions have different PIDs.
8. Close browser A's tab and verify browser B's task stays alive.
9. Use **Terminate and Delete** and verify both clients exit and the registry entry disappears.

## Read-only diagnostics

Use the configured registry path and socket name:

```bash
code-server --list-extensions --show-versions
tmux -V
tmux -L code-server-shared-tasks list-sessions
tmux -L code-server-shared-tasks list-clients
```

The default registry resides under the extension's code-server global storage directory. Do not print terminal contents or secret-bearing environment variables.

## Docker compatibility smoke

Build a clean image with tmux and start code-server on an unused local port:

```bash
docker build -f test/docker/Dockerfile -t shared-terminals-code-server:test .
docker run --rm -d --name shared-terminals-code-server-test \
  -p 127.0.0.1:18080:8080 \
  -e PASSWORD=shared-terminal-smoke-only \
  shared-terminals-code-server:test
```

Install the packaged VSIX and verify metadata:

```bash
docker cp code-server-shared-terminals-0.2.0.vsix \
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

1. Do not stop the dedicated tmux server while tasks must be preserved.
2. Uninstall `9904099.code-server-shared-terminals` or reinstall the previous VSIX.
3. Reload every browser window.
4. Verify ordinary terminals remain functional.
5. Only after confirming no task must be retained, remove the registry and run:

```bash
tmux -L code-server-shared-tasks kill-server
```
