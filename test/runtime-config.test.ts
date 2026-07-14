import assert from "node:assert/strict";
import test from "node:test";

import { applyRuntimeOverrides, resolveExecutablePath, resolveRuntimeConfig } from "../src/runtime-config";

test("runtime configuration follows the current code-server user and shell", () => {
  const config = resolveRuntimeConfig({
    home: "/config",
    environment: {
      USER: "coder",
      LOGNAME: "coder",
      PATH: "/custom/bin:/usr/bin",
      SHELL: "/bin/zsh",
      CODEX_HOME: "/config/.codex-custom",
    },
    globalStoragePath: "/config/data/User/globalStorage/9904099.code-server-shared-terminals",
  });

  assert.equal(config.registryPath, "/config/data/User/globalStorage/9904099.code-server-shared-terminals/tasks.json");
  assert.equal(config.shellPath, "/bin/zsh");
  assert.equal(config.tmuxPath, "tmux");
  assert.equal(config.socketName, "code-server-shared-tasks");
  assert.deepEqual(config.environment, {
    HOME: "/config",
    USER: "coder",
    LOGNAME: "coder",
    PATH: "/custom/bin:/usr/bin",
    SHELL: "/bin/zsh",
    CODEX_HOME: "/config/.codex-custom",
  });
});

test("runtime configuration uses portable fallbacks without Codex", () => {
  const config = resolveRuntimeConfig({
    home: "/home/alice",
    environment: {},
    globalStoragePath: "/home/alice/.local/share/code-server/User/globalStorage/extension",
  });

  assert.equal(config.shellPath, "/bin/sh");
  assert.equal(config.environment.HOME, "/home/alice");
  assert.equal(config.environment.USER, "alice");
  assert.equal(config.environment.LOGNAME, "alice");
  assert.equal(config.environment.PATH, "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
  assert.equal("CODEX_HOME" in config.environment, false);
});

test("runtime settings override paths and merge additional environment variables", () => {
  const base = resolveRuntimeConfig({
    home: "/config",
    environment: { USER: "coder", PATH: "/usr/bin", SHELL: "/bin/bash" },
    globalStoragePath: "/global/storage",
  });
  const config = applyRuntimeOverrides(base, {
    registryPath: "/shared/tasks.json",
    tmuxPath: "/usr/local/bin/tmux",
    socketName: "team-terminals",
    shellPath: "/bin/fish",
    environment: { TEAM: "platform" },
  });

  assert.equal(config.registryPath, "/shared/tasks.json");
  assert.equal(config.tmuxPath, "/usr/local/bin/tmux");
  assert.equal(config.socketName, "team-terminals");
  assert.equal(config.shellPath, "/bin/fish");
  assert.equal(config.environment.TEAM, "platform");
  assert.equal(config.environment.HOME, "/config");
  assert.equal(config.environment.SHELL, "/bin/fish");
});

test("tmux executable is resolved from PATH for native terminal creation", () => {
  const existing = new Set(["/usr/local/bin/tmux"]);

  assert.equal(
    resolveExecutablePath("tmux", "/opt/bin:/usr/local/bin:/usr/bin", (path) => existing.has(path)),
    "/usr/local/bin/tmux",
  );
  assert.equal(resolveExecutablePath("/custom/tmux", "/usr/bin", () => true), "/custom/tmux");
  assert.throws(() => resolveExecutablePath("tmux", "/usr/bin", () => false), /未找到 tmux/);
});
