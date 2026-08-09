import assert from "node:assert/strict";
import test from "node:test";

import {
  applyRuntimeOverrides,
  resolveExecutablePath,
  resolveRuntimeConfig,
  validateRuntimeResourceBudget,
} from "../src/runtime-config";

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
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/code-server-shared-terminals-1000",
  });

  assert.equal(config.registryPath, "/config/data/User/globalStorage/9904099.code-server-shared-terminals/tasks.json");
  assert.equal(config.shellPath, "/bin/zsh");
  assert.equal(config.tmuxPath, "tmux");
  assert.equal(config.socketName, "code-server-shared-tasks");
  assert.equal(config.pythonPath, "python3");
  assert.equal(config.brokerScriptPath, "/extension/scripts/pty_broker.py");
  assert.equal(config.socketDirectory, "/tmp/code-server-shared-terminals-1000");
  assert.equal(config.maxTasks, 12);
  assert.equal(config.maxClientsPerTask, 4);
  assert.equal(config.replayBytes, 512 * 1024);
  assert.equal(config.maxClientInputBytes, 256 * 1024);
  assert.equal(config.maxClientOutputBytes, 2 * 1024 * 1024);
  assert.equal(config.maxPtyInputBytes, 256 * 1024);
  assert.equal(config.requireCgroup, true);
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
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/code-server-shared-terminals-1001",
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
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/code-server-shared-terminals-1000",
  });
  const config = applyRuntimeOverrides(base, {
    registryPath: "/shared/tasks.json",
    tmuxPath: "/usr/local/bin/tmux",
    socketName: "team-terminals",
    pythonPath: "/opt/python/bin/python3",
    socketDirectory: "/run/user/1000/shared-terminals",
    shellPath: "/bin/fish",
    environment: { TEAM: "platform" },
    maxTasks: 8,
    maxClientsPerTask: 3,
    replayBytes: 131072,
    maxClientInputBytes: 32768,
    maxClientOutputBytes: 524288,
    maxPtyInputBytes: 65536,
    requireCgroup: false,
  });

  assert.equal(config.registryPath, "/shared/tasks.json");
  assert.equal(config.tmuxPath, "/usr/local/bin/tmux");
  assert.equal(config.socketName, "team-terminals");
  assert.equal(config.pythonPath, "/opt/python/bin/python3");
  assert.equal(config.socketDirectory, "/run/user/1000/shared-terminals");
  assert.equal(config.shellPath, "/bin/fish");
  assert.equal(config.environment.TEAM, "platform");
  assert.equal(config.environment.HOME, "/config");
  assert.equal(config.environment.SHELL, "/bin/fish");
  assert.equal(config.maxTasks, 8);
  assert.equal(config.maxClientsPerTask, 3);
  assert.equal(config.replayBytes, 131072);
  assert.equal(config.maxClientInputBytes, 32768);
  assert.equal(config.maxClientOutputBytes, 524288);
  assert.equal(config.maxPtyInputBytes, 65536);
  assert.equal(config.requireCgroup, false);
});

test("tmux executable is resolved from PATH for native terminal creation", () => {
  const existing = new Set(["/usr/local/bin/tmux"]);

  assert.equal(
    resolveExecutablePath("tmux", "/opt/bin:/usr/local/bin:/usr/bin", (path) => existing.has(path)),
    "/usr/local/bin/tmux",
  );
  assert.equal(resolveExecutablePath("/custom/tmux", "/usr/bin", () => true), "/custom/tmux");
  assert.throws(() => resolveExecutablePath("tmux", "/usr/bin", () => false), /未找到 tmux/);
  assert.throws(() => resolveExecutablePath("python3", "/usr/bin", () => false, "Python 3"), /未找到 Python 3/);
});

test("runtime resource validation rejects replay and aggregate memory combinations that can overflow", () => {
  const base = resolveRuntimeConfig({
    home: "/config",
    environment: { USER: "coder", PATH: "/usr/bin", SHELL: "/bin/bash" },
    globalStoragePath: "/global/storage",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/code-server-shared-terminals-1000",
  });

  assert.throws(() => validateRuntimeResourceBudget({
    ...base,
    replayBytes: 1024 * 1024,
    maxClientOutputBytes: 512 * 1024,
  }), /回放缓冲必须小于客户端输出队列/);
  assert.throws(() => validateRuntimeResourceBudget({
    ...base,
    maxClientsPerTask: 8,
    maxClientOutputBytes: 8 * 1024 * 1024,
  }), /单终端聚合缓冲预算/);
  assert.throws(() => validateRuntimeResourceBudget({
    ...base,
    maxTasks: 64,
  }), /全部终端聚合缓冲预算/);
  assert.doesNotThrow(() => validateRuntimeResourceBudget(base));
});
