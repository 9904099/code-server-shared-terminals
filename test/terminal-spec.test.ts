import assert from "node:assert/strict";
import test from "node:test";

import {
  buildTerminalSpec,
  sharedTerminalPrefix,
  sharedTerminalTaskIdEnvironmentKey,
  terminalCreationMatchesTask,
} from "../src/terminal-spec";
import { TaskRuntimeConfig } from "../src/runtime-config";
import { SharedTask } from "../src/task-store";

const task: SharedTask = {
  id: "abc",
  name: "Codex 主任务",
  cwd: "/home/coder/workspace",
  session: "shared-abc",
  createdAt: "2026-07-14T00:00:00.000Z",
  open: true,
  backend: "pty-broker",
  brokerDataPath: "/tmp/shared-terminals-test/t-short.sock",
};

const runtime: TaskRuntimeConfig = {
  tmuxPath: "/custom/tmux",
  socketName: "portable-shared-tasks",
  pythonPath: "/usr/bin/python3",
  brokerScriptPath: "/extension/scripts/pty_broker.py",
  socketDirectory: "/tmp/shared-terminals-test",
  shellPath: "/bin/bash",
  maxAttachOutputBytes: 100 * 1024 * 1024,
  environment: {},
};

test("terminal spec opens the persistent server task as a native VS Code tab", () => {
  const spec = buildTerminalSpec(task, runtime);

  assert.equal(spec.name, `${sharedTerminalPrefix}Codex 主任务`);
  assert.equal(spec.shellPath, "/usr/bin/python3");
  assert.deepEqual(spec.env, { [sharedTerminalTaskIdEnvironmentKey]: task.id });
  assert.deepEqual(spec.shellArgs, [
    "/extension/scripts/pty_broker.py", "attach", "--socket", "/tmp/shared-terminals-test/t-short.sock",
    "--max-output-bytes", String(100 * 1024 * 1024),
  ]);
  assert.equal(spec.cwd, "/home/coder/workspace");
  assert.equal(spec.isTransient, true);
});

test("terminal spec preserves attach support for legacy tmux registry rows", () => {
  const spec = buildTerminalSpec({ ...task, backend: "tmux" }, runtime);

  assert.equal(spec.shellPath, "/custom/tmux");
  assert.deepEqual(spec.shellArgs, ["-L", "portable-shared-tasks", "attach-session", "-t", "shared-abc"]);
  assert.equal(spec.isTransient, true);
});

test("terminal identity requires the immutable task ID and never adopts a lookalike ordinary terminal", () => {
  const spec = buildTerminalSpec(task, runtime);

  assert.equal(terminalCreationMatchesTask({
    shellPath: spec.shellPath,
    shellArgs: spec.shellArgs,
  }, task, runtime), false);
  assert.equal(terminalCreationMatchesTask({
    shellPath: spec.shellPath,
    shellArgs: ["lookalike", ...spec.shellArgs],
    env: { [sharedTerminalTaskIdEnvironmentKey]: task.id },
  }, task, runtime), true);
  assert.equal(terminalCreationMatchesTask({
    shellPath: spec.shellPath,
    shellArgs: spec.shellArgs,
    env: { [sharedTerminalTaskIdEnvironmentKey]: "different-task" },
  }, task, runtime), false);
});
