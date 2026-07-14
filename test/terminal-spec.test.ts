import assert from "node:assert/strict";
import test from "node:test";

import { buildTerminalSpec, sharedTerminalPrefix } from "../src/terminal-spec";
import { SharedTask } from "../src/task-store";

const task: SharedTask = {
  id: "abc",
  name: "Codex 主任务",
  cwd: "/home/coder/workspace",
  session: "shared-abc",
  createdAt: "2026-07-14T00:00:00.000Z",
};

test("terminal spec opens the persistent server task as a native VS Code tab", () => {
  const spec = buildTerminalSpec(task, "/custom/tmux", "portable-shared-tasks");

  assert.equal(spec.name, `${sharedTerminalPrefix}Codex 主任务`);
  assert.equal(spec.shellPath, "/custom/tmux");
  assert.deepEqual(spec.shellArgs, ["-L", "portable-shared-tasks", "attach-session", "-t", "shared-abc"]);
  assert.equal(spec.cwd, "/home/coder/workspace");
});
