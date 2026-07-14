import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { CommandRunner, TaskStore } from "../src/task-store";

class FakeRunner implements CommandRunner {
  readonly calls: Array<{ command: string; args: string[] }> = [];
  readonly sessions = new Set<string>();

  async run(command: string, args: string[]): Promise<{ stdout: string }> {
    this.calls.push({ command, args });
    if (args.includes("has-session")) {
      const name = args.at(-1)!;
      if (!this.sessions.has(name)) {
        throw new Error("missing session");
      }
    }
    if (args.includes("new-session")) {
      this.sessions.add(args[args.indexOf("-s") + 1]);
    }
    if (args.includes("kill-session")) {
      this.sessions.delete(args.at(-1)!);
    }
    return { stdout: "" };
  }
}

class FailStatusRunner extends FakeRunner {
  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    const result = await super.run(command, args);
    if (args.includes("set-option")) {
      throw new Error("status setup failed");
    }
    return result;
  }
}

async function fixture() {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new FakeRunner();
  return { runner, store: new TaskStore(join(dir, "tasks.json"), runner) };
}

test("create registers a persistent task and starts a hidden tmux session", async () => {
  const { runner, store } = await fixture();

  const task = await store.create("Codex 主任务", "/home/coder/aiwork");

  assert.equal(task.name, "Codex 主任务");
  assert.equal(task.cwd, "/home/coder/aiwork");
  assert.match(task.id, /^[a-z0-9-]+$/);
  assert.equal(runner.sessions.has(task.session), true);
  const createCall = runner.calls.find(({ args }) => args.includes("new-session"));
  assert.equal(createCall?.args.includes("HOME=/home/coder"), true);
  assert.equal(createCall?.args.includes("CODEX_HOME=/home/coder/.codex"), true);
  assert.equal(createCall?.args.includes("PATH=/home/coder/.local/bin:/home/coder/.local/node-v24.11.1-linux-x64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"), true);
  assert.equal(runner.calls.some(({ args }) => args.includes("status") && args.includes("off")), true);
  assert.deepEqual(await store.list(), [task]);
});

test("list reports whether each server session is alive", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("日志", "/home/coder/aiwork");

  assert.equal((await store.listWithStatus())[0].alive, true);
  runner.sessions.delete(task.session);
  assert.equal((await store.listWithStatus())[0].alive, false);
});

test("rename changes the shared label without replacing the session", async () => {
  const { store } = await fixture();
  const task = await store.create("旧名称", "/home/coder/aiwork");

  const renamed = await store.rename(task.id, "后端服务");

  assert.equal(renamed.name, "后端服务");
  assert.equal(renamed.session, task.session);
});

test("delete kills the server session and removes the registry entry", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("临时任务", "/home/coder/aiwork");

  await store.delete(task.id);

  assert.equal(runner.sessions.has(task.session), false);
  assert.deepEqual(await store.list(), []);
  assert.deepEqual(JSON.parse(await readFile(store.registryPath, "utf8")), { version: 1, tasks: [] });
});

test("create rejects duplicate names and invalid working directories", async () => {
  const { store } = await fixture();
  await store.create("Codex", "/home/coder/aiwork");

  await assert.rejects(() => store.create("Codex", "/home/coder/aiwork"), /已存在/);
  await assert.rejects(() => store.create("Other", "relative/path"), /绝对路径/);
});

test("create removes the tmux session when setup fails", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new FailStatusRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner);

  await assert.rejects(() => store.create("失败任务", "/home/coder/aiwork"), /status setup failed/);

  assert.deepEqual([...runner.sessions], []);
  assert.deepEqual(await store.list(), []);
});
