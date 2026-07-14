import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, utimes } from "node:fs/promises";
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

class MissingTmuxRunner extends FakeRunner {
  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    if (args.length === 1 && args[0] === "-V") {
      throw Object.assign(new Error("spawn tmux ENOENT"), { code: "ENOENT" });
    }
    return super.run(command, args);
  }
}

async function fixture() {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new FakeRunner();
  return {
    runner,
    store: new TaskStore(join(dir, "tasks.json"), runner, {
      tmuxPath: "/custom/tmux",
      socketName: "portable-shared-tasks",
      shellPath: "/bin/zsh",
      environment: {
        HOME: "/config",
        USER: "coder",
        LOGNAME: "coder",
        PATH: "/custom/bin:/usr/bin",
        SHELL: "/bin/zsh",
      },
    }),
  };
}

test("create registers a persistent task and starts a hidden tmux session", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("Codex 主任务", "/home/coder/workspace");

  assert.equal(task.name, "Codex 主任务");
  assert.equal(task.cwd, "/home/coder/workspace");
  assert.match(task.id, /^[a-z0-9-]+$/);
  assert.equal(runner.sessions.has(task.session), true);
  const createCall = runner.calls.find(({ args }) => args.includes("new-session"));
  assert.equal(createCall?.command, "/custom/tmux");
  assert.equal(createCall?.args.includes("portable-shared-tasks"), true);
  assert.equal(createCall?.args.includes("HOME=/config"), true);
  assert.equal(createCall?.args.includes("USER=coder"), true);
  assert.equal(createCall?.args.includes("PATH=/custom/bin:/usr/bin"), true);
  assert.deepEqual(createCall?.args.slice(-2), ["/bin/zsh", "-l"]);
  assert.equal(runner.calls.some(({ args }) => args.includes("status") && args.includes("off")), true);
  assert.deepEqual(await store.list(), [task]);
});

test("list reports whether each server session is alive", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("日志", "/home/coder/workspace");
  assert.equal((await store.listWithStatus())[0].alive, true);
  runner.sessions.delete(task.session);
  assert.equal((await store.listWithStatus())[0].alive, false);
});

test("rename changes the shared label without replacing the session", async () => {
  const { store } = await fixture();
  const task = await store.create("旧名称", "/home/coder/workspace");
  const renamed = await store.rename(task.id, "后端服务");
  assert.equal(renamed.name, "后端服务");
  assert.equal(renamed.session, task.session);
});

test("delete kills the server session and removes the registry entry", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("临时任务", "/home/coder/workspace");
  await store.delete(task.id);
  assert.equal(runner.sessions.has(task.session), false);
  assert.deepEqual(await store.list(), []);
  assert.deepEqual(JSON.parse(await readFile(store.registryPath, "utf8")), { version: 1, tasks: [] });
});

test("create rejects duplicate names and invalid working directories", async () => {
  const { store } = await fixture();
  await store.create("Codex", "/home/coder/workspace");
  await assert.rejects(() => store.create("Codex", "/home/coder/workspace"), /已存在/);
  await assert.rejects(() => store.create("Other", "relative/path"), /绝对路径/);
});

test("create removes the tmux session when setup fails", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new FailStatusRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner);
  await assert.rejects(() => store.create("失败任务", "/home/coder/workspace"), /status setup failed/);
  assert.deepEqual([...runner.sessions], []);
  assert.deepEqual(await store.list(), []);
});

test("dependency check reports a clear tmux installation error", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const store = new TaskStore(join(dir, "tasks.json"), new MissingTmuxRunner());
  await assert.rejects(() => store.verifyTmux(), /未找到 tmux/);
});

test("create recovers a stale registry lock", async () => {
  const { store } = await fixture();
  const lockPath = `${store.registryPath}.lock`;
  await mkdir(lockPath, { recursive: true });
  const old = new Date(Date.now() - 120_000);
  await utimes(lockPath, old, old);
  const task = await store.create("恢复锁", "/workspace");
  assert.equal(task.name, "恢复锁");
});
