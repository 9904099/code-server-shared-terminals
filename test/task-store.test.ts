import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, open, readdir, readFile, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  CommandRunner,
  InProcessRegistryLock,
  PythonRegistryLock,
  readFileHandleLimited,
  TaskStore,
  TaskStoreDependencies,
  writeFileAtomically,
} from "../src/task-store";

const brokerScript = join(process.cwd(), "scripts", "pty_broker.py");

class FakeRunner implements CommandRunner {
  readonly calls: Array<{ command: string; args: string[] }> = [];
  readonly starts: Array<{ command: string; args: string[]; options: { cwd: string; env: NodeJS.ProcessEnv } }> = [];
  readonly sessions = new Set<string>();
  readonly brokerSockets = new Set<string>();
  readonly socketIdentities = new Map<string, { dev: string; ino: string }>();
  readonly brokerDataByControl = new Map<string, string>();
  readonly cgroups = new Set<string>();
  readonly cgroupIdentities = new Map<string, { dev: string; ino: string }>();
  readonly brokerIdentities = new Map<string, {
    taskId: string;
    pid: number;
    startTicks: number;
    nonce: string;
    dev: string;
    ino: string;
  }>();
  readonly processes = new Map<number, number>();

  async run(command: string, args: string[]): Promise<{ stdout: string }> {
    this.calls.push({ command, args });
    if (args.includes("status")) {
      const socket = args[args.indexOf("--socket") + 1];
      const identity = this.brokerIdentities.get(socket);
      if (!identity) {
        throw new Error("missing broker");
      }
      return {
        stdout: JSON.stringify({
          version: "0.3.0",
          taskId: identity.taskId,
          brokerPid: identity.pid,
          brokerStartTicks: identity.startTicks,
          instanceNonce: identity.nonce,
          controlSocketDev: identity.dev,
          controlSocketIno: identity.ino,
          shellPid: identity.pid + 1,
          shellPgid: identity.pid + 1,
          processBoundary: "cgroup-v2",
          cgroupPath: `/fake/shared-terminal-${identity.taskId}`,
          cgroupDev: this.cgroupIdentities.get(`/fake/shared-terminal-${identity.taskId}`)?.dev,
          cgroupIno: this.cgroupIdentities.get(`/fake/shared-terminal-${identity.taskId}`)?.ino,
        }),
      };
    }
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
    if (args.includes("stop")) {
      const socket = args[args.indexOf("--socket") + 1];
      const identity = this.brokerIdentities.get(socket);
      this.brokerSockets.delete(socket);
      this.socketIdentities.delete(socket);
      const dataSocket = this.brokerDataByControl.get(socket);
      if (dataSocket) {
        this.brokerSockets.delete(dataSocket);
        this.socketIdentities.delete(dataSocket);
        this.brokerDataByControl.delete(socket);
      }
      this.brokerIdentities.delete(socket);
      if (identity) {
        this.processes.delete(identity.pid);
        this.cgroups.delete(`/fake/shared-terminal-${identity.taskId}`);
        this.cgroupIdentities.delete(`/fake/shared-terminal-${identity.taskId}`);
      }
      return {
        stdout: JSON.stringify(identity ? {
          taskId: identity.taskId,
          brokerPid: identity.pid,
          brokerStartTicks: identity.startTicks,
          instanceNonce: identity.nonce,
          controlSocketDev: identity.dev,
          controlSocketIno: identity.ino,
        } : {}),
      };
    }
    return { stdout: "" };
  }

  async start(command: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }): Promise<{ pid: number; startTicks: number }> {
    this.calls.push({ command, args });
    this.starts.push({ command, args, options });
    const socket = args[args.indexOf("--control-socket") + 1];
    const dataSocket = args[args.indexOf("--socket") + 1];
    const pid = 1234 + this.starts.length - 1;
    const startTicks = 5678 + this.starts.length - 1;
    const nonce = args[args.indexOf("--instance-nonce") + 1];
    this.brokerSockets.add(socket);
    this.brokerSockets.add(dataSocket);
    this.socketIdentities.set(socket, { dev: "1", ino: String(pid) });
    this.socketIdentities.set(dataSocket, { dev: "2", ino: String(pid + 10_000) });
    this.brokerDataByControl.set(socket, dataSocket);
    this.brokerIdentities.set(socket, {
      taskId: args[args.indexOf("--task-id") + 1],
      pid,
      startTicks,
      nonce,
      dev: "1",
      ino: String(pid),
    });
    this.processes.set(pid, startTicks);
    const cgroupPath = `/fake/shared-terminal-${args[args.indexOf("--task-id") + 1]}`;
    this.cgroups.add(cgroupPath);
    this.cgroupIdentities.set(cgroupPath, { dev: "3", ino: String(pid + 20_000) });
    return { pid, startTicks };
  }
}

class MissingPythonRunner extends FakeRunner {
  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    if (args.includes("--version")) {
      throw Object.assign(new Error("spawn python3 ENOENT"), { code: "ENOENT" });
    }
    return super.run(command, args);
  }
}

class StubbornBrokerRunner extends FakeRunner {
  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    if (args.includes("stop")) {
      this.calls.push({ command, args });
      const socket = args[args.indexOf("--socket") + 1];
      const identity = this.brokerIdentities.get(socket)!;
      return {
        stdout: JSON.stringify({
          taskId: identity.taskId,
          brokerPid: identity.pid,
          brokerStartTicks: identity.startTicks,
          instanceNonce: identity.nonce,
          controlSocketDev: identity.dev,
          controlSocketIno: identity.ino,
        }),
      };
    }
    return super.run(command, args);
  }
}

class MismatchedStopAckRunner extends FakeRunner {
  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    if (args.includes("stop")) {
      const result = await super.run(command, args);
      return {
        stdout: JSON.stringify({
          ...JSON.parse(result.stdout),
          instanceNonce: "f".repeat(64),
        }),
      };
    }
    return super.run(command, args);
  }
}

class DriftedBrokerRunner extends FakeRunner {
  drift = false;

  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    if (args.includes("status") && this.drift) {
      const result = await super.run(command, args);
      return { stdout: JSON.stringify({ ...JSON.parse(result.stdout), taskId: "different-task" }) };
    }
    return super.run(command, args);
  }
}

class MismatchedStatusRunner extends FakeRunner {
  override async run(command: string, args: string[]): Promise<{ stdout: string }> {
    if (args.includes("status")) {
      const result = await super.run(command, args);
      const status = JSON.parse(result.stdout) as { brokerPid: number };
      return { stdout: JSON.stringify({ ...status, brokerPid: status.brokerPid + 1 }) };
    }
    return super.run(command, args);
  }
}

class MismatchedStartedProcessRunner extends FakeRunner {
  override async start(
    command: string,
    args: string[],
    options: { cwd: string; env: NodeJS.ProcessEnv },
  ): Promise<{ pid: number; startTicks: number }> {
    await super.start(command, args, options);
    return { pid: 4321, startTicks: 8765 };
  }
}

class BlockingStartRunner extends FakeRunner {
  entered!: () => void;
  release!: () => void;
  readonly enteredPromise = new Promise<void>((resolve) => { this.entered = resolve; });
  readonly releasePromise = new Promise<void>((resolve) => { this.release = resolve; });

  override async start(
    command: string,
    args: string[],
    options: { cwd: string; env: NodeJS.ProcessEnv },
  ): Promise<{ pid: number; startTicks: number }> {
    const started = await super.start(command, args, options);
    this.entered();
    await this.releasePromise;
    return started;
  }
}

function fakeDependencies(runner: FakeRunner): TaskStoreDependencies {
  return {
    registryLock: new InProcessRegistryLock(),
    processStartTicks: async (pid) => runner.processes.get(pid),
    socketIdentity: async (path) => runner.socketIdentities.get(path),
    cgroupExists: async (path) => runner.cgroups.has(path),
    cgroupIdentity: async (path) => runner.cgroupIdentities.get(path),
    sleep: async () => undefined,
  };
}

async function fixture() {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new FakeRunner();
  return {
    runner,
    store: new TaskStore(join(dir, "tasks.json"), runner, {
      tmuxPath: "/custom/tmux",
      socketName: "portable-shared-tasks",
      pythonPath: "/usr/bin/python3",
      brokerScriptPath: "/extension/scripts/pty_broker.py",
      socketDirectory: "/tmp/shared-terminals-test",
      shellPath: "/bin/zsh",
      environment: {
        HOME: "/config",
        USER: "coder",
        LOGNAME: "coder",
        PATH: "/custom/bin:/usr/bin",
        SHELL: "/bin/zsh",
      },
    }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner)),
  };
}

test("create registers a persistent task and starts a lightweight PTY broker", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("Codex 主任务", "/home/coder/workspace");

  assert.equal(task.name, "Codex 主任务");
  assert.equal(task.cwd, "/home/coder/workspace");
  assert.match(task.id, /^[a-z0-9-]+$/);
  assert.equal(task.backend, "pty-broker");
  assert.equal(task.brokerPid, 1234);
  assert.equal(task.brokerStartTicks, 5678);
  assert.equal(runner.brokerSockets.has(store.brokerControlSocketPath(task)), true);
  const createCall = runner.starts[0];
  assert.equal(createCall.command, "/usr/bin/python3");
  assert.equal(createCall.args.includes("/extension/scripts/pty_broker.py"), true);
  assert.equal(createCall.args.includes("serve"), true);
  assert.equal(createCall.args.includes("/bin/zsh"), true);
  assert.equal(createCall.options.cwd, "/home/coder/workspace");
  assert.equal(createCall.options.env.HOME, "/config");
  assert.equal(createCall.options.env.USER, "coder");
  assert.equal(createCall.options.env.PATH, "/custom/bin:/usr/bin");
  assert.deepEqual(await store.list(), [task]);
});

test("create durably publishes starting identity before exposing a running broker", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-starting-state-"));
  const runner = new FakeRunner();
  const snapshots: Array<{ tasks: Array<Record<string, unknown>> }> = [];
  const dependencies = fakeDependencies(runner);
  dependencies.registryWriter = async (path, content) => {
    snapshots.push(JSON.parse(content) as { tasks: Array<Record<string, unknown>> });
    await writeFileAtomically(path, content);
  };
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), dependencies);

  const task = await store.create("持久启动登记", "/workspace");

  assert.equal(snapshots.length, 3);
  assert.equal(snapshots[0].tasks[0].brokerState, "starting");
  assert.equal(snapshots[0].tasks[0].brokerPid, undefined);
  assert.equal(snapshots[1].tasks[0].brokerState, "starting");
  assert.equal(snapshots[1].tasks[0].brokerPid, task.brokerPid);
  assert.equal(snapshots[2].tasks[0].brokerState, "running");
  assert.equal(snapshots[2].tasks[0].brokerNonce, task.brokerNonce);
});

test("create persists complete recovery identity before a failed broker cleanup", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-publish-cleanup-failure-"));
  const runner = new FakeRunner();
  const originalRun = runner.run.bind(runner);
  runner.run = async (command, args) => {
    if (!args.includes("stop")) {
      return originalRun(command, args);
    }
    const controlPath = args[args.indexOf("--socket") + 1];
    const dataPath = runner.brokerDataByControl.get(controlPath)!;
    const dataIdentity = runner.socketIdentities.get(dataPath)!;
    const brokerIdentity = runner.brokerIdentities.get(controlPath)!;
    const result = await originalRun(command, args);
    runner.brokerSockets.add(dataPath);
    runner.socketIdentities.set(dataPath, dataIdentity);
    const cgroupPath = `/fake/shared-terminal-${brokerIdentity.taskId}`;
    runner.cgroups.add(cgroupPath);
    runner.cgroupIdentities.set(cgroupPath, { dev: "3", ino: String(brokerIdentity.pid + 20_000) });
    return result;
  };
  let writes = 0;
  const dependencies = fakeDependencies(runner);
  dependencies.registryWriter = async (path, content) => {
    writes += 1;
    if (writes === 3) {
      throw new Error("injected running publication failure");
    }
    await writeFileAtomically(path, content);
  };
  const registryPath = join(dir, "tasks.json");
  const store = new TaskStore(registryPath, runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), dependencies);

  await assert.rejects(
    () => store.create("发布回滚失败", "/workspace"),
    /注册表发布失败，且无法确认 Broker 已完整回收/,
  );

  const durable = JSON.parse(await readFile(registryPath, "utf8")) as {
    tasks: Array<Record<string, unknown>>;
  };
  assert.equal(durable.tasks.length, 1);
  assert.equal(durable.tasks[0].brokerState, "starting");
  assert.equal(durable.tasks[0].brokerPid, 1234);
  assert.equal(durable.tasks[0].brokerStartTicks, 5678);
  assert.equal(durable.tasks[0].brokerDataDev, "2");
  assert.equal(durable.tasks[0].brokerDataIno, "11234");
  assert.equal(durable.tasks[0].brokerControlDev, "1");
  assert.equal(durable.tasks[0].brokerControlIno, "1234");
  assert.match(String(durable.tasks[0].brokerCgroupPath), /shared-terminal-/);
  assert.equal(durable.tasks[0].brokerCgroupDev, "3");
  assert.equal(durable.tasks[0].brokerCgroupIno, "21234");
});

test("list recovers a live broker from a durable starting row after host restart", async () => {
  const { runner, store } = await fixture();
  const taskId = "recover-after-host-exit";
  const controlPath = "/tmp/shared-terminals-test/t-recovery.ctl";
  const dataPath = "/tmp/shared-terminals-test/t-recovery.sock";
  runner.brokerSockets.add(controlPath);
  runner.brokerSockets.add(dataPath);
  runner.socketIdentities.set(controlPath, { dev: "7", ino: "11" });
  runner.socketIdentities.set(dataPath, { dev: "8", ino: "12" });
  runner.brokerIdentities.set(controlPath, {
    taskId,
    pid: 2468,
    startTicks: 13579,
    nonce: "b".repeat(64),
    dev: "7",
    ino: "11",
  });
  runner.processes.set(2468, 13579);
  runner.cgroups.add(`/fake/shared-terminal-${taskId}`);
  runner.cgroupIdentities.set(`/fake/shared-terminal-${taskId}`, { dev: "3", ino: "22468" });
  await writeFile(store.registryPath, `${JSON.stringify({
    version: 1,
    tasks: [{
      id: taskId,
      name: "恢复任务",
      cwd: "/workspace",
      session: `shared-${taskId}`,
      createdAt: "2026-08-09T00:00:00.000Z",
      open: true,
      backend: "pty-broker",
      brokerState: "starting",
      brokerPid: 2468,
      brokerStartTicks: 13579,
      brokerNonce: "b".repeat(64),
      brokerDataPath: dataPath,
      brokerControlPath: controlPath,
    }],
  }, null, 2)}\n`);

  const [recovered] = await store.list();

  assert.equal(recovered.brokerState, "running");
  assert.equal(recovered.brokerNonce, "b".repeat(64));
  assert.equal(recovered.brokerDataDev, "8");
  assert.equal(recovered.brokerDataIno, "12");
  assert.equal(recovered.brokerControlDev, "7");
  assert.equal(recovered.brokerControlIno, "11");
  assert.equal(JSON.parse(await readFile(store.registryPath, "utf8")).tasks[0].brokerState, "running");
});

test("list never adopts a socket responder for a starting row without durable PID identity", async () => {
  const { runner, store } = await fixture();
  const taskId = "unbound-starting-row";
  const controlPath = "/tmp/shared-terminals-test/t-unbound.ctl";
  const dataPath = "/tmp/shared-terminals-test/t-unbound.sock";
  runner.brokerSockets.add(controlPath);
  runner.brokerSockets.add(dataPath);
  runner.socketIdentities.set(controlPath, { dev: "17", ino: "21" });
  runner.socketIdentities.set(dataPath, { dev: "18", ino: "22" });
  runner.brokerIdentities.set(controlPath, {
    taskId,
    pid: 9753,
    startTicks: 8642,
    nonce: "c".repeat(64),
    dev: "17",
    ino: "21",
  });
  runner.processes.set(9753, 8642);
  runner.cgroups.add(`/fake/shared-terminal-${taskId}`);
  runner.cgroupIdentities.set(`/fake/shared-terminal-${taskId}`, { dev: "3", ino: "29753" });
  await writeFile(store.registryPath, `${JSON.stringify({
    version: 1,
    tasks: [{
      id: taskId,
      name: "未绑定启动任务",
      cwd: "/workspace",
      session: `shared-${taskId}`,
      createdAt: "2026-08-09T00:00:00.000Z",
      open: true,
      backend: "pty-broker",
      brokerState: "starting",
      brokerNonce: "c".repeat(64),
      brokerDataPath: dataPath,
      brokerControlPath: controlPath,
    }],
  }, null, 2)}\n`);

  const [preserved] = await store.list();

  assert.equal(preserved.brokerState, "starting");
  assert.equal(preserved.brokerPid, undefined);
  assert.equal(preserved.brokerStartTicks, undefined);
  assert.equal(runner.calls.some(({ args }) => args.includes("status")), false);
  assert.equal(JSON.parse(await readFile(store.registryPath, "utf8")).tasks[0].brokerState, "starting");
});

test("new tasks use the lightweight PTY broker instead of tmux", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("快速终端", "/home/coder/workspace");

  assert.equal(task.backend, "pty-broker");
  const startCall = runner.calls.find(({ args }) => args.includes("serve"));
  assert.equal(startCall?.command, "/usr/bin/python3");
  assert.equal(startCall?.args.includes("/extension/scripts/pty_broker.py"), true);
  assert.equal(startCall?.args.includes(store.brokerSocketPath(task)), true);
  assert.equal(startCall?.args.includes("--require-cgroup"), true);
  assert.equal(runner.calls.some(({ args }) => args.includes("new-session")), false);
});

test("default terminal profile creates stable unique task names without prompting", async () => {
  const { store } = await fixture();

  const first = await store.createAutomatic("/home/coder/workspace");
  const second = await store.createAutomatic("/home/coder/workspace");

  assert.equal(first.name, "终端 1");
  assert.equal(second.name, "终端 2");
  assert.equal(first.backend, "pty-broker");
  assert.equal(second.backend, "pty-broker");
});

test("create enforces the configured per-user task ceiling before spawning", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-limit-"));
  const runner = new FakeRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
    maxTasks: 1,
    maxClientsPerTask: 4,
    replayBytes: 65536,
    maxClientInputBytes: 65536,
    maxClientOutputBytes: 262144,
    maxPtyInputBytes: 65536,
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));

  await store.create("唯一终端", "/workspace");
  await assert.rejects(() => store.create("超限终端", "/workspace"), /最多允许 1 个共享终端/);
  assert.equal(runner.starts.length, 1);
});

test("registry loading rejects oversized files, excessive rows, and invalid task fields", async () => {
  const { store } = await fixture();
  await writeFile(store.registryPath, Buffer.alloc(1024 * 1024 + 1, 0x20));
  await assert.rejects(() => store.list(), /注册表超过/);

  const rows = Array.from({ length: 13 }, (_, index) => ({
    id: `task-${index}`,
    name: `task ${index}`,
    cwd: "/workspace",
    session: `shared-task-${index}`,
    createdAt: "2026-08-09T00:00:00.000Z",
    open: true,
    backend: "tmux",
  }));
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: rows })}\n`);
  await assert.rejects(() => store.list(), /任务数量超过 12/);

  rows.length = 1;
  rows[0].cwd = "relative/workspace";
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: rows })}\n`);
  await assert.rejects(() => store.list(), /工作目录无效/);

  rows[0].cwd = "/workspace";
  rows[0].open = "false" as unknown as boolean;
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: rows })}\n`);
  await assert.rejects(() => store.list(), /open 无效/);

  rows[0].open = true;
  rows[0].backend = "pty-broker";
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: rows })}\n`);
  await assert.rejects(() => store.list(), /Broker 身份不完整/);

  rows[0].backend = "tmux";
  (rows[0] as Record<string, unknown>).unexpected = "must-not-be-accepted";
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: rows })}\n`);
  await assert.rejects(() => store.list(), /未知字段/);

  delete (rows[0] as Record<string, unknown>).unexpected;
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: rows, unexpected: true })}\n`);
  await assert.rejects(() => store.list(), /未知字段/);
});

test("bounded registry reads stop after maxBytes plus one even when the file grows after stat", async () => {
  const bytes = Buffer.from("0123456789abcdef", "utf8");
  let position = 0;
  const growingHandle = {
    async read(buffer: Buffer, offset: number, length: number): Promise<{ bytesRead: number; buffer: Buffer }> {
      const bytesRead = Math.min(length, bytes.length - position);
      bytes.copy(buffer, offset, position, position + bytesRead);
      position += bytesRead;
      return { bytesRead, buffer };
    },
  };

  await assert.rejects(
    () => readFileHandleLimited(growingHandle, 8),
    /注册表超过 8 字节/,
  );
  assert.equal(position, 9);
});

test("list reports whether each server session is alive", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("日志", "/home/coder/workspace");
  assert.equal((await store.listWithStatus())[0].alive, true);
  const controlSocketPath = store.brokerControlSocketPath(task);
  runner.brokerSockets.delete(controlSocketPath);
  runner.brokerIdentities.delete(controlSocketPath);
  assert.equal((await store.listWithStatus())[0].alive, false);
});

test("rename changes the shared label without replacing the session", async () => {
  const { store } = await fixture();
  const task = await store.create("旧名称", "/home/coder/workspace");
  const renamed = await store.rename(task.id, "后端服务");
  assert.equal(renamed.name, "后端服务");
  assert.equal(renamed.session, task.session);
});

test("closing and reopening a task updates the shared visibility state", async () => {
  const { store } = await fixture();
  const task = await store.create("同步关闭", "/home/coder/workspace");

  assert.equal(task.open, true);
  assert.equal((await store.setOpen(task.id, false)).open, false);
  assert.equal((await store.list())[0].open, false);
  assert.equal((await store.setOpen(task.id, true)).open, true);
});

test("remote terminal close is idempotent after another window deletes the task", async () => {
  const { store } = await fixture();
  const task = await store.create("删除收敛", "/home/coder/workspace");
  await store.delete(task.id);

  assert.equal(await store.closeIfPresent(task.id), false);
  assert.deepEqual(await store.list(), []);
});

test("delete stops the broker session and removes the registry entry", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("临时任务", "/home/coder/workspace");
  await store.delete(task.id);
  assert.equal(runner.brokerSockets.has(store.brokerControlSocketPath(task)), false);
  assert.deepEqual(await store.list(), []);
  assert.deepEqual(JSON.parse(await readFile(store.registryPath, "utf8")), { version: 1, tasks: [] });
});

test("delete keeps the registry row when the broker refuses to stop", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new StubbornBrokerRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));
  const task = await store.create("拒绝停止", "/workspace");

  await assert.rejects(() => store.delete(task.id), /停止超时/);
  assert.equal((await store.list())[0].id, task.id);
});

test("delete keeps the registry row when the stop acknowledgement identity differs", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-stop-ack-"));
  const runner = new MismatchedStopAckRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));
  const task = await store.create("停止 ACK 漂移", "/workspace");

  await assert.rejects(() => store.delete(task.id), /停止确认身份不匹配/);
  assert.equal((await store.list())[0].id, task.id);
});

test("delete fails closed when the live broker identity does not match the registry", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new DriftedBrokerRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));
  const task = await store.create("身份漂移", "/workspace");
  runner.drift = true;

  await assert.rejects(() => store.delete(task.id), /身份不匹配/);
  assert.equal((await store.list())[0].id, task.id);
  assert.equal(runner.calls.some(({ args }) => args.includes("stop")), false);
});

test("create preserves a recovery row and does not signal an unauthenticated broker identity", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-start-identity-"));
  const runner = new MismatchedStartedProcessRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));

  await assert.rejects(() => store.create("启动身份漂移", "/workspace"), /身份尚未认证.*保留 starting 登记/);
  const preserved = await store.list();
  assert.equal(preserved.length, 1);
  assert.equal(preserved[0].brokerState, "starting");
  assert.equal(preserved[0].brokerPid, 4321);
  assert.equal(runner.calls.some(({ args }) => args.includes("stop")), false);
});

test("create uses a prepublished nonce to stop the exact broker when status identity drifts", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-start-status-drift-"));
  const runner = new MismatchedStatusRunner();
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));

  await assert.rejects(() => store.create("状态漂移回收", "/workspace"), /启动后身份不匹配/);
  assert.deepEqual(await store.list(), []);
  assert.equal(runner.calls.some(({ args }) => args.includes("stop")), true);
  assert.equal(runner.processes.size, 0);
  assert.equal(runner.brokerSockets.size, 0);
});

test("create never TERM/KILLs a broker before authenticated stop identity exists", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-start-cleanup-confirmation-"));
  const runner = new MismatchedStartedProcessRunner();
  const kills: Array<{ pid: number; signal: NodeJS.Signals }> = [];
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), {
    registryLock: new InProcessRegistryLock(),
    processStartTicks: async () => 8765,
    socketIdentity: async (path) => {
      const identity = runner.brokerIdentities.get(path);
      return identity ? { dev: identity.dev, ino: identity.ino } : undefined;
    },
    cgroupExists: async (path) => runner.cgroups.has(path),
    killProcess: (pid, signal) => { kills.push({ pid, signal }); },
    sleep: async () => undefined,
  });

  await assert.rejects(() => store.create("清理确认", "/workspace"), /身份尚未认证.*保留 starting 登记/);
  assert.deepEqual(kills, []);
  const preserved = await store.list();
  assert.equal(preserved.length, 1);
  assert.equal(preserved[0].brokerState, "starting");
  assert.equal(preserved[0].brokerPid, 4321);
});

test("delete preserves the registry when the broker exits but its data socket remains", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("数据 socket 泄漏", "/workspace");
  const dataIdentity = runner.socketIdentities.get(task.brokerDataPath!)!;
  const originalRun = runner.run.bind(runner);
  runner.run = async (command, args) => {
    const result = await originalRun(command, args);
    if (args.includes("stop")) {
      runner.brokerSockets.add(task.brokerDataPath!);
      runner.socketIdentities.set(task.brokerDataPath!, dataIdentity);
    }
    return result;
  };

  await assert.rejects(() => store.delete(task.id), /数据 socket.*仍存在|停止超时/);
  assert.equal((await store.list())[0].id, task.id);
});

test("delete preserves the registry when the broker exits but its cgroup remains", async () => {
  const { runner, store } = await fixture();
  const task = await store.create("cgroup 泄漏", "/workspace");
  const cgroupPath = task.brokerCgroupPath!;
  const cgroupIdentity = runner.cgroupIdentities.get(cgroupPath)!;
  const originalRun = runner.run.bind(runner);
  runner.run = async (command, args) => {
    const result = await originalRun(command, args);
    if (args.includes("stop")) {
      runner.cgroups.add(cgroupPath);
      runner.cgroupIdentities.set(cgroupPath, cgroupIdentity);
    }
    return result;
  };

  await assert.rejects(() => store.delete(task.id), /停止超时/);
  assert.equal((await store.list())[0].id, task.id);
});

test("delete retries identity-bound remnant cleanup after the broker has exited", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-remnant-recovery-"));
  const runner = new FakeRunner();
  const taskId = "recover-remnants";
  const dataPath = "/tmp/shared-terminals-test/t-remnant.sock";
  const controlPath = "/tmp/shared-terminals-test/t-remnant.ctl";
  const cgroupPath = `/fake/shared-terminal-${taskId}`;
  runner.brokerSockets.add(dataPath);
  runner.socketIdentities.set(dataPath, { dev: "21", ino: "31" });
  runner.cgroups.add(cgroupPath);
  runner.cgroupIdentities.set(cgroupPath, { dev: "23", ino: "33" });
  const originalRun = runner.run.bind(runner);
  runner.run = async (command, args) => {
    if (!args.includes("recover")) {
      return originalRun(command, args);
    }
    runner.calls.push({ command, args });
    runner.brokerSockets.delete(dataPath);
    runner.socketIdentities.delete(dataPath);
    runner.cgroups.delete(cgroupPath);
    return {
      stdout: JSON.stringify({
        taskId,
        brokerPid: 2468,
        brokerStartTicks: 13579,
        dataSocketDev: "21",
        dataSocketIno: "31",
        controlSocketDev: "22",
        controlSocketIno: "32",
        cgroupPath,
        cgroupDev: "23",
        cgroupIno: "33",
      }),
    };
  };
  const registryPath = join(dir, "tasks.json");
  await writeFile(registryPath, `${JSON.stringify({
    version: 1,
    tasks: [{
      id: taskId,
      name: "恢复残留",
      cwd: "/workspace",
      session: `shared-${taskId}`,
      createdAt: "2026-08-09T00:00:00.000Z",
      open: true,
      backend: "pty-broker",
      brokerState: "starting",
      brokerPid: 2468,
      brokerStartTicks: 13579,
      brokerNonce: "d".repeat(64),
      brokerDataPath: dataPath,
      brokerDataDev: "21",
      brokerDataIno: "31",
      brokerControlPath: controlPath,
      brokerControlDev: "22",
      brokerControlIno: "32",
      brokerCgroupPath: cgroupPath,
      brokerCgroupDev: "23",
      brokerCgroupIno: "33",
    }],
  }, null, 2)}\n`);
  const store = new TaskStore(registryPath, runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => runner.brokerSockets.has(socketPath), fakeDependencies(runner));

  await store.delete(taskId);

  assert.equal(runner.calls.some(({ args }) => args.includes("recover")), true);
  assert.deepEqual(await store.list(), []);
});

test("delete preserves the registry when one transient false probe occurs but broker identity remains alive", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-stop-unknown-"));
  const runner = new StubbornBrokerRunner();
  let probes = 0;
  const store = new TaskStore(join(dir, "tasks.json"), runner, {
    tmuxPath: "/custom/tmux",
    socketName: "portable-shared-tasks",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: "/extension/scripts/pty_broker.py",
    socketDirectory: "/tmp/shared-terminals-test",
    shellPath: "/bin/zsh",
    environment: {},
  }, async (socketPath) => {
    probes += 1;
    return probes === 2 ? false : runner.brokerSockets.has(socketPath);
  }, fakeDependencies(runner));
  const task = await store.create("停止状态未知", "/workspace");

  await assert.rejects(() => store.delete(task.id), /停止超时|状态未知/);
  assert.equal((await store.list())[0].id, task.id);
});

test("create rejects duplicate names and invalid working directories", async () => {
  const { store } = await fixture();
  await store.create("Codex", "/home/coder/workspace");
  await assert.rejects(() => store.create("Codex", "/home/coder/workspace"), /已存在/);
  await assert.rejects(() => store.create("Other", "relative/path"), /绝对路径/);
});

test("create stops the broker when it never becomes ready", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const runner = new FakeRunner();
  const store = new TaskStore(
    join(dir, "tasks.json"),
    runner,
    undefined,
    async () => false,
    fakeDependencies(runner),
  );
  await assert.rejects(() => store.create("失败任务", "/home/coder/workspace"), /启动超时/);
  assert.equal(runner.calls.some(({ args }) => args.includes("stop")), true);
  assert.deepEqual(await store.list(), []);
});

test("dependency check reports a clear Python installation error", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-"));
  const store = new TaskStore(join(dir, "tasks.json"), new MissingPythonRunner());
  await assert.rejects(() => store.verifyBroker(), /未找到 Python 3/);
});

test("legacy registry rows continue to use their existing tmux sessions", async () => {
  const { runner, store } = await fixture();
  const legacy = {
    id: "legacy",
    name: "旧任务",
    cwd: "/workspace",
    session: "shared-legacy",
    createdAt: "2026-07-14T00:00:00.000Z",
    open: true,
  };
  await writeFile(store.registryPath, `${JSON.stringify({ version: 1, tasks: [legacy] })}\n`);
  runner.sessions.add(legacy.session);

  const listed = await store.listWithStatus();
  assert.equal(listed[0].backend, "tmux");
  assert.equal(listed[0].alive, true);
  await store.delete(legacy.id);
  assert.equal(runner.sessions.has(legacy.session), false);
});

test("an unlocked persistent registry lock file can be reused regardless of mtime", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-lock-reuse-"));
  const lockPath = join(dir, "tasks.json.lock");
  await writeFile(lockPath, "stale owner metadata\n");
  const old = new Date(Date.now() - 120_000);
  await utimes(lockPath, old, old);
  const lock = new PythonRegistryLock("/usr/bin/python3", brokerScript);
  let entered = false;
  await lock.withLock(lockPath, async () => { entered = true; });
  assert.equal(entered, true);
});

test("a live registry lock cannot be stolen by age or released by an older owner", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-lock-owner-"));
  const lockPath = join(dir, "tasks.json.lock");
  const first = new PythonRegistryLock("/usr/bin/python3", brokerScript);
  const second = new PythonRegistryLock("/usr/bin/python3", brokerScript);
  const third = new PythonRegistryLock("/usr/bin/python3", brokerScript);
  let enterFirst!: () => void;
  let releaseFirst!: () => void;
  const firstEntered = new Promise<void>((resolve) => { enterFirst = resolve; });
  const firstRelease = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const firstHold = first.withLock(lockPath, async () => {
    enterFirst();
    await firstRelease;
  });
  await firstEntered;
  const old = new Date(Date.now() - 120_000);
  await utimes(lockPath, old, old);

  let enterSecond!: () => void;
  let releaseSecond!: () => void;
  const secondEntered = new Promise<void>((resolve) => { enterSecond = resolve; });
  const secondRelease = new Promise<void>((resolve) => { releaseSecond = resolve; });
  let secondActive = false;
  const secondHold = second.withLock(lockPath, async () => {
    secondActive = true;
    enterSecond();
    await secondRelease;
  });
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(secondActive, false, "second writer stole a live lock after its mtime aged");

  releaseFirst();
  await firstHold;
  await secondEntered;

  let thirdActive = false;
  const thirdHold = third.withLock(lockPath, async () => { thirdActive = true; });
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(thirdActive, false, "older owner release allowed a third writer to bypass the second lock");

  releaseSecond();
  await secondHold;
  await thirdHold;
  assert.equal(thirdActive, true);
});

test("registry flock remains held by the Node file descriptor after the helper exits", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-lock-fd-"));
  const lockPath = join(dir, "tasks.json.lock");
  const first = new PythonRegistryLock("/usr/bin/python3", brokerScript);
  const second = new PythonRegistryLock("/usr/bin/python3", brokerScript);
  let releaseFirst!: () => void;
  const firstRelease = new Promise<void>((resolve) => { releaseFirst = resolve; });
  let firstEntered!: () => void;
  const firstEnteredPromise = new Promise<void>((resolve) => { firstEntered = resolve; });

  const firstHold = first.withLock(lockPath, async () => {
    const owner = JSON.parse(await readFile(lockPath, "utf8")) as { pid: number };
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      try {
        await readFile(`/proc/${owner.pid}/stat`, "utf8");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          break;
        }
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    await assert.rejects(() => readFile(`/proc/${owner.pid}/stat`, "utf8"), { code: "ENOENT" });
    firstEntered();
    await firstRelease;
  });
  await firstEnteredPromise;

  let secondActive = false;
  const secondHold = second.withLock(lockPath, async () => { secondActive = true; });
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(secondActive, false);
  releaseFirst();
  await Promise.all([firstHold, secondHold]);
  assert.equal(secondActive, true);
});

test("lock helper leaves the shared open-file-description locked until the Node descriptor closes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-lock-ofd-"));
  const lockPath = join(dir, "tasks.json.lock");
  const file = await open(lockPath, "w+", 0o600);
  const helper = spawn("/usr/bin/python3", [
    brokerScript,
    "lock",
    "--fd", "3",
    "--timeout-seconds", "1",
  ], { stdio: ["ignore", "pipe", "pipe", file.fd] });
  let stdout = "";
  helper.stdout!.setEncoding("utf8");
  helper.stdout!.on("data", (chunk: string) => { stdout += chunk; });
  const exitCode = await new Promise<number | null>((resolve, reject) => {
    helper.once("error", reject);
    helper.once("exit", resolve);
  });
  assert.equal(exitCode, 0);
  assert.equal(stdout.trim(), "LOCKED");

  const probe = async (): Promise<string> => {
    const child = spawn("/usr/bin/python3", ["-c", [
      "import fcntl, os, sys",
      "fd = os.open(sys.argv[1], os.O_RDWR)",
      "try:",
      "    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)",
      "except BlockingIOError:",
      "    print('BLOCKED')",
      "else:",
      "    print('ACQUIRED')",
    ].join("\n"), lockPath], { stdio: ["ignore", "pipe", "inherit"] });
    let output = "";
    child.stdout!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => { output += chunk; });
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", resolve);
    });
    assert.equal(code, 0);
    return output.trim();
  };

  assert.equal(await probe(), "BLOCKED");
  await file.close();
  assert.equal(await probe(), "ACQUIRED");
});

test("atomic registry write closes file handles and removes temporary files after rename failure", async () => {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminals-atomic-write-"));
  const target = join(dir, "tasks.json");
  await mkdir(target);
  const fdBaseline = (await readdir("/proc/self/fd")).length;

  for (let attempt = 0; attempt < 20; attempt += 1) {
    await assert.rejects(() => writeFileAtomically(target, "{}\n"));
  }

  const leftovers = (await readdir(dir)).filter((name) => name.endsWith(".tmp"));
  const fdAfter = (await readdir("/proc/self/fd")).length;
  assert.deepEqual(leftovers, []);
  assert.ok(fdAfter <= fdBaseline + 1, `${JSON.stringify({ fdBaseline, fdAfter })}`);
});
