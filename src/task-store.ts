import { execFile, spawn } from "node:child_process";
import { constants } from "node:fs";
import { lstat, mkdir, open, readFile, rename, rm } from "node:fs/promises";
import { createConnection } from "node:net";
import { isAbsolute, dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { promisify } from "node:util";

import { TaskRuntimeConfig } from "./runtime-config";

const execFileAsync = promisify(execFile);
const defaultRuntime: TaskRuntimeConfig = {
  tmuxPath: "tmux",
  socketName: "code-server-shared-tasks",
  pythonPath: "python3",
  brokerScriptPath: "",
  socketDirectory: "/tmp",
  shellPath: process.env.SHELL || "/bin/sh",
  maxTasks: 12,
  maxClientsPerTask: 4,
  replayBytes: 512 * 1024,
  maxClientInputBytes: 256 * 1024,
  maxClientOutputBytes: 2 * 1024 * 1024,
  maxPtyInputBytes: 256 * 1024,
  environment: Object.fromEntries(
    ["HOME", "USER", "LOGNAME", "PATH", "SHELL", "CODEX_HOME"]
      .flatMap((key) => process.env[key] ? [[key, process.env[key] as string]] : []),
  ),
};

export interface SharedTask {
  id: string;
  name: string;
  cwd: string;
  session: string;
  createdAt: string;
  open: boolean;
  backend?: "tmux" | "pty-broker";
  brokerState?: "starting" | "running";
  brokerPid?: number;
  brokerStartTicks?: number;
  brokerNonce?: string;
  brokerDataPath?: string;
  brokerDataDev?: string;
  brokerDataIno?: string;
  brokerControlPath?: string;
  brokerControlDev?: string;
  brokerControlIno?: string;
  brokerCgroupPath?: string;
  brokerCgroupDev?: string;
  brokerCgroupIno?: string;
}

interface BrokerStatus {
  version: string;
  taskId: string;
  brokerPid: number;
  brokerStartTicks: number;
  instanceNonce: string;
  controlSocketDev: string;
  controlSocketIno: string;
  shellPid: number;
  shellPgid: number;
  processBoundary: "cgroup-v2" | "subreaper";
  cgroupPath: string;
  cgroupDev: string;
  cgroupIno: string;
}

interface BrokerStopAck {
  taskId: string;
  brokerPid: number;
  brokerStartTicks: number;
  instanceNonce: string;
  controlSocketDev: string;
  controlSocketIno: string;
}

interface BrokerRecoveryAck {
  taskId: string;
  brokerPid: number;
  brokerStartTicks: number;
  dataSocketDev: string;
  dataSocketIno: string;
  controlSocketDev: string;
  controlSocketIno: string;
  cgroupPath: string;
  cgroupDev: string;
  cgroupIno: string;
}

export interface SocketIdentity {
  dev: string;
  ino: string;
}

export interface SharedTaskStatus extends SharedTask {
  alive: boolean;
}

interface Registry {
  version: 1;
  tasks: SharedTask[];
}

export interface CommandRunner {
  run(command: string, args: string[]): Promise<{ stdout: string }>;
  start(
    command: string,
    args: string[],
    options: { cwd: string; env: NodeJS.ProcessEnv },
  ): Promise<StartedProcess>;
}

export interface StartedProcess {
  pid: number;
  startTicks: number;
  releaseStartup?: () => void;
  cancelStartup?: () => void;
}

export async function processStartTicks(pid: number): Promise<number | undefined> {
  try {
    const statLine = await readFile(`/proc/${pid}/stat`, "utf8");
    const commandEnd = statLine.lastIndexOf(")");
    if (commandEnd < 0) {
      return undefined;
    }
    const fieldsAfterCommand = statLine.slice(commandEnd + 2).trim().split(/\s+/);
    if (fieldsAfterCommand[0] === "Z" || fieldsAfterCommand[0] === "X") {
      return undefined;
    }
    const value = Number(fieldsAfterCommand[19]);
    return Number.isInteger(value) && value > 0 ? value : undefined;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

export async function readSocketIdentity(path: string): Promise<SocketIdentity | undefined> {
  try {
    const info = await lstat(path, { bigint: true });
    const expectedUid = process.getuid?.();
    if (!info.isSocket()) {
      throw new Error(`快速共享终端控制路径不是 Unix socket：${path}`);
    }
    if (expectedUid !== undefined && info.uid !== BigInt(expectedUid)) {
      throw new Error(`快速共享终端控制 socket 不属于当前用户：${path}`);
    }
    if ((info.mode & 0o077n) !== 0n) {
      throw new Error(`快速共享终端控制 socket 权限不是 0600：${path}`);
    }
    return { dev: info.dev.toString(), ino: info.ino.toString() };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

export interface RegistryLock {
  withLock<T>(lockPath: string, action: () => Promise<T>): Promise<T>;
}

export class PythonRegistryLock implements RegistryLock {
  constructor(
    private readonly pythonPath: string,
    private readonly brokerScriptPath: string,
  ) {}

  async withLock<T>(lockPath: string, action: () => Promise<T>): Promise<T> {
    await mkdir(dirname(lockPath), { recursive: true });
    const noFollow = constants.O_NOFOLLOW ?? 0;
    const file = await open(lockPath, constants.O_CREAT | constants.O_RDWR | noFollow, 0o600);
    try {
      const info = await file.stat();
      const expectedUid = process.getuid?.();
      if (!info.isFile() || (expectedUid !== undefined && info.uid !== expectedUid)) {
        throw new Error(`共享终端注册表锁不是当前用户拥有的普通文件：${lockPath}`);
      }
      await file.chmod(0o600);
      const child = spawn(this.pythonPath, [
        this.brokerScriptPath,
        "lock",
        "--fd", "3",
        "--timeout-seconds", "10",
      ], { stdio: ["ignore", "pipe", "pipe", file.fd] });
      let stdout = "";
      let stderr = "";
      child.stdout!.setEncoding("utf8");
      child.stderr!.setEncoding("utf8");
      child.stdout!.on("data", (chunk: string) => { stdout += chunk; });
      child.stderr!.on("data", (chunk: string) => { stderr += chunk; });
      const exitCode = await new Promise<number | null>((resolve, reject) => {
        const timeout = setTimeout(() => child.kill("SIGKILL"), 12_000);
        child.once("error", (error) => {
          clearTimeout(timeout);
          reject(error);
        });
        child.once("exit", (code) => {
          clearTimeout(timeout);
          resolve(code);
        });
      });
      if (exitCode !== 0 || stdout.trim() !== "LOCKED") {
        throw new Error(`共享终端注册表锁获取失败（code=${exitCode}）：${stderr.trim() || lockPath}`);
      }
      const pathInfo = await lstat(lockPath);
      if (pathInfo.dev !== info.dev || pathInfo.ino !== info.ino) {
        throw new Error(`共享终端注册表锁文件身份漂移：${lockPath}`);
      }
      return await action();
    } finally {
      await file.close();
    }
  }
}

export class InProcessRegistryLock implements RegistryLock {
  private static readonly tails = new Map<string, Promise<void>>();

  async withLock<T>(lockPath: string, action: () => Promise<T>): Promise<T> {
    const previous = InProcessRegistryLock.tails.get(lockPath) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => { release = resolve; });
    const tail = previous.then(() => current);
    InProcessRegistryLock.tails.set(lockPath, tail);
    await previous;
    try {
      return await action();
    } finally {
      release();
      if (InProcessRegistryLock.tails.get(lockPath) === tail) {
        InProcessRegistryLock.tails.delete(lockPath);
      }
    }
  }
}

export interface TaskStoreDependencies {
  registryLock?: RegistryLock;
  processStartTicks?: (pid: number) => Promise<number | undefined>;
  socketIdentity?: (path: string) => Promise<SocketIdentity | undefined>;
  killProcess?: (pid: number, signal: NodeJS.Signals) => void;
  sleep?: (milliseconds: number) => Promise<void>;
  registryWriter?: (path: string, content: string) => Promise<void>;
  cgroupExists?: (relativePath: string) => Promise<boolean>;
  cgroupIdentity?: (relativePath: string) => Promise<SocketIdentity | undefined>;
}

interface LimitedReadableFile {
  read(
    buffer: Buffer,
    offset: number,
    length: number,
    position: number | null,
  ): Promise<{ bytesRead: number }>;
}

export async function readFileHandleLimited(file: LimitedReadableFile, maxBytes: number): Promise<string> {
  const chunks: Buffer[] = [];
  let total = 0;
  while (total <= maxBytes) {
    const chunk = Buffer.allocUnsafe(Math.min(64 * 1024, maxBytes + 1 - total));
    const { bytesRead } = await file.read(chunk, 0, chunk.length, null);
    if (bytesRead === 0) {
      return Buffer.concat(chunks, total).toString("utf8");
    }
    chunks.push(chunk.subarray(0, bytesRead));
    total += bytesRead;
    if (total > maxBytes) {
      throw new Error(`共享终端注册表超过 ${maxBytes} 字节`);
    }
  }
  throw new Error(`共享终端注册表超过 ${maxBytes} 字节`);
}

export async function writeFileAtomically(path: string, content: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  let file: Awaited<ReturnType<typeof open>> | undefined;
  try {
    file = await open(temporaryPath, "wx", 0o600);
    try {
      await file.writeFile(content, "utf8");
      await file.sync();
    } finally {
      await file.close();
      file = undefined;
    }
    await rename(temporaryPath, path);
  } finally {
    await file?.close().catch(() => undefined);
    await rm(temporaryPath, { force: true }).catch(() => undefined);
  }
}

export class ProcessRunner implements CommandRunner {
  async run(command: string, args: string[]): Promise<{ stdout: string }> {
    const result = await execFileAsync(command, args, { encoding: "utf8" });
    return { stdout: result.stdout };
  }

  async start(command: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }): Promise<StartedProcess> {
    return new Promise<StartedProcess>((resolve, reject) => {
      const gated = args.includes("--startup-gate-fd");
      const child = spawn(command, args, {
        ...options,
        detached: true,
        stdio: gated ? ["ignore", "ignore", "ignore", "pipe"] : "ignore",
      });
      child.once("error", reject);
      child.once("spawn", async () => {
        try {
          if (!child.pid) {
            throw new Error("快速共享终端 Broker 未返回 PID");
          }
          const startTicks = await processStartTicks(child.pid);
          if (!startTicks) {
            child.kill("SIGTERM");
            throw new Error("无法核对快速共享终端 Broker 启动身份");
          }
          const gate = gated ? child.stdio[3] as Writable | null : undefined;
          let gateClosed = false;
          const closeGate = (release: boolean) => {
            if (gateClosed || gate === undefined || gate === null) {
              return;
            }
            gateClosed = true;
            if (release) {
              gate.write(Buffer.from("1"));
            }
            gate.end();
          };
          child.unref();
          resolve({
            pid: child.pid,
            startTicks,
            releaseStartup: gated ? () => closeGate(true) : undefined,
            cancelStartup: gated ? () => closeGate(false) : undefined,
          });
        } catch (error) {
          reject(error);
        }
      });
    });
  }
}

export type SocketProbe = (socketPath: string) => Promise<boolean>;

export async function probeBrokerSocket(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const client = createConnection(socketPath);
    const response: Buffer[] = [];
    let responseLength = 0;
    let settled = false;
    const timeout = setTimeout(() => finish(false), 250);
    const finish = (alive: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      client.destroy();
      resolve(alive);
    };
    client.once("error", () => finish(false));
    client.once("connect", () => client.write(Buffer.from([0x50, 0, 0, 0, 0])));
    client.on("data", (data) => {
      response.push(data);
      responseLength += data.length;
      if (responseLength >= 5) {
        const frame = Buffer.concat(response, responseLength);
        finish(frame[0] === 0x50 && frame.readUInt32BE(1) === 0);
      }
    });
  });
}

export class TaskStore {
  private readonly registryLock: RegistryLock;
  private readonly lookupProcessStartTicks: (pid: number) => Promise<number | undefined>;
  private readonly lookupSocketIdentity: (path: string) => Promise<SocketIdentity | undefined>;
  private readonly killProcess: (pid: number, signal: NodeJS.Signals) => void;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private readonly registryWriter: (path: string, content: string) => Promise<void>;
  private readonly cgroupExists: (relativePath: string) => Promise<boolean>;
  private readonly lookupCgroupIdentity: (relativePath: string) => Promise<SocketIdentity | undefined>;

  constructor(
    public readonly registryPath: string,
    private readonly runner: CommandRunner = new ProcessRunner(),
    private readonly runtime: TaskRuntimeConfig = defaultRuntime,
    private readonly probe: SocketProbe = probeBrokerSocket,
    dependencies: TaskStoreDependencies = {},
  ) {
    this.registryLock = dependencies.registryLock
      ?? new PythonRegistryLock(runtime.pythonPath, runtime.brokerScriptPath);
    this.lookupProcessStartTicks = dependencies.processStartTicks ?? processStartTicks;
    this.lookupSocketIdentity = dependencies.socketIdentity ?? readSocketIdentity;
    this.killProcess = dependencies.killProcess ?? ((pid, signal) => process.kill(pid, signal));
    this.sleep = dependencies.sleep ?? ((milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)));
    this.registryWriter = dependencies.registryWriter ?? writeFileAtomically;
    this.cgroupExists = dependencies.cgroupExists ?? (async (relativePath) => {
      try {
        const info = await lstat(join("/sys/fs/cgroup", relativePath.slice(1)));
        return info.isDirectory();
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          return false;
        }
        throw error;
      }
    });
    this.lookupCgroupIdentity = dependencies.cgroupIdentity ?? (async (relativePath) => {
      try {
        const info = await lstat(join("/sys/fs/cgroup", relativePath.slice(1)), { bigint: true });
        const expectedUid = process.getuid?.();
        if (!info.isDirectory() || (expectedUid !== undefined && info.uid !== BigInt(expectedUid))) {
          throw new Error(`快速共享终端 cgroup 不属于当前用户：${relativePath}`);
        }
        return { dev: info.dev.toString(), ino: info.ino.toString() };
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          return undefined;
        }
        throw error;
      }
    });
  }

  async list(): Promise<SharedTask[]> {
    return this.withLock(async () => {
      const registry = await this.readRegistry();
      let changed = false;
      const recovered: SharedTask[] = [];
      for (const task of registry.tasks) {
        if (task.backend === "pty-broker" && task.brokerState === "starting") {
          try {
            if (task.brokerPid !== undefined && task.brokerStartTicks !== undefined) {
              await this.hydrateBrokerIdentity(task);
              changed = true;
            } else {
              throw new Error("快速共享终端 starting 登记尚未持久化进程身份");
            }
          } catch {
            const dataSocket = task.brokerDataPath
              ? await this.lookupSocketIdentity(task.brokerDataPath)
              : undefined;
            const controlSocket = task.brokerControlPath
              ? await this.lookupSocketIdentity(task.brokerControlPath)
              : undefined;
            const cgroup = task.brokerCgroupPath
              ? await this.lookupCgroupIdentity(task.brokerCgroupPath)
              : undefined;
            const processGone = task.brokerPid === undefined
              || task.brokerStartTicks === undefined
              || await this.lookupProcessStartTicks(task.brokerPid) !== task.brokerStartTicks;
            if (
              processGone
              && dataSocket === undefined
              && controlSocket === undefined
              && cgroup === undefined
            ) {
              changed = true;
              continue;
            }
            // Keep the durable starting row while any recoverable resource remains.
          }
        }
        recovered.push(task);
      }
      registry.tasks = recovered;
      if (changed) {
        await this.writeRegistry(registry);
      }
      return registry.tasks;
    });
  }

  async verifyTmux(): Promise<string> {
    try {
      return (await this.runner.run(this.runtime.tmuxPath, ["-V"])).stdout.trim();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        throw new Error(`未找到 tmux（配置路径：${this.runtime.tmuxPath}），请先安装 tmux 或修改 sharedTerminals.tmuxPath`);
      }
      throw error;
    }
  }

  async verifyBroker(): Promise<string> {
    try {
      return (await this.runner.run(this.runtime.pythonPath, [this.runtime.brokerScriptPath, "--version"])).stdout.trim();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        throw new Error(`未找到 Python 3（配置路径：${this.runtime.pythonPath}），无法启动快速共享终端`);
      }
      throw error;
    }
  }

  async listWithStatus(): Promise<SharedTaskStatus[]> {
    const tasks = await this.list();
    return Promise.all(tasks.map(async (task) => ({ ...task, alive: await this.isAlive(task) })));
  }

  async create(name: string, cwd: string): Promise<SharedTask> {
    const cleanName = this.validateName(name);
    if (!isAbsolute(cwd) || cwd.length > 4096) {
      throw new Error("工作目录必须是长度不超过 4096 的绝对路径");
    }
    await this.verifyBroker();

    return this.withLock(async () => {
      const registry = await this.readRegistry();
      if (registry.tasks.some((task) => task.name === cleanName)) {
        throw new Error(`共享终端任务“${cleanName}”已存在`);
      }
      const maxTasks = this.runtime.maxTasks ?? 12;
      if (registry.tasks.length >= maxTasks) {
        throw new Error(`最多允许 ${maxTasks} 个共享终端，请先结束不再使用的任务`);
      }

      const id = randomUUID().toLowerCase();
      const task: SharedTask = {
        id,
        name: cleanName,
        cwd,
        session: `shared-${id}`,
        createdAt: new Date().toISOString(),
        open: true,
        backend: "pty-broker",
        brokerState: "starting",
        brokerNonce: `${randomUUID().replaceAll("-", "")}${randomUUID().replaceAll("-", "")}`,
      };

      const instanceToken = randomUUID().replaceAll("-", "").slice(0, 24);
      task.brokerDataPath = join(this.runtime.socketDirectory, `t-${instanceToken}.sock`);
      task.brokerControlPath = join(this.runtime.socketDirectory, `t-${instanceToken}.ctl`);
      registry.tasks.push(task);
      await this.writeRegistry(registry);

      const socketPath = this.brokerSocketPath(task);
      const controlSocketPath = this.brokerControlSocketPath(task);
      const brokerArgs = [
        this.runtime.brokerScriptPath, "serve", "--socket", socketPath,
        "--control-socket", controlSocketPath,
        "--cwd", cwd, "--shell", this.runtime.shellPath,
        "--task-id", task.id,
        "--instance-nonce", task.brokerNonce!,
        "--max-clients", String(this.runtime.maxClientsPerTask ?? 4),
        "--replay-bytes", String(this.runtime.replayBytes ?? 512 * 1024),
        "--max-client-input-bytes", String(this.runtime.maxClientInputBytes ?? 256 * 1024),
        "--max-client-output-bytes", String(this.runtime.maxClientOutputBytes ?? 2 * 1024 * 1024),
        "--max-pty-input-bytes", String(this.runtime.maxPtyInputBytes ?? 256 * 1024),
        "--startup-gate-fd", "3",
      ];
      if (this.runtime.requireCgroup ?? true) {
        brokerArgs.push("--require-cgroup");
      }
      let started: StartedProcess | undefined;
      try {
        started = await this.runner.start(this.runtime.pythonPath, brokerArgs, {
          cwd,
          env: { ...process.env, ...this.runtime.environment },
        });
        task.brokerPid = started.pid;
        task.brokerStartTicks = started.startTicks;
        await this.writeRegistry(registry);
        started.releaseStartup?.();
        await this.waitForBroker(controlSocketPath);
        await this.hydrateBrokerIdentity(task, started);
        await this.writeRegistry(registry);
      } catch (error) {
        started?.cancelStartup?.();
        if (started && (!task.brokerDataDev || !task.brokerDataIno || !task.brokerControlDev || !task.brokerControlIno)) {
          await this.hydrateBrokerIdentity(task, started).catch(() => undefined);
        }
        if (
          started
          && task.brokerNonce
          && task.brokerDataPath
          && task.brokerControlPath
          && (!task.brokerDataDev || !task.brokerDataIno || !task.brokerControlDev || !task.brokerControlIno)
        ) {
          await this.captureStartingBrokerIdentity(task, started).catch(() => undefined);
        }
        if (this.hasCompleteBrokerIdentity(task)) {
          const verifyStopStatus = task.brokerState === "running";
          task.brokerState = "starting";
          try {
            await this.writeRegistry(registry);
          } catch (recoveryWriteError) {
            throw new AggregateError(
              [error, recoveryWriteError],
              "共享终端注册表发布失败，且无法持久化完整恢复身份；已保留 Broker 运行态并拒绝清理",
            );
          }
          try {
            await this.stopBroker(task, verifyStopStatus);
          } catch (cleanupError) {
            try {
              if (await this.lookupProcessStartTicks(task.brokerPid) === task.brokerStartTicks) {
                throw cleanupError;
              }
              await this.recoverBrokerRemnants(task);
            } catch (recoveryError) {
              throw new AggregateError(
                [error, cleanupError, recoveryError],
                "共享终端注册表发布失败，且无法确认 Broker 已完整回收",
              );
            }
          }
        } else if (started) {
          const brokerGone = await this.lookupProcessStartTicks(started.pid) !== started.startTicks;
          const dataSocketGone = task.brokerDataPath
            ? await this.lookupSocketIdentity(task.brokerDataPath) === undefined
            : true;
          const controlSocketGone = task.brokerControlPath
            ? await this.lookupSocketIdentity(task.brokerControlPath) === undefined
            : true;
          if (!brokerGone || !dataSocketGone || !controlSocketGone) {
            throw new AggregateError(
              [error],
              "快速共享终端身份尚未认证，已拒绝发送信号并保留 starting 登记",
            );
          }
        }
        registry.tasks = registry.tasks.filter((candidate) => candidate.id !== task.id);
        try {
          await this.writeRegistry(registry);
        } catch (unregisterError) {
          throw new AggregateError(
            [error, unregisterError],
            "共享终端启动失败且无法清除 starting 登记；已保留恢复句柄",
          );
        }
        throw error;
      }
      return task;
    });
  }

  async createAutomatic(cwd: string): Promise<SharedTask> {
    const names = new Set((await this.list()).map((task) => task.name));
    for (let number = 1; ; number += 1) {
      const name = `终端 ${number}`;
      if (names.has(name)) {
        continue;
      }
      try {
        return await this.create(name, cwd);
      } catch (error) {
        if (!(error instanceof Error) || !error.message.includes("已存在")) {
          throw error;
        }
        names.add(name);
      }
    }
  }

  async rename(id: string, name: string): Promise<SharedTask> {
    const cleanName = this.validateName(name);
    return this.withLock(async () => {
      const registry = await this.readRegistry();
      const task = registry.tasks.find((candidate) => candidate.id === id);
      if (!task) {
        throw new Error("共享终端任务不存在");
      }
      if (registry.tasks.some((candidate) => candidate.id !== id && candidate.name === cleanName)) {
        throw new Error(`共享终端任务“${cleanName}”已存在`);
      }
      task.name = cleanName;
      await this.writeRegistry(registry);
      return task;
    });
  }

  async setOpen(id: string, open: boolean): Promise<SharedTask> {
    return this.withLock(async () => {
      const registry = await this.readRegistry();
      const task = registry.tasks.find((candidate) => candidate.id === id);
      if (!task) {
        throw new Error("共享终端任务不存在");
      }
      task.open = open;
      await this.writeRegistry(registry);
      return task;
    });
  }

  async closeIfPresent(id: string): Promise<boolean> {
    return this.withLock(async () => {
      const registry = await this.readRegistry();
      const task = registry.tasks.find((candidate) => candidate.id === id);
      if (!task) {
        return false;
      }
      if (!task.open) {
        return true;
      }
      task.open = false;
      await this.writeRegistry(registry);
      return true;
    });
  }

  async delete(id: string): Promise<void> {
    await this.withLock(async () => {
      const registry = await this.readRegistry();
      const task = registry.tasks.find((candidate) => candidate.id === id);
      if (!task) {
        return;
      }
      if ((task.backend ?? "tmux") === "pty-broker") {
        if (
          this.hasCompleteBrokerIdentity(task)
          && await this.lookupProcessStartTicks(task.brokerPid) !== task.brokerStartTicks
        ) {
          await this.recoverBrokerRemnants(task);
        } else {
          await this.stopBroker(task);
        }
      } else {
        await this.runner.run(this.runtime.tmuxPath, ["-L", this.runtime.socketName, "kill-session", "-t", task.session]).catch(() => undefined);
      }
      registry.tasks = registry.tasks.filter((candidate) => candidate.id !== id);
      await this.writeRegistry(registry);
    });
  }

  private async isAlive(task: SharedTask): Promise<boolean> {
    if ((task.backend ?? "tmux") === "pty-broker") {
      if (task.brokerState === "starting") {
        return false;
      }
      if (
        !task.brokerPid
        || !task.brokerStartTicks
        || !task.brokerNonce
        || !task.brokerDataPath
        || !task.brokerDataDev
        || !task.brokerDataIno
        || !task.brokerControlPath
        || !task.brokerControlDev
        || !task.brokerControlIno
      ) {
        return false;
      }
      try {
        const socketIdentity = await this.lookupSocketIdentity(task.brokerControlPath);
        const dataSocketIdentity = await this.lookupSocketIdentity(task.brokerDataPath);
        const status = await this.readBrokerStatus(task);
        return status.taskId === task.id
          && status.brokerPid === task.brokerPid
          && status.brokerStartTicks === task.brokerStartTicks
          && status.instanceNonce === task.brokerNonce
          && status.controlSocketDev === task.brokerControlDev
          && status.controlSocketIno === task.brokerControlIno
          && socketIdentity?.dev === task.brokerControlDev
          && socketIdentity.ino === task.brokerControlIno
          && dataSocketIdentity?.dev === task.brokerDataDev
          && dataSocketIdentity.ino === task.brokerDataIno
          && (!(this.runtime.requireCgroup ?? true)
            || (task.brokerCgroupPath !== undefined && await this.cgroupExists(task.brokerCgroupPath)))
          && await this.lookupProcessStartTicks(task.brokerPid) === task.brokerStartTicks;
      } catch {
        return false;
      }
    }
    try {
      await this.runner.run(this.runtime.tmuxPath, ["-L", this.runtime.socketName, "has-session", "-t", task.session]);
      return true;
    } catch {
      return false;
    }
  }

  brokerSocketPath(task: SharedTask): string {
    return task.brokerDataPath ?? join(this.runtime.socketDirectory, `${task.session}.sock`);
  }

  brokerControlSocketPath(task: SharedTask): string {
    return task.brokerControlPath ?? join(this.runtime.socketDirectory, `${task.session}.control.sock`);
  }

  private async waitForBroker(socketPath: string): Promise<void> {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      if (await this.probe(socketPath)) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    throw new Error("快速共享终端启动超时");
  }

  private async readBrokerStatus(task: SharedTask): Promise<BrokerStatus> {
    const result = await this.runner.run(this.runtime.pythonPath, [
      this.runtime.brokerScriptPath, "status", "--socket", this.brokerControlSocketPath(task),
    ]);
    let status: BrokerStatus;
    try {
      status = JSON.parse(result.stdout) as BrokerStatus;
    } catch {
      throw new Error("快速共享终端状态响应无效");
    }
    if (
      typeof status.taskId !== "string"
      || !Number.isInteger(status.brokerPid)
      || status.brokerPid <= 0
      || !Number.isInteger(status.brokerStartTicks)
      || status.brokerStartTicks <= 0
      || typeof status.instanceNonce !== "string"
      || !/^[a-f0-9]{64}$/.test(status.instanceNonce)
      || !/^\d+$/.test(status.controlSocketDev)
      || !/^\d+$/.test(status.controlSocketIno)
      || typeof status.cgroupPath !== "string"
      || status.cgroupPath.length > 4096
      || (status.processBoundary !== "cgroup-v2" && status.processBoundary !== "subreaper")
      || (status.processBoundary === "cgroup-v2" && (
        !/^\d+$/.test(status.cgroupDev)
        || !/^\d+$/.test(status.cgroupIno)
      ))
    ) {
      throw new Error("快速共享终端状态缺少身份字段");
    }
    return status;
  }

  private async hydrateBrokerIdentity(task: SharedTask, started?: StartedProcess): Promise<void> {
    const expectedPid = started?.pid ?? task.brokerPid;
    const expectedStartTicks = started?.startTicks ?? task.brokerStartTicks;
    if (expectedPid === undefined || expectedStartTicks === undefined || task.brokerNonce === undefined) {
      throw new Error("快速共享终端 starting 登记缺少预先持久化的进程身份或 nonce");
    }
    const controlSocketPath = this.brokerControlSocketPath(task);
    const dataSocketPath = this.brokerSocketPath(task);
    const socketBeforeStatus = await this.lookupSocketIdentity(controlSocketPath);
    const dataSocketBeforeStatus = await this.lookupSocketIdentity(dataSocketPath);
    const status = await this.readBrokerStatus(task);
    const cgroupBeforeStatus = status.cgroupPath
      ? await this.lookupCgroupIdentity(status.cgroupPath)
      : undefined;
    const socketAfterStatus = await this.lookupSocketIdentity(controlSocketPath);
    const dataSocketAfterStatus = await this.lookupSocketIdentity(dataSocketPath);
    const cgroupAfterStatus = status.cgroupPath
      ? await this.lookupCgroupIdentity(status.cgroupPath)
      : undefined;
    const liveStartTicks = await this.lookupProcessStartTicks(status.brokerPid);
    if (
      socketBeforeStatus === undefined
      || socketAfterStatus === undefined
      || dataSocketBeforeStatus === undefined
      || dataSocketAfterStatus === undefined
    ) {
      throw new Error("快速共享终端启动后 data/control socket 消失");
    }
    if (
      socketBeforeStatus.dev !== socketAfterStatus.dev
      || socketBeforeStatus.ino !== socketAfterStatus.ino
      || dataSocketBeforeStatus.dev !== dataSocketAfterStatus.dev
      || dataSocketBeforeStatus.ino !== dataSocketAfterStatus.ino
      || status.taskId !== task.id
      || status.instanceNonce !== task.brokerNonce
      || status.brokerPid !== expectedPid
      || status.brokerStartTicks !== expectedStartTicks
      || liveStartTicks !== status.brokerStartTicks
      || status.controlSocketDev !== socketBeforeStatus.dev
      || status.controlSocketIno !== socketBeforeStatus.ino
      || ((this.runtime.requireCgroup ?? true) && (
        status.processBoundary !== "cgroup-v2"
        || !this.isSafeTaskCgroupPath(status.cgroupPath, task.id)
        || cgroupBeforeStatus === undefined
        || cgroupAfterStatus === undefined
        || cgroupBeforeStatus.dev !== status.cgroupDev
        || cgroupBeforeStatus.ino !== status.cgroupIno
        || cgroupAfterStatus.dev !== status.cgroupDev
        || cgroupAfterStatus.ino !== status.cgroupIno
      ))
    ) {
      throw new Error("快速共享终端启动后身份不匹配");
    }
    task.brokerPid = status.brokerPid;
    task.brokerStartTicks = status.brokerStartTicks;
    task.brokerNonce = status.instanceNonce;
    task.brokerDataDev = dataSocketBeforeStatus.dev;
    task.brokerDataIno = dataSocketBeforeStatus.ino;
    task.brokerControlDev = socketBeforeStatus.dev;
    task.brokerControlIno = socketBeforeStatus.ino;
    task.brokerCgroupPath = status.cgroupPath || undefined;
    task.brokerCgroupDev = status.cgroupPath ? status.cgroupDev : undefined;
    task.brokerCgroupIno = status.cgroupPath ? status.cgroupIno : undefined;
    task.brokerState = "running";
  }

  private async captureStartingBrokerIdentity(task: SharedTask, started: StartedProcess): Promise<void> {
    if (await this.lookupProcessStartTicks(started.pid) !== started.startTicks) {
      throw new Error("快速共享终端启动进程身份已消失或漂移");
    }
    const controlBefore = await this.lookupSocketIdentity(this.brokerControlSocketPath(task));
    const dataBefore = await this.lookupSocketIdentity(this.brokerSocketPath(task));
    const status = await this.readBrokerStatus(task);
    const cgroupBefore = status.cgroupPath
      ? await this.lookupCgroupIdentity(status.cgroupPath)
      : undefined;
    const controlAfter = await this.lookupSocketIdentity(this.brokerControlSocketPath(task));
    const dataAfter = await this.lookupSocketIdentity(this.brokerSocketPath(task));
    const cgroupAfter = status.cgroupPath
      ? await this.lookupCgroupIdentity(status.cgroupPath)
      : undefined;
    if (
      controlBefore === undefined
      || dataBefore === undefined
      || controlAfter === undefined
      || dataAfter === undefined
      || controlBefore.dev !== controlAfter.dev
      || controlBefore.ino !== controlAfter.ino
      || dataBefore.dev !== dataAfter.dev
      || dataBefore.ino !== dataAfter.ino
      || status.taskId !== task.id
      || status.instanceNonce !== task.brokerNonce
      || status.controlSocketDev !== controlBefore.dev
      || status.controlSocketIno !== controlBefore.ino
      || ((this.runtime.requireCgroup ?? true) && (
        status.processBoundary !== "cgroup-v2"
        || !this.isSafeTaskCgroupPath(status.cgroupPath, task.id)
        || cgroupBefore === undefined
        || cgroupAfter === undefined
        || cgroupBefore.dev !== status.cgroupDev
        || cgroupBefore.ino !== status.cgroupIno
        || cgroupAfter.dev !== status.cgroupDev
        || cgroupAfter.ino !== status.cgroupIno
      ))
    ) {
      throw new Error("快速共享终端启动期 socket 身份无法连续核验");
    }
    task.brokerPid = started.pid;
    task.brokerStartTicks = started.startTicks;
    task.brokerControlDev = controlBefore.dev;
    task.brokerControlIno = controlBefore.ino;
    task.brokerDataDev = dataBefore.dev;
    task.brokerDataIno = dataBefore.ino;
    task.brokerCgroupPath = status.cgroupPath || undefined;
    task.brokerCgroupDev = status.cgroupPath ? status.cgroupDev : undefined;
    task.brokerCgroupIno = status.cgroupPath ? status.cgroupIno : undefined;
  }

  private async stopBroker(task: SharedTask, verifyStatus = true): Promise<void> {
    if (!this.hasCompleteBrokerIdentity(task)) {
      throw new Error("快速共享终端缺少完整停止身份，已保留任务登记；请先只读核对 Broker 状态");
    }
    const socketBeforeStatus = await this.lookupSocketIdentity(task.brokerControlPath);
    const dataSocketBeforeStatus = await this.lookupSocketIdentity(task.brokerDataPath);
    const cgroupBeforeStatus = task.brokerCgroupPath
      ? await this.lookupCgroupIdentity(task.brokerCgroupPath)
      : undefined;
    const status = verifyStatus ? await this.readBrokerStatus(task) : undefined;
    const socketAfterStatus = await this.lookupSocketIdentity(task.brokerControlPath);
    const dataSocketAfterStatus = await this.lookupSocketIdentity(task.brokerDataPath);
    const cgroupAfterStatus = task.brokerCgroupPath
      ? await this.lookupCgroupIdentity(task.brokerCgroupPath)
      : undefined;
    const identityMatches = (!verifyStatus || (
      status?.taskId === task.id
      && status.brokerPid === task.brokerPid
      && status.brokerStartTicks === task.brokerStartTicks
      && status.instanceNonce === task.brokerNonce
      && status.controlSocketDev === task.brokerControlDev
      && status.controlSocketIno === task.brokerControlIno
    ))
      && socketBeforeStatus?.dev === task.brokerControlDev
      && socketBeforeStatus.ino === task.brokerControlIno
      && socketAfterStatus?.dev === task.brokerControlDev
      && socketAfterStatus.ino === task.brokerControlIno
      && dataSocketBeforeStatus?.dev === task.brokerDataDev
      && dataSocketBeforeStatus.ino === task.brokerDataIno
      && dataSocketAfterStatus?.dev === task.brokerDataDev
      && dataSocketAfterStatus.ino === task.brokerDataIno
      && (!verifyStatus || (
        status?.cgroupPath === (task.brokerCgroupPath ?? "")
        && status.cgroupDev === (task.brokerCgroupDev ?? "-1")
        && status.cgroupIno === (task.brokerCgroupIno ?? "-1")
      ))
      && (task.brokerCgroupPath === undefined || (
        cgroupBeforeStatus?.dev === task.brokerCgroupDev
        && cgroupBeforeStatus?.ino === task.brokerCgroupIno
        && cgroupAfterStatus?.dev === task.brokerCgroupDev
        && cgroupAfterStatus?.ino === task.brokerCgroupIno
      ));
    if (!identityMatches || await this.lookupProcessStartTicks(task.brokerPid) !== task.brokerStartTicks) {
      throw new Error("快速共享终端身份不匹配，已拒绝停止；请先只读核对 Broker 状态");
    }
    const stopResult = await this.runner.run(this.runtime.pythonPath, [
      this.runtime.brokerScriptPath,
      "stop",
      "--socket", task.brokerControlPath,
      "--task-id", task.id,
      "--broker-pid", String(task.brokerPid),
      "--broker-start-ticks", String(task.brokerStartTicks),
      "--instance-nonce", task.brokerNonce,
      "--control-socket-dev", task.brokerControlDev,
      "--control-socket-ino", task.brokerControlIno,
    ]);
    let acknowledgement: BrokerStopAck;
    try {
      acknowledgement = JSON.parse(stopResult.stdout) as BrokerStopAck;
    } catch {
      throw new Error("快速共享终端停止确认无效，已保留任务登记");
    }
    if (
      acknowledgement.taskId !== task.id
      || acknowledgement.brokerPid !== task.brokerPid
      || acknowledgement.brokerStartTicks !== task.brokerStartTicks
      || acknowledgement.instanceNonce !== task.brokerNonce
      || acknowledgement.controlSocketDev !== task.brokerControlDev
      || acknowledgement.controlSocketIno !== task.brokerControlIno
    ) {
      throw new Error("快速共享终端停止确认身份不匹配，已保留任务登记");
    }
    for (let attempt = 0; attempt < 500; attempt += 1) {
      if (await this.lookupProcessStartTicks(task.brokerPid) !== task.brokerStartTicks) {
        const remainingControlSocket = await this.lookupSocketIdentity(task.brokerControlPath);
        const remainingDataSocket = await this.lookupSocketIdentity(task.brokerDataPath);
        const remainingCgroup = task.brokerCgroupPath === undefined
          ? undefined
          : await this.lookupCgroupIdentity(task.brokerCgroupPath);
        const cgroupGone = remainingCgroup === undefined;
        if (remainingControlSocket === undefined && remainingDataSocket === undefined && cgroupGone) {
          return;
        }
        if (
          remainingControlSocket !== undefined
          && (remainingControlSocket.dev !== task.brokerControlDev || remainingControlSocket.ino !== task.brokerControlIno)
        ) {
          throw new Error("快速共享终端进程已退出但控制 socket 身份已漂移，已保留任务登记");
        }
        if (
          remainingDataSocket !== undefined
          && (remainingDataSocket.dev !== task.brokerDataDev || remainingDataSocket.ino !== task.brokerDataIno)
        ) {
          throw new Error("快速共享终端进程已退出但数据 socket 身份已漂移，已保留任务登记");
        }
        if (
          remainingCgroup !== undefined
          && (remainingCgroup.dev !== task.brokerCgroupDev || remainingCgroup.ino !== task.brokerCgroupIno)
        ) {
          throw new Error("快速共享终端进程已退出但 cgroup 身份已漂移，已保留任务登记");
        }
      }
      await this.sleep(20);
    }
    if (await this.lookupSocketIdentity(task.brokerDataPath) !== undefined) {
      throw new Error("快速共享终端停止超时：数据 socket 仍存在，已保留任务登记");
    }
    throw new Error("快速共享终端停止超时");
  }

  private async recoverBrokerRemnants(
    task: SharedTask & Required<Pick<
      SharedTask,
      | "brokerPid"
      | "brokerStartTicks"
      | "brokerNonce"
      | "brokerDataPath"
      | "brokerDataDev"
      | "brokerDataIno"
      | "brokerControlPath"
      | "brokerControlDev"
      | "brokerControlIno"
    >>,
  ): Promise<void> {
    if (await this.lookupProcessStartTicks(task.brokerPid) === task.brokerStartTicks) {
      throw new Error("快速共享终端 Broker 仍在运行，已拒绝残留回收");
    }
    const dataSocket = await this.lookupSocketIdentity(task.brokerDataPath);
    const controlSocket = await this.lookupSocketIdentity(task.brokerControlPath);
    const cgroup = task.brokerCgroupPath
      ? await this.lookupCgroupIdentity(task.brokerCgroupPath)
      : undefined;
    if (
      (dataSocket !== undefined
        && (dataSocket.dev !== task.brokerDataDev || dataSocket.ino !== task.brokerDataIno))
      || (controlSocket !== undefined
        && (controlSocket.dev !== task.brokerControlDev || controlSocket.ino !== task.brokerControlIno))
      || (task.brokerCgroupPath !== undefined && !this.isSafeTaskCgroupPath(task.brokerCgroupPath, task.id))
      || (task.brokerCgroupPath !== undefined && (!task.brokerCgroupDev || !task.brokerCgroupIno))
      || (cgroup !== undefined
        && (cgroup.dev !== task.brokerCgroupDev || cgroup.ino !== task.brokerCgroupIno))
    ) {
      throw new Error("快速共享终端残留资源身份漂移，已拒绝回收并保留任务登记");
    }
    const recoveryArgs = [
      this.runtime.brokerScriptPath,
      "recover",
      "--data-socket", task.brokerDataPath,
      "--data-socket-dev", task.brokerDataDev,
      "--data-socket-ino", task.brokerDataIno,
      "--control-socket", task.brokerControlPath,
      "--control-socket-dev", task.brokerControlDev,
      "--control-socket-ino", task.brokerControlIno,
      "--task-id", task.id,
      "--broker-pid", String(task.brokerPid),
      "--broker-start-ticks", String(task.brokerStartTicks),
      "--cgroup-path", task.brokerCgroupPath ?? "",
    ];
    if (task.brokerCgroupPath) {
      recoveryArgs.push(
        "--cgroup-dev", task.brokerCgroupDev!,
        "--cgroup-ino", task.brokerCgroupIno!,
      );
    }
    const result = await this.runner.run(this.runtime.pythonPath, recoveryArgs);
    let acknowledgement: BrokerRecoveryAck;
    try {
      acknowledgement = JSON.parse(result.stdout) as BrokerRecoveryAck;
    } catch {
      throw new Error("快速共享终端残留回收确认无效，已保留任务登记");
    }
    if (
      acknowledgement.taskId !== task.id
      || acknowledgement.brokerPid !== task.brokerPid
      || acknowledgement.brokerStartTicks !== task.brokerStartTicks
      || acknowledgement.dataSocketDev !== task.brokerDataDev
      || acknowledgement.dataSocketIno !== task.brokerDataIno
      || acknowledgement.controlSocketDev !== task.brokerControlDev
      || acknowledgement.controlSocketIno !== task.brokerControlIno
      || acknowledgement.cgroupPath !== (task.brokerCgroupPath ?? "")
      || acknowledgement.cgroupDev !== (task.brokerCgroupDev ?? "")
      || acknowledgement.cgroupIno !== (task.brokerCgroupIno ?? "")
    ) {
      throw new Error("快速共享终端残留回收确认身份不匹配，已保留任务登记");
    }
    if (
      await this.lookupProcessStartTicks(task.brokerPid) === task.brokerStartTicks
      || await this.lookupSocketIdentity(task.brokerDataPath) !== undefined
      || await this.lookupSocketIdentity(task.brokerControlPath) !== undefined
      || (task.brokerCgroupPath !== undefined && await this.cgroupExists(task.brokerCgroupPath))
    ) {
      throw new Error("快速共享终端残留回收后仍有资源存在，已保留任务登记");
    }
  }

  private hasCompleteBrokerIdentity(task: SharedTask): task is SharedTask & Required<Pick<
    SharedTask,
    | "brokerPid"
    | "brokerStartTicks"
    | "brokerNonce"
    | "brokerDataPath"
    | "brokerDataDev"
    | "brokerDataIno"
    | "brokerControlPath"
    | "brokerControlDev"
    | "brokerControlIno"
  >> {
    return Boolean(
      task.brokerPid
      && task.brokerStartTicks
      && task.brokerNonce
      && task.brokerDataPath
      && task.brokerDataDev
      && task.brokerDataIno
      && task.brokerControlPath
      && task.brokerControlDev
      && task.brokerControlIno
      && (!(this.runtime.requireCgroup ?? true) || (
        task.brokerCgroupPath
        && task.brokerCgroupDev
        && task.brokerCgroupIno
      )),
    );
  }

  private isSafeTaskCgroupPath(relativePath: string, taskId: string): boolean {
    if (!relativePath.startsWith("/") || relativePath.includes("\0")) {
      return false;
    }
    const segments = relativePath.split("/").filter(Boolean);
    return segments.length > 0
      && !segments.some((segment) => segment === "." || segment === "..")
      && segments.at(-1) === `shared-terminal-${taskId}`;
  }

  private async terminateStartedProcess(started: StartedProcess): Promise<void> {
    if (await this.lookupProcessStartTicks(started.pid) !== started.startTicks) {
      return;
    }
    try {
      this.killProcess(started.pid, "SIGTERM");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ESRCH") {
        return;
      }
      throw error;
    }
    for (let attempt = 0; attempt < 50; attempt += 1) {
      if (await this.lookupProcessStartTicks(started.pid) !== started.startTicks) {
        return;
      }
      await this.sleep(20);
    }
    if (await this.lookupProcessStartTicks(started.pid) === started.startTicks) {
      try {
        this.killProcess(started.pid, "SIGKILL");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ESRCH") {
          return;
        }
        throw error;
      }
    }
    for (let attempt = 0; attempt < 50; attempt += 1) {
      if (await this.lookupProcessStartTicks(started.pid) !== started.startTicks) {
        return;
      }
      await this.sleep(20);
    }
    throw new Error(`快速共享终端启动失败后无法确认已退出：pid=${started.pid}`);
  }

  private validateName(name: string): string {
    const cleanName = name.trim();
    if (!cleanName || cleanName.length > 128) {
      throw new Error("任务名称不能为空且不得超过 128 个字符");
    }
    return cleanName;
  }

  private async readRegistry(): Promise<Registry> {
    try {
      const file = await open(this.registryPath, "r");
      let content: string;
      try {
        content = await readFileHandleLimited(file, 1024 * 1024);
      } finally {
        await file.close();
      }
      const data = JSON.parse(content) as { version?: unknown; tasks?: unknown };
      const topLevelKeys = Object.keys(data);
      if (topLevelKeys.some((key) => key !== "version" && key !== "tasks")) {
        throw new Error("共享终端注册表包含未知字段");
      }
      if (data.version !== 1 || !Array.isArray(data.tasks)) {
        throw new Error("共享终端注册表结构无效");
      }
      const maxTasks = this.runtime.maxTasks ?? 12;
      if (data.tasks.length > maxTasks) {
        throw new Error(`共享终端注册表任务数量超过 ${maxTasks}`);
      }
      const tasks = data.tasks.map((task, index) => this.validateRegistryTask(task, index));
      const ids = new Set(tasks.map((task) => task.id));
      const names = new Set(tasks.map((task) => task.name));
      const sessions = new Set(tasks.map((task) => task.session));
      if (ids.size !== tasks.length || names.size !== tasks.length || sessions.size !== tasks.length) {
        throw new Error("共享终端注册表包含重复的 ID、名称或 session");
      }
      return { version: 1, tasks };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return { version: 1, tasks: [] };
      }
      throw error;
    }
  }

  private validateRegistryTask(value: unknown, index: number): SharedTask {
    if (!value || typeof value !== "object") {
      throw new Error(`共享终端注册表第 ${index + 1} 行任务结构无效`);
    }
    const task = value as Partial<SharedTask>;
    const allowedKeys = new Set([
      "id", "name", "cwd", "session", "createdAt", "open", "backend", "brokerState",
      "brokerPid", "brokerStartTicks", "brokerNonce", "brokerDataPath", "brokerControlPath",
      "brokerDataDev", "brokerDataIno", "brokerControlDev", "brokerControlIno", "brokerCgroupPath",
      "brokerCgroupDev", "brokerCgroupIno",
    ]);
    if (Object.keys(task).some((key) => !allowedKeys.has(key))) {
      throw new Error(`共享终端注册表第 ${index + 1} 行包含未知字段`);
    }
    if (typeof task.id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(task.id)) {
      throw new Error(`共享终端注册表第 ${index + 1} 行任务 ID 无效`);
    }
    if (typeof task.name !== "string" || !task.name.trim() || task.name.length > 128) {
      throw new Error(`共享终端注册表第 ${index + 1} 行任务名称无效`);
    }
    if (typeof task.cwd !== "string" || !isAbsolute(task.cwd) || task.cwd.length > 4096) {
      throw new Error(`共享终端注册表第 ${index + 1} 行工作目录无效`);
    }
    if (typeof task.session !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(task.session)) {
      throw new Error(`共享终端注册表第 ${index + 1} 行 session 无效`);
    }
    if (
      typeof task.createdAt !== "string"
      || task.createdAt.length > 64
      || Number.isNaN(Date.parse(task.createdAt))
    ) {
      throw new Error(`共享终端注册表第 ${index + 1} 行创建时间无效`);
    }
    if (task.backend !== undefined && task.backend !== "tmux" && task.backend !== "pty-broker") {
      throw new Error(`共享终端注册表第 ${index + 1} 行 backend 无效`);
    }
    if (task.brokerState !== undefined && task.brokerState !== "starting" && task.brokerState !== "running") {
      throw new Error(`共享终端注册表第 ${index + 1} 行 Broker state 无效`);
    }
    if (task.open !== undefined && typeof task.open !== "boolean") {
      throw new Error(`共享终端注册表第 ${index + 1} 行 open 无效`);
    }
    if (task.brokerPid !== undefined && (!Number.isInteger(task.brokerPid) || task.brokerPid <= 0)) {
      throw new Error(`共享终端注册表第 ${index + 1} 行 Broker PID 无效`);
    }
    if (
      task.brokerStartTicks !== undefined
      && (!Number.isInteger(task.brokerStartTicks) || task.brokerStartTicks <= 0)
    ) {
      throw new Error(`共享终端注册表第 ${index + 1} 行 Broker start ticks 无效`);
    }
    if (task.brokerNonce !== undefined && !/^[a-f0-9]{64}$/.test(task.brokerNonce)) {
      throw new Error(`共享终端注册表第 ${index + 1} 行 Broker nonce 无效`);
    }
    if (task.backend === "pty-broker" && (
      typeof task.brokerDataPath !== "string"
      || !isAbsolute(task.brokerDataPath)
      || task.brokerDataPath.length > 4096
      || typeof task.brokerControlPath !== "string"
      || !isAbsolute(task.brokerControlPath)
      || task.brokerControlPath.length > 4096
    )) {
      throw new Error(`共享终端注册表第 ${index + 1} 行 Broker 身份不完整`);
    }
    const brokerState = task.backend === "pty-broker" ? (task.brokerState ?? "running") : undefined;
    if (task.backend === "pty-broker" && brokerState === "running" && (
      !task.brokerPid
      || !task.brokerStartTicks
      || !task.brokerNonce
      || typeof task.brokerDataDev !== "string"
      || !/^\d+$/.test(task.brokerDataDev)
      || typeof task.brokerDataIno !== "string"
      || !/^\d+$/.test(task.brokerDataIno)
      || typeof task.brokerControlDev !== "string"
      || !/^\d+$/.test(task.brokerControlDev)
      || typeof task.brokerControlIno !== "string"
      || !/^\d+$/.test(task.brokerControlIno)
      || ((this.runtime.requireCgroup ?? true) && (
        typeof task.brokerCgroupPath !== "string"
        || !this.isSafeTaskCgroupPath(task.brokerCgroupPath, task.id)
        || typeof task.brokerCgroupDev !== "string"
        || !/^\d+$/.test(task.brokerCgroupDev)
        || typeof task.brokerCgroupIno !== "string"
        || !/^\d+$/.test(task.brokerCgroupIno)
      ))
    )) {
      throw new Error(`共享终端注册表第 ${index + 1} 行运行中 Broker 身份不完整`);
    }
    const startingHasAnyResourceIdentity = task.brokerDataDev !== undefined
      || task.brokerDataIno !== undefined
      || task.brokerControlDev !== undefined
      || task.brokerControlIno !== undefined
      || task.brokerCgroupPath !== undefined
      || task.brokerCgroupDev !== undefined
      || task.brokerCgroupIno !== undefined;
    const startingHasAnyCgroupIdentity = task.brokerCgroupPath !== undefined
      || task.brokerCgroupDev !== undefined
      || task.brokerCgroupIno !== undefined;
    const startingHasCompleteCgroupIdentity = typeof task.brokerCgroupPath === "string"
      && this.isSafeTaskCgroupPath(task.brokerCgroupPath, task.id)
      && typeof task.brokerCgroupDev === "string"
      && /^\d+$/.test(task.brokerCgroupDev)
      && typeof task.brokerCgroupIno === "string"
      && /^\d+$/.test(task.brokerCgroupIno);
    const startingHasCompleteResourceIdentity = task.brokerPid !== undefined
      && task.brokerStartTicks !== undefined
      && typeof task.brokerDataDev === "string"
      && /^\d+$/.test(task.brokerDataDev)
      && typeof task.brokerDataIno === "string"
      && /^\d+$/.test(task.brokerDataIno)
      && typeof task.brokerControlDev === "string"
      && /^\d+$/.test(task.brokerControlDev)
      && typeof task.brokerControlIno === "string"
      && /^\d+$/.test(task.brokerControlIno)
      && (!(this.runtime.requireCgroup ?? true) || startingHasCompleteCgroupIdentity)
      && (!startingHasAnyCgroupIdentity || startingHasCompleteCgroupIdentity);
    if (task.backend === "pty-broker" && brokerState === "starting" && (
      !task.brokerNonce
      || (task.brokerPid === undefined) !== (task.brokerStartTicks === undefined)
      || (startingHasAnyResourceIdentity && !startingHasCompleteResourceIdentity)
    )) {
      throw new Error(`共享终端注册表第 ${index + 1} 行 starting Broker 身份无效`);
    }
    return {
      id: task.id,
      name: task.name,
      cwd: task.cwd,
      session: task.session,
      createdAt: task.createdAt,
      open: task.open !== false,
      backend: task.backend ?? "tmux",
      brokerState,
      brokerPid: task.brokerPid,
      brokerStartTicks: task.brokerStartTicks,
      brokerNonce: task.brokerNonce,
      brokerDataPath: task.brokerDataPath,
      brokerDataDev: task.brokerDataDev,
      brokerDataIno: task.brokerDataIno,
      brokerControlPath: task.brokerControlPath,
      brokerControlDev: task.brokerControlDev,
      brokerControlIno: task.brokerControlIno,
      brokerCgroupPath: task.brokerCgroupPath,
      brokerCgroupDev: task.brokerCgroupDev,
      brokerCgroupIno: task.brokerCgroupIno,
    };
  }

  private async writeRegistry(registry: Registry): Promise<void> {
    await this.registryWriter(this.registryPath, `${JSON.stringify(registry, null, 2)}\n`);
  }

  private async withLock<T>(action: () => Promise<T>): Promise<T> {
    const lockPath = `${this.registryPath}.lock`;
    await mkdir(dirname(this.registryPath), { recursive: true });
    return this.registryLock.withLock(lockPath, action);
  }
}
