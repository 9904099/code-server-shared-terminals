import { execFile } from "node:child_process";
import { mkdir, open, readFile, rename, rm, stat } from "node:fs/promises";
import { isAbsolute, dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";

import { TaskRuntimeConfig } from "./runtime-config";

const execFileAsync = promisify(execFile);
const defaultRuntime: TaskRuntimeConfig = {
  tmuxPath: "tmux",
  socketName: "code-server-shared-tasks",
  shellPath: process.env.SHELL || "/bin/sh",
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
}

export class ProcessRunner implements CommandRunner {
  async run(command: string, args: string[]): Promise<{ stdout: string }> {
    const result = await execFileAsync(command, args, { encoding: "utf8" });
    return { stdout: result.stdout };
  }
}

export class TaskStore {
  constructor(
    public readonly registryPath: string,
    private readonly runner: CommandRunner = new ProcessRunner(),
    private readonly runtime: TaskRuntimeConfig = defaultRuntime,
  ) {}

  async list(): Promise<SharedTask[]> {
    return (await this.readRegistry()).tasks;
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

  async listWithStatus(): Promise<SharedTaskStatus[]> {
    const tasks = await this.list();
    return Promise.all(tasks.map(async (task) => ({ ...task, alive: await this.isAlive(task) })));
  }

  async create(name: string, cwd: string): Promise<SharedTask> {
    const cleanName = this.validateName(name);
    if (!isAbsolute(cwd)) {
      throw new Error("工作目录必须是绝对路径");
    }
    await this.verifyTmux();

    return this.withLock(async () => {
      const registry = await this.readRegistry();
      if (registry.tasks.some((task) => task.name === cleanName)) {
        throw new Error(`共享终端任务“${cleanName}”已存在`);
      }

      const id = randomUUID().toLowerCase();
      const task: SharedTask = {
        id,
        name: cleanName,
        cwd,
        session: `shared-${id}`,
        createdAt: new Date().toISOString(),
        open: true,
      };

      await this.runner.run(this.runtime.tmuxPath, [
        "-L", this.runtime.socketName, "new-session", "-d", "-s", task.session, "-c", cwd,
        "/usr/bin/env", ...Object.entries(this.runtime.environment).map(([key, value]) => `${key}=${value}`),
        this.runtime.shellPath, "-l",
      ]);
      try {
        await this.runner.run(this.runtime.tmuxPath, ["-L", this.runtime.socketName, "set-option", "-t", task.session, "status", "off"]);
        registry.tasks.push(task);
        await this.writeRegistry(registry);
      } catch (error) {
        await this.runner.run(this.runtime.tmuxPath, ["-L", this.runtime.socketName, "kill-session", "-t", task.session]).catch(() => undefined);
        throw error;
      }
      return task;
    });
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

  async delete(id: string): Promise<void> {
    await this.withLock(async () => {
      const registry = await this.readRegistry();
      const task = registry.tasks.find((candidate) => candidate.id === id);
      if (!task) {
        return;
      }
      await this.runner.run(this.runtime.tmuxPath, ["-L", this.runtime.socketName, "kill-session", "-t", task.session]).catch(() => undefined);
      registry.tasks = registry.tasks.filter((candidate) => candidate.id !== id);
      await this.writeRegistry(registry);
    });
  }

  private async isAlive(task: SharedTask): Promise<boolean> {
    try {
      await this.runner.run(this.runtime.tmuxPath, ["-L", this.runtime.socketName, "has-session", "-t", task.session]);
      return true;
    } catch {
      return false;
    }
  }

  private validateName(name: string): string {
    const cleanName = name.trim();
    if (!cleanName) {
      throw new Error("任务名称不能为空");
    }
    return cleanName;
  }

  private async readRegistry(): Promise<Registry> {
    try {
      const data = JSON.parse(await readFile(this.registryPath, "utf8")) as Registry;
      const tasks = Array.isArray(data.tasks)
        ? data.tasks.map((task) => ({ ...task, open: task.open !== false }))
        : [];
      return { version: 1, tasks };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return { version: 1, tasks: [] };
      }
      throw error;
    }
  }

  private async writeRegistry(registry: Registry): Promise<void> {
    await mkdir(dirname(this.registryPath), { recursive: true });
    const temporaryPath = `${this.registryPath}.${process.pid}.${randomUUID()}.tmp`;
    const file = await open(temporaryPath, "w", 0o600);
    await file.writeFile(`${JSON.stringify(registry, null, 2)}\n`, "utf8");
    await file.close();
    await rename(temporaryPath, this.registryPath);
  }

  private async withLock<T>(action: () => Promise<T>): Promise<T> {
    const lockPath = `${this.registryPath}.lock`;
    await mkdir(dirname(this.registryPath), { recursive: true });
    for (let attempt = 0; ; attempt += 1) {
      try {
        await mkdir(lockPath);
        break;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
          throw error;
        }
        const lockAge = Date.now() - (await stat(lockPath)).mtimeMs;
        if (lockAge > 30_000) {
          await rm(lockPath, { recursive: true, force: true });
          continue;
        }
        if (attempt >= 50) {
          throw new Error(`共享终端注册表锁等待超时：${lockPath}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    try {
      return await action();
    } finally {
      await rm(lockPath, { recursive: true, force: true });
    }
  }
}
