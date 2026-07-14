import { execFile } from "node:child_process";
import { mkdir, open, readFile, rename, rm } from "node:fs/promises";
import { isAbsolute, dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const socketName = "code-server-shared-tasks";
const terminalPath = "/home/coder/.local/bin:/home/coder/.local/node-v24.11.1-linux-x64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

export interface SharedTask {
  id: string;
  name: string;
  cwd: string;
  session: string;
  createdAt: string;
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
  ) {}

  async list(): Promise<SharedTask[]> {
    return (await this.readRegistry()).tasks;
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
      };

      await this.runner.run("tmux", [
        "-L", socketName, "new-session", "-d", "-s", task.session, "-c", cwd,
        "/usr/bin/env", "HOME=/home/coder", "USER=admin", "LOGNAME=admin",
        "CODEX_HOME=/home/coder/.codex", `PATH=${terminalPath}`, "/bin/bash", "-l",
      ]);
      try {
        await this.runner.run("tmux", ["-L", socketName, "set-option", "-t", task.session, "status", "off"]);
        registry.tasks.push(task);
        await this.writeRegistry(registry);
      } catch (error) {
        await this.runner.run("tmux", ["-L", socketName, "kill-session", "-t", task.session]).catch(() => undefined);
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

  async delete(id: string): Promise<void> {
    await this.withLock(async () => {
      const registry = await this.readRegistry();
      const task = registry.tasks.find((candidate) => candidate.id === id);
      if (!task) {
        return;
      }
      await this.runner.run("tmux", ["-L", socketName, "kill-session", "-t", task.session]).catch(() => undefined);
      registry.tasks = registry.tasks.filter((candidate) => candidate.id !== id);
      await this.writeRegistry(registry);
    });
  }

  private async isAlive(task: SharedTask): Promise<boolean> {
    try {
      await this.runner.run("tmux", ["-L", socketName, "has-session", "-t", task.session]);
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
      return { version: 1, tasks: Array.isArray(data.tasks) ? data.tasks : [] };
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
        if ((error as NodeJS.ErrnoException).code !== "EEXIST" || attempt >= 50) {
          throw error;
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

export const sharedTmuxSocket = socketName;
