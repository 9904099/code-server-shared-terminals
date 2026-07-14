import { delimiter, isAbsolute, basename, join } from "node:path";

const fallbackPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

export interface RuntimeConfigInput {
  home: string;
  environment: NodeJS.ProcessEnv;
  globalStoragePath: string;
}

export interface TaskRuntimeConfig {
  tmuxPath: string;
  socketName: string;
  shellPath: string;
  environment: Record<string, string>;
}

export interface RuntimeConfig extends TaskRuntimeConfig {
  registryPath: string;
}

export interface RuntimeOverrides {
  registryPath?: string;
  tmuxPath?: string;
  socketName?: string;
  shellPath?: string;
  environment?: Record<string, string>;
}

export function resolveRuntimeConfig(input: RuntimeConfigInput): RuntimeConfig {
  const user = input.environment.USER || input.environment.LOGNAME || basename(input.home);
  const shellPath = input.environment.SHELL || "/bin/sh";
  const environment: Record<string, string> = {
    HOME: input.home,
    USER: user,
    LOGNAME: input.environment.LOGNAME || user,
    PATH: input.environment.PATH || fallbackPath,
    SHELL: shellPath,
  };
  if (input.environment.CODEX_HOME) {
    environment.CODEX_HOME = input.environment.CODEX_HOME;
  }

  return {
    registryPath: join(input.globalStoragePath, "tasks.json"),
    tmuxPath: "tmux",
    socketName: "code-server-shared-tasks",
    shellPath,
    environment,
  };
}

export function applyRuntimeOverrides(base: RuntimeConfig, overrides: RuntimeOverrides): RuntimeConfig {
  const shellPath = overrides.shellPath || base.shellPath;
  return {
    registryPath: overrides.registryPath || base.registryPath,
    tmuxPath: overrides.tmuxPath || base.tmuxPath,
    socketName: overrides.socketName || base.socketName,
    shellPath,
    environment: {
      ...base.environment,
      ...overrides.environment,
      SHELL: shellPath,
    },
  };
}

export function resolveExecutablePath(
  command: string,
  pathValue: string,
  exists: (path: string) => boolean,
): string {
  const candidates = isAbsolute(command)
    ? [command]
    : pathValue.split(delimiter).filter(Boolean).map((directory) => join(directory, command));
  const resolved = candidates.find(exists);
  if (!resolved) {
    throw new Error(`未找到 tmux（配置路径：${command}），请先安装 tmux 或修改 sharedTerminals.tmuxPath`);
  }
  return resolved;
}
