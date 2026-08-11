import { delimiter, isAbsolute, basename, join } from "node:path";

const fallbackPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const protocolHeaderBytes = 5;
const pendingDataClients = 2;
const controlBufferBytes = 8 * 64 * 1024;
const maxBrokerBufferBytes = 64 * 1024 * 1024;
const maxUserBufferBytes = 256 * 1024 * 1024;
const defaultAttachOutputBytes = 8 * 1024 * 1024;
const maxAttachOutputBytes = 100 * 1024 * 1024;
const maxUserAttachOutputBytes = 5 * 1024 * 1024 * 1024;

export interface RuntimeConfigInput {
  home: string;
  environment: NodeJS.ProcessEnv;
  globalStoragePath: string;
  brokerScriptPath: string;
  socketDirectory: string;
}

export interface TaskRuntimeConfig {
  tmuxPath: string;
  socketName: string;
  pythonPath: string;
  brokerScriptPath: string;
  socketDirectory: string;
  shellPath: string;
  environment: Record<string, string>;
  maxTasks?: number;
  maxClientsPerTask?: number;
  replayBytes?: number;
  maxClientInputBytes?: number;
  maxClientOutputBytes?: number;
  maxAttachOutputBytes?: number;
  maxPtyInputBytes?: number;
  requireCgroup?: boolean;
}

export interface RuntimeConfig extends TaskRuntimeConfig {
  registryPath: string;
}

export interface RuntimeOverrides {
  registryPath?: string;
  tmuxPath?: string;
  socketName?: string;
  pythonPath?: string;
  socketDirectory?: string;
  shellPath?: string;
  environment?: Record<string, string>;
  maxTasks?: number;
  maxClientsPerTask?: number;
  replayBytes?: number;
  maxClientInputBytes?: number;
  maxClientOutputBytes?: number;
  maxAttachOutputBytes?: number;
  maxPtyInputBytes?: number;
  requireCgroup?: boolean;
}

export function validateRuntimeResourceBudget(runtime: TaskRuntimeConfig): void {
  const maxTasks = runtime.maxTasks ?? 12;
  const maxClients = runtime.maxClientsPerTask ?? 4;
  const replayBytes = runtime.replayBytes ?? 512 * 1024;
  const clientInputBytes = runtime.maxClientInputBytes ?? 256 * 1024;
  const clientOutputBytes = runtime.maxClientOutputBytes ?? 2 * 1024 * 1024;
  const attachOutputBytes = runtime.maxAttachOutputBytes ?? defaultAttachOutputBytes;
  const ptyInputBytes = runtime.maxPtyInputBytes ?? 256 * 1024;
  if (replayBytes + protocolHeaderBytes > clientOutputBytes) {
    throw new Error("回放缓冲必须小于客户端输出队列，并为协议头保留空间");
  }
  const perBroker = (
    (maxClients + pendingDataClients) * clientInputBytes
    + maxClients * clientOutputBytes
    + replayBytes
    + ptyInputBytes
    + controlBufferBytes
  );
  if (perBroker > maxBrokerBufferBytes) {
    throw new Error(`单终端聚合缓冲预算 ${perBroker} 超过 ${maxBrokerBufferBytes} 字节`);
  }
  const perUser = perBroker * maxTasks;
  if (perUser > maxUserBufferBytes) {
    throw new Error(`全部终端聚合缓冲预算 ${perUser} 超过 ${maxUserBufferBytes} 字节`);
  }
  if (attachOutputBytes > maxAttachOutputBytes) {
    throw new Error(`单客户端 attach 输出缓冲 ${attachOutputBytes} 超过 ${maxAttachOutputBytes} 字节`);
  }
  const perUserAttach = maxTasks * maxClients * attachOutputBytes;
  if (perUserAttach > maxUserAttachOutputBytes) {
    throw new Error(`全部 attach 输出缓冲预算 ${perUserAttach} 超过 ${maxUserAttachOutputBytes} 字节`);
  }
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
    pythonPath: "python3",
    brokerScriptPath: input.brokerScriptPath,
    socketDirectory: input.socketDirectory,
    shellPath,
    environment,
    maxTasks: 12,
    maxClientsPerTask: 4,
    replayBytes: 512 * 1024,
    maxClientInputBytes: 256 * 1024,
    maxClientOutputBytes: 2 * 1024 * 1024,
    maxAttachOutputBytes: defaultAttachOutputBytes,
    maxPtyInputBytes: 256 * 1024,
    requireCgroup: true,
  };
}

export function applyRuntimeOverrides(base: RuntimeConfig, overrides: RuntimeOverrides): RuntimeConfig {
  const shellPath = overrides.shellPath || base.shellPath;
  const runtime = {
    registryPath: overrides.registryPath || base.registryPath,
    tmuxPath: overrides.tmuxPath || base.tmuxPath,
    socketName: overrides.socketName || base.socketName,
    pythonPath: overrides.pythonPath || base.pythonPath,
    brokerScriptPath: base.brokerScriptPath,
    socketDirectory: overrides.socketDirectory || base.socketDirectory,
    shellPath,
    maxTasks: overrides.maxTasks ?? base.maxTasks,
    maxClientsPerTask: overrides.maxClientsPerTask ?? base.maxClientsPerTask,
    replayBytes: overrides.replayBytes ?? base.replayBytes,
    maxClientInputBytes: overrides.maxClientInputBytes ?? base.maxClientInputBytes,
    maxClientOutputBytes: overrides.maxClientOutputBytes ?? base.maxClientOutputBytes,
    maxAttachOutputBytes: overrides.maxAttachOutputBytes ?? base.maxAttachOutputBytes,
    maxPtyInputBytes: overrides.maxPtyInputBytes ?? base.maxPtyInputBytes,
    requireCgroup: overrides.requireCgroup ?? base.requireCgroup,
    environment: {
      ...base.environment,
      ...overrides.environment,
      SHELL: shellPath,
    },
  };
  validateRuntimeResourceBudget(runtime);
  return runtime;
}

export function resolveExecutablePath(
  command: string,
  pathValue: string,
  exists: (path: string) => boolean,
  label = "tmux",
): string {
  const candidates = isAbsolute(command)
    ? [command]
    : pathValue.split(delimiter).filter(Boolean).map((directory) => join(directory, command));
  const resolved = candidates.find(exists);
  if (!resolved) {
    throw new Error(`未找到 ${label}（配置路径：${command}）`);
  }
  return resolved;
}
