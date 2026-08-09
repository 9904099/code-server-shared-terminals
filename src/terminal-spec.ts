import { join } from "node:path";

import { TaskRuntimeConfig } from "./runtime-config";
import { SharedTask } from "./task-store";

export const sharedTerminalPrefix = "共享 · ";
export const sharedTerminalTaskIdEnvironmentKey = "CODE_SERVER_SHARED_TERMINAL_TASK_ID";

export interface TerminalSpec {
  name: string;
  shellPath: string;
  shellArgs: string[];
  cwd: string;
  env: Record<string, string>;
  isTransient: boolean;
}

export interface TerminalCreationOptionsIdentity {
  shellPath?: string;
  shellArgs?: string | readonly string[];
  env?: Record<string, string | null | undefined>;
}

export function buildTerminalSpec(task: SharedTask, runtime: TaskRuntimeConfig): TerminalSpec {
  if ((task.backend ?? "tmux") === "pty-broker") {
    return {
      name: `${sharedTerminalPrefix}${task.name}`,
      shellPath: runtime.pythonPath,
      shellArgs: [
        runtime.brokerScriptPath,
        "attach",
        "--socket",
        task.brokerDataPath ?? join(runtime.socketDirectory, `${task.session}.sock`),
      ],
      cwd: task.cwd,
      env: { [sharedTerminalTaskIdEnvironmentKey]: task.id },
      isTransient: true,
    };
  }
  return {
    name: `${sharedTerminalPrefix}${task.name}`,
    shellPath: runtime.tmuxPath,
    shellArgs: ["-L", runtime.socketName, "attach-session", "-t", task.session],
    cwd: task.cwd,
    env: { [sharedTerminalTaskIdEnvironmentKey]: task.id },
    isTransient: true,
  };
}

export function terminalCreationMatchesTask(
  options: TerminalCreationOptionsIdentity,
  task: SharedTask,
  _runtime: TaskRuntimeConfig,
): boolean {
  const taskId = options.env?.[sharedTerminalTaskIdEnvironmentKey];
  return typeof taskId === "string" && taskId === task.id;
}
