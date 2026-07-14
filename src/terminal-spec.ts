import { SharedTask } from "./task-store";

export const sharedTerminalPrefix = "共享 · ";

export interface TerminalSpec {
  name: string;
  shellPath: string;
  shellArgs: string[];
  cwd: string;
}

export function buildTerminalSpec(task: SharedTask, tmuxPath = "tmux", socketName = "code-server-shared-tasks"): TerminalSpec {
  return {
    name: `${sharedTerminalPrefix}${task.name}`,
    shellPath: tmuxPath,
    shellArgs: ["-L", socketName, "attach-session", "-t", task.session],
    cwd: task.cwd,
  };
}
