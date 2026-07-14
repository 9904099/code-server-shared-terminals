import { SharedTask, sharedTmuxSocket } from "./task-store";

export const sharedTerminalPrefix = "共享 · ";

export interface TerminalSpec {
  name: string;
  shellPath: string;
  shellArgs: string[];
  cwd: string;
}

export function buildTerminalSpec(task: SharedTask): TerminalSpec {
  return {
    name: `${sharedTerminalPrefix}${task.name}`,
    shellPath: "/usr/bin/tmux",
    shellArgs: ["-L", sharedTmuxSocket, "attach-session", "-t", task.session],
    cwd: task.cwd,
  };
}
