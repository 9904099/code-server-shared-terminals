import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname } from "node:path";
import * as vscode from "vscode";

import { applyRuntimeOverrides, RuntimeConfig, resolveExecutablePath, resolveRuntimeConfig } from "./runtime-config";
import { SharedTask, SharedTaskStatus, TaskStore } from "./task-store";
import { buildTerminalSpec, sharedTerminalPrefix } from "./terminal-spec";

class TaskItem extends vscode.TreeItem {
  constructor(readonly task: SharedTaskStatus) {
    super(task.name, vscode.TreeItemCollapsibleState.None);
    this.description = task.alive ? "运行中" : "已停止";
    this.iconPath = new vscode.ThemeIcon(task.alive ? "terminal" : "warning");
    this.contextValue = "sharedTerminalTask";
    this.tooltip = `${task.cwd}\n${task.session}`;
    this.command = {
      command: "sharedTerminals.open",
      title: "打开共享终端",
      arguments: [task],
    };
  }
}

class TaskTreeProvider implements vscode.TreeDataProvider<TaskItem> {
  private readonly changed = new vscode.EventEmitter<TaskItem | undefined>();
  readonly onDidChangeTreeData = this.changed.event;

  constructor(private readonly store: TaskStore) {}

  refresh(): void {
    this.changed.fire(undefined);
  }

  getTreeItem(item: TaskItem): vscode.TreeItem {
    return item;
  }

  async getChildren(): Promise<TaskItem[]> {
    return (await this.store.listWithStatus()).map((task) => new TaskItem(task));
  }
}

class SharedTerminalController implements vscode.Disposable {
  private readonly terminals = new Map<string, vscode.Terminal>();
  private readonly disposables: vscode.Disposable[] = [];

  constructor(
    private readonly store: TaskStore,
    private readonly provider: TaskTreeProvider,
    private readonly runtime: RuntimeConfig,
    private readonly defaultCwd: string,
  ) {
    this.disposables.push(vscode.window.onDidCloseTerminal((terminal) => {
      for (const [id, candidate] of this.terminals) {
        if (candidate === terminal) {
          this.terminals.delete(id);
        }
      }
    }));
  }

  dispose(): void {
    for (const disposable of this.disposables) {
      disposable.dispose();
    }
  }

  async create(): Promise<void> {
    const name = await vscode.window.showInputBox({
      title: "新建共享终端任务",
      prompt: "该名称会同步到所有 code-server 浏览器窗口",
      validateInput: (value) => value.trim() ? undefined : "任务名称不能为空",
    });
    if (!name) {
      return;
    }
    const cwd = await vscode.window.showInputBox({
      title: "共享终端工作目录",
      value: this.defaultCwd,
      validateInput: (value) => value.startsWith("/") ? undefined : "请输入绝对路径",
    });
    if (!cwd) {
      return;
    }
    await vscode.workspace.fs.stat(vscode.Uri.file(cwd));
    const task = await this.store.create(name, cwd);
    this.provider.refresh();
    await this.open(task, true);
  }

  async open(task: SharedTask, show = true): Promise<void> {
    const status = (await this.store.listWithStatus()).find((candidate) => candidate.id === task.id);
    if (!status?.alive) {
      throw new Error(`共享终端任务“${task.name}”未运行`);
    }
    let terminal = this.terminals.get(task.id);
    if (!terminal) {
      const expectedName = `${sharedTerminalPrefix}${task.name}`;
      terminal = vscode.window.terminals.find((candidate) => candidate.name === expectedName);
      if (!terminal) {
        terminal = vscode.window.createTerminal(buildTerminalSpec(task, this.runtime.tmuxPath, this.runtime.socketName));
      }
      this.terminals.set(task.id, terminal);
    }
    if (show) {
      terminal.show(false);
    }
  }

  async openAll(): Promise<void> {
    const tasks = await this.store.listWithStatus();
    for (const task of tasks.filter((candidate) => candidate.alive)) {
      await this.open(task, false);
    }
  }

  async rename(task: SharedTask): Promise<void> {
    const name = await vscode.window.showInputBox({
      title: "重命名共享终端任务",
      value: task.name,
      validateInput: (value) => value.trim() ? undefined : "任务名称不能为空",
    });
    if (!name || name.trim() === task.name) {
      return;
    }
    const renamed = await this.store.rename(task.id, name);
    this.terminals.get(task.id)?.dispose();
    this.terminals.delete(task.id);
    this.provider.refresh();
    await this.open(renamed, true);
  }

  async delete(task: SharedTask): Promise<void> {
    const answer = await vscode.window.showWarningMessage(
      `确定结束并删除共享终端“${task.name}”吗？其中运行的命令会立即终止。`,
      { modal: true },
      "结束并删除",
    );
    if (answer !== "结束并删除") {
      return;
    }
    this.terminals.get(task.id)?.dispose();
    this.terminals.delete(task.id);
    await this.store.delete(task.id);
    this.provider.refresh();
  }

  async synchronize(): Promise<void> {
    this.provider.refresh();
    if (vscode.workspace.getConfiguration("sharedTerminals").get<boolean>("autoOpen", true)) {
      await this.openAll();
    }
  }
}

function taskFromArgument(argument: SharedTask | TaskItem): SharedTask {
  return argument instanceof TaskItem ? argument.task : argument;
}

function reportError(error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  void vscode.window.showErrorMessage(`共享终端：${message}`);
}

function configuredValue(configuration: vscode.WorkspaceConfiguration, key: string): string | undefined {
  const value = configuration.get<string>(key, "").trim();
  return value || undefined;
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const configuration = vscode.workspace.getConfiguration("sharedTerminals");
  const baseRuntime = resolveRuntimeConfig({
    home: homedir(),
    environment: process.env,
    globalStoragePath: context.globalStorageUri.fsPath,
  });
  const runtime = applyRuntimeOverrides(baseRuntime, {
    registryPath: configuredValue(configuration, "registryPath"),
    tmuxPath: configuredValue(configuration, "tmuxPath"),
    socketName: configuredValue(configuration, "socketName"),
    shellPath: configuredValue(configuration, "shellPath"),
    environment: configuration.get<Record<string, string>>("environment", {}),
  });
  runtime.tmuxPath = resolveExecutablePath(runtime.tmuxPath, runtime.environment.PATH, existsSync);
  const defaultCwd = configuredValue(configuration, "defaultCwd")
    || vscode.workspace.workspaceFolders?.[0]?.uri.fsPath
    || homedir();
  const store = new TaskStore(runtime.registryPath, undefined, runtime);
  const provider = new TaskTreeProvider(store);
  const controller = new SharedTerminalController(store, provider, runtime, defaultCwd);

  context.subscriptions.push(
    controller,
    vscode.window.registerTreeDataProvider("sharedTerminals.tasks", provider),
    vscode.commands.registerCommand("sharedTerminals.create", () => controller.create().catch(reportError)),
    vscode.commands.registerCommand("sharedTerminals.open", (argument: SharedTask | TaskItem) => controller.open(taskFromArgument(argument)).catch(reportError)),
    vscode.commands.registerCommand("sharedTerminals.rename", (argument: SharedTask | TaskItem) => controller.rename(taskFromArgument(argument)).catch(reportError)),
    vscode.commands.registerCommand("sharedTerminals.delete", (argument: SharedTask | TaskItem) => controller.delete(taskFromArgument(argument)).catch(reportError)),
    vscode.commands.registerCommand("sharedTerminals.refresh", () => controller.synchronize().catch(reportError)),
    vscode.commands.registerCommand("sharedTerminals.openAll", () => controller.openAll().catch(reportError)),
  );

  const watcher = vscode.workspace.createFileSystemWatcher(new vscode.RelativePattern(dirname(runtime.registryPath), basename(runtime.registryPath)));
  const synchronize = () => controller.synchronize().catch(reportError);
  const poller = setInterval(synchronize, 3000);
  context.subscriptions.push(
    watcher,
    watcher.onDidCreate(synchronize),
    watcher.onDidChange(synchronize),
    watcher.onDidDelete(synchronize),
    { dispose: () => clearInterval(poller) },
  );

  await store.verifyTmux().catch(reportError);
  await controller.synchronize();
}

export function deactivate(): void {}
