import { existsSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import * as vscode from "vscode";

import {
  CoalescingExecutor,
  LifecycleGuard,
  ProfilePublicationGate,
  requireCommandArgument,
  shouldReportUnavailableTerminal,
  shouldPublishTerminalClose,
  TerminalRegistry,
} from "./lifecycle";
import { applyRuntimeOverrides, RuntimeConfig, resolveExecutablePath, resolveRuntimeConfig } from "./runtime-config";
import { SharedTask, SharedTaskStatus, TaskStore } from "./task-store";
import { buildTerminalSpec, terminalCreationMatchesTask } from "./terminal-spec";

function terminalMatchesTask(terminal: vscode.Terminal, task: SharedTask, runtime: RuntimeConfig): boolean {
  const options = terminal.creationOptions;
  if (!("shellPath" in options)) {
    return false;
  }
  return terminalCreationMatchesTask(options, task, runtime);
}

class TaskItem extends vscode.TreeItem {
  constructor(readonly task: SharedTaskStatus) {
    super(task.name, vscode.TreeItemCollapsibleState.None);
    const starting = task.backend === "pty-broker" && task.brokerState === "starting";
    this.description = starting ? "启动待恢复" : task.alive ? "运行中" : "已停止";
    this.iconPath = new vscode.ThemeIcon(task.alive ? "terminal" : starting ? "sync" : "warning");
    this.contextValue = "sharedTerminalTask";
    this.tooltip = `${task.cwd}\n${task.session}`;
    this.command = {
      command: "sharedTerminals.open",
      title: "打开共享终端",
      arguments: [task],
    };
  }
}

class TaskTreeProvider implements vscode.TreeDataProvider<TaskItem>, vscode.Disposable {
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

  dispose(): void {
    this.changed.dispose();
  }
}

class SharedTerminalController implements vscode.Disposable {
  private readonly terminals = new TerminalRegistry<vscode.Terminal>();
  private readonly pendingProfileTasks = new Map<string, number>();
  private readonly profilePublications = new ProfilePublicationGate();

  private readonly disposables: vscode.Disposable[] = [];
  private readonly lifecycle = new LifecycleGuard();

  constructor(
    private readonly store: TaskStore,
    private readonly provider: TaskTreeProvider,
    private readonly runtime: RuntimeConfig,
    private readonly defaultCwd: string,
  ) {
    this.disposables.push(vscode.window.onDidCloseTerminal((terminal) => {
      if (this.lifecycle.disposed) {
        return;
      }
      const close = this.terminals.handleClose(terminal, terminal.exitStatus?.reason);
      if (close.kind === "tracked") {
        this.publishTerminalClose(close.id, close.reason);
      }
    }));
  }

  private publishTerminalClose(id: string, reason: number | undefined): void {
    if (!shouldPublishTerminalClose(reason, vscode.TerminalExitReason.User)) {
      return;
    }
    const token = this.lifecycle.capture();
    void this.store.closeIfPresent(id).then((changed) => {
      if (changed && this.lifecycle.isCurrent(token)) {
        this.provider.refresh();
      }
    }).catch(reportError);
  }

  dispose(): void {
    this.lifecycle.dispose();
    for (const disposable of this.disposables) {
      disposable.dispose();
    }
    this.disposables.length = 0;
    this.terminals.clear();
    this.pendingProfileTasks.clear();
    this.profilePublications.clear();
  }

  async create(): Promise<void> {
    const token = this.lifecycle.capture();
    const name = await vscode.window.showInputBox({
      title: "新建共享终端任务",
      prompt: "该名称会同步到所有 code-server 浏览器窗口",
      validateInput: (value) => value.trim() ? undefined : "任务名称不能为空",
    });
    if (!name || !this.lifecycle.isCurrent(token)) {
      return;
    }
    const cwd = await vscode.window.showInputBox({
      title: "共享终端工作目录",
      value: this.defaultCwd,
      validateInput: (value) => value.startsWith("/") ? undefined : "请输入绝对路径",
    });
    if (!cwd || !this.lifecycle.isCurrent(token)) {
      return;
    }
    await vscode.workspace.fs.stat(vscode.Uri.file(cwd));
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    const task = await this.lifecycle.resolveOrCompensate(
      token,
      this.store.create(name, cwd),
      (created) => this.store.delete(created.id),
    );
    if (!task) {
      return;
    }
    this.provider.refresh();
    await this.open(task, true);
  }

  async provideProfile(): Promise<vscode.TerminalProfile> {
    const token = this.lifecycle.capture();
    const finishPublication = this.profilePublications.begin();
    try {
      const task = await this.lifecycle.resolveOrCompensate(
        token,
        this.store.createAutomatic(this.defaultCwd),
        (created) => this.store.delete(created.id),
      );
      if (!task) {
        throw new Error("扩展已释放，已取消创建本地共享终端标签");
      }
      const deadline = Date.now() + 5_000;
      this.pendingProfileTasks.set(task.id, deadline);
      this.profilePublications.publish(task.id, deadline);
      this.provider.refresh();
      return new vscode.TerminalProfile(buildTerminalSpec(task, this.runtime));
    } finally {
      finishPublication();
    }
  }

  async trackTerminal(terminal: vscode.Terminal): Promise<void> {
    const token = this.lifecycle.capture();
    const tasks = await this.store.list();
    if (this.lifecycle.isCurrent(token)) {
      this.trackTerminalFromTasks(terminal, tasks);
    }
  }

  private trackTerminalFromTasks(terminal: vscode.Terminal, tasks: readonly SharedTask[]): void {
    const task = tasks.find(
      (candidate) => terminalMatchesTask(terminal, candidate, this.runtime),
    );
    if (task) {
      const registration = this.terminals.registerAfterLookup(
        task.id,
        terminal,
        (duplicate) => duplicate.dispose(),
      );
      this.pendingProfileTasks.delete(task.id);
      this.profilePublications.consume(task.id);
      if (registration.kind === "closed" && registration.publish) {
        this.publishTerminalClose(task.id, registration.reason);
      }
    }
  }

  async open(task: SharedTask, show = true, publish = true): Promise<void> {
    const token = this.lifecycle.capture();
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    if (publish) {
      task = await this.store.setOpen(task.id, true);
      if (!this.lifecycle.isCurrent(token)) {
        return;
      }
    }
    const status = (await this.store.listWithStatus()).find((candidate) => candidate.id === task.id);
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    if (!status?.alive) {
      if (!shouldReportUnavailableTerminal(publish)) {
        return;
      }
      throw new Error(`共享终端任务“${task.name}”未运行`);
    }
    let terminal = this.terminals.get(task.id);
    if (!terminal) {
      terminal = vscode.window.terminals.find(
        (candidate) => !this.terminals.isClosed(candidate)
          && terminalMatchesTask(candidate, task, this.runtime),
      );
      let createdHere = false;
      if (!terminal) {
        terminal = vscode.window.createTerminal(buildTerminalSpec(task, this.runtime));
        createdHere = true;
      }
      if (!this.lifecycle.isCurrent(token)) {
        if (createdHere) {
          terminal.dispose();
        }
        return;
      }
      this.terminals.set(task.id, terminal);
    }
    if (show) {
      terminal.show(false);
    }
  }

  async openAll(): Promise<void> {
    const token = this.lifecycle.capture();
    const tasks = await this.store.listWithStatus();
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    for (const task of tasks.filter((candidate) => candidate.alive)) {
      if (!this.lifecycle.isCurrent(token)) {
        return;
      }
      await this.open(task, false);
    }
  }

  async rename(task: SharedTask): Promise<void> {
    const token = this.lifecycle.capture();
    const name = await vscode.window.showInputBox({
      title: "重命名共享终端任务",
      value: task.name,
      validateInput: (value) => value.trim() ? undefined : "任务名称不能为空",
    });
    if (!name || name.trim() === task.name || !this.lifecycle.isCurrent(token)) {
      return;
    }
    const renamed = await this.store.rename(task.id, name);
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    this.disposeLocal(task.id);
    this.provider.refresh();
    await this.open(renamed, true);
  }

  async delete(task: SharedTask): Promise<void> {
    const token = this.lifecycle.capture();
    const answer = await vscode.window.showWarningMessage(
      `确定结束并删除共享终端“${task.name}”吗？其中运行的命令会立即终止。`,
      { modal: true },
      "结束并删除",
    );
    if (answer !== "结束并删除" || !this.lifecycle.isCurrent(token)) {
      return;
    }
    await this.store.delete(task.id);
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    this.disposeLocal(task.id);
    this.provider.refresh();
  }

  private disposeLocal(id: string): void {
    this.terminals.dispose(id, (terminal) => terminal.dispose());
  }

  async synchronize(): Promise<void> {
    const token = this.lifecycle.capture();
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    this.provider.refresh();
    const tasks = await this.store.listWithStatus();
    if (!this.lifecycle.isCurrent(token)) {
      return;
    }
    for (const terminal of vscode.window.terminals) {
      this.trackTerminalFromTasks(terminal, tasks);
    }
    this.terminals.disposeMissing(
      new Set(tasks.filter((candidate) => candidate.open).map((task) => task.id)),
      (terminal) => terminal.dispose(),
    );
    const now = Date.now();
    for (const [id, deadline] of this.pendingProfileTasks) {
      if (deadline <= now || !tasks.some((task) => task.id === id)) {
        this.pendingProfileTasks.delete(id);
      }
    }
    this.profilePublications.reconcile(new Set(tasks.map((task) => task.id)), now);
    if (vscode.workspace.getConfiguration("sharedTerminals").get<boolean>("autoOpen", true)) {
      for (const task of tasks.filter(
        (candidate) => candidate.alive
          && candidate.open
          && !this.pendingProfileTasks.has(candidate.id)
          && this.profilePublications.canAutoOpen(candidate.id, now),
      )) {
        if (!this.lifecycle.isCurrent(token)) {
          return;
        }
        await this.open(task, false, false);
      }
    }
  }
}

function taskFromArgument(argument: SharedTask | TaskItem | undefined): SharedTask {
  const selected = requireCommandArgument(argument);
  return selected instanceof TaskItem ? selected.task : selected;
}

function reportError(error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  void vscode.window.showErrorMessage(`共享终端：${message}`);
}

function configuredValue(configuration: vscode.WorkspaceConfiguration, key: string): string | undefined {
  const value = configuration.get<string>(key, "").trim();
  return value || undefined;
}

function configuredInteger(
  configuration: vscode.WorkspaceConfiguration,
  key: string,
  minimum: number,
  maximum: number,
): number | undefined {
  const value = configuration.get<number>(key);
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${key} 必须是 ${minimum} 到 ${maximum} 之间的整数`);
  }
  return value;
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const configuration = vscode.workspace.getConfiguration("sharedTerminals");
  const baseRuntime = resolveRuntimeConfig({
    home: homedir(),
    environment: process.env,
    globalStoragePath: context.globalStorageUri.fsPath,
    brokerScriptPath: context.asAbsolutePath("scripts/pty_broker.py"),
    socketDirectory: join(tmpdir(), `code-server-shared-terminals-${process.getuid?.() ?? "user"}`),
  });
  const runtime = applyRuntimeOverrides(baseRuntime, {
    registryPath: configuredValue(configuration, "registryPath"),
    tmuxPath: configuredValue(configuration, "tmuxPath"),
    socketName: configuredValue(configuration, "socketName"),
    pythonPath: configuredValue(configuration, "pythonPath"),
    socketDirectory: configuredValue(configuration, "socketDirectory"),
    shellPath: configuredValue(configuration, "shellPath"),
    environment: configuration.get<Record<string, string>>("environment", {}),
    maxTasks: configuredInteger(configuration, "maxTasks", 1, 16),
    maxClientsPerTask: configuredInteger(configuration, "maxClientsPerTask", 1, 8),
    replayBytes: configuredInteger(configuration, "replayBytes", 0, 1024 * 1024),
    maxClientInputBytes: configuredInteger(configuration, "maxClientInputBytes", 5, 1024 * 1024 + 5),
    maxClientOutputBytes: configuredInteger(configuration, "maxClientOutputBytes", 5, 8 * 1024 * 1024),
    maxPtyInputBytes: configuredInteger(configuration, "maxPtyInputBytes", 1, 1024 * 1024),
  });
  runtime.pythonPath = resolveExecutablePath(runtime.pythonPath, runtime.environment.PATH, existsSync, "Python 3");
  const defaultCwd = configuredValue(configuration, "defaultCwd")
    || vscode.workspace.workspaceFolders?.[0]?.uri.fsPath
    || homedir();
  const store = new TaskStore(runtime.registryPath, undefined, runtime);
  const provider = new TaskTreeProvider(store);
  const controller = new SharedTerminalController(store, provider, runtime, defaultCwd);
  const synchronize = new CoalescingExecutor(() => controller.synchronize());
  let poller: NodeJS.Timeout | undefined;
  const activationDisposables: vscode.Disposable[] = [
    controller,
    provider,
    {
      dispose: () => {
        if (poller !== undefined) {
          clearInterval(poller);
        }
        synchronize.dispose();
      },
    },
  ];
  try {
    await store.verifyBroker().catch(reportError);
    await synchronize.run();

    activationDisposables.push(vscode.window.registerTreeDataProvider("sharedTerminals.tasks", provider));
    activationDisposables.push(vscode.window.registerTerminalProfileProvider("sharedTerminals.fast", {
      provideTerminalProfile: () => controller.provideProfile(),
    }));
    activationDisposables.push(vscode.window.onDidOpenTerminal(
      (terminal) => controller.trackTerminal(terminal).catch(reportError),
    ));
    activationDisposables.push(vscode.commands.registerCommand(
      "sharedTerminals.create",
      () => controller.create().catch(reportError),
    ));
    activationDisposables.push(vscode.commands.registerCommand(
      "sharedTerminals.open",
      (argument: SharedTask | TaskItem | undefined) => controller.open(taskFromArgument(argument)).catch(reportError),
    ));
    activationDisposables.push(vscode.commands.registerCommand(
      "sharedTerminals.rename",
      (argument: SharedTask | TaskItem | undefined) => controller.rename(taskFromArgument(argument)).catch(reportError),
    ));
    activationDisposables.push(vscode.commands.registerCommand(
      "sharedTerminals.delete",
      (argument: SharedTask | TaskItem | undefined) => controller.delete(taskFromArgument(argument)).catch(reportError),
    ));
    activationDisposables.push(vscode.commands.registerCommand(
      "sharedTerminals.refresh",
      () => synchronize.run().catch(reportError),
    ));
    activationDisposables.push(vscode.commands.registerCommand(
      "sharedTerminals.openAll",
      () => controller.openAll().catch(reportError),
    ));

    const watcher = vscode.workspace.createFileSystemWatcher(
      new vscode.RelativePattern(dirname(runtime.registryPath), basename(runtime.registryPath)),
    );
    const requestSynchronization = () => { void synchronize.run().catch(reportError); };
    poller = setInterval(requestSynchronization, 3000);
    activationDisposables.push(watcher);
    activationDisposables.push(watcher.onDidCreate(requestSynchronization));
    activationDisposables.push(watcher.onDidChange(requestSynchronization));
    activationDisposables.push(watcher.onDidDelete(requestSynchronization));
    context.subscriptions.push(...activationDisposables);
  } catch (error) {
    for (const disposable of activationDisposables.reverse()) {
      disposable.dispose();
    }
    throw error;
  }
}

export function deactivate(): void {}
