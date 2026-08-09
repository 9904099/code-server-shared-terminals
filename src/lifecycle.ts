export class LifecycleGuard {
  private generation = 0;
  private isDisposed = false;

  get disposed(): boolean {
    return this.isDisposed;
  }

  capture(): number {
    return this.generation;
  }

  isCurrent(token: number): boolean {
    return !this.isDisposed && token === this.generation;
  }

  async resolveOrCompensate<T>(
    token: number,
    operation: Promise<T>,
    compensate: (value: T) => Promise<void>,
  ): Promise<T | undefined> {
    const value = await operation;
    if (this.isCurrent(token)) {
      return value;
    }
    await compensate(value);
    return undefined;
  }

  dispose(): void {
    if (this.isDisposed) {
      return;
    }
    this.isDisposed = true;
    this.generation += 1;
  }
}

export function shouldPublishTerminalClose(reason: number | undefined, userReason: number): boolean {
  return reason === userReason;
}

export function shouldReportUnavailableTerminal(publish: boolean): boolean {
  return publish;
}

export function requireCommandArgument<T>(argument: T | undefined): T {
  if (argument === undefined) {
    throw new Error("请从共享终端任务列表选择任务后重试");
  }
  return argument;
}

export type TerminalCloseResult =
  | { kind: "suppressed" }
  | { kind: "pending" }
  | { kind: "tracked"; id: string; reason: number | undefined };

export type TerminalRegistrationResult =
  | { kind: "registered" }
  | { kind: "duplicate" }
  | { kind: "closed"; reason: number | undefined; publish: boolean };

export class TerminalRegistry<T extends object> {
  private readonly terminals = new Map<string, T>();
  private readonly suppressedCloseEvents = new WeakSet<T>();
  private readonly closedTerminals = new WeakMap<T, {
    reason: number | undefined;
    publicationClaimed: boolean;
  }>();

  get size(): number {
    return this.terminals.size;
  }

  get(id: string): T | undefined {
    return this.terminals.get(id);
  }

  set(id: string, terminal: T): boolean {
    if (this.isClosed(terminal)) {
      return false;
    }
    this.terminals.set(id, terminal);
    return true;
  }

  setOrDisposeDuplicate(id: string, terminal: T, disposer: (terminal: T) => void): boolean {
    if (this.closedTerminals.has(terminal) || this.suppressedCloseEvents.has(terminal)) {
      return false;
    }
    const existing = this.terminals.get(id);
    if (existing === undefined) {
      this.terminals.set(id, terminal);
      return true;
    }
    if (existing === terminal) {
      return true;
    }
    this.suppressedCloseEvents.add(terminal);
    disposer(terminal);
    return false;
  }

  registerAfterLookup(id: string, terminal: T, disposer: (terminal: T) => void): TerminalRegistrationResult {
    const closed = this.closedTerminals.get(terminal);
    if (closed !== undefined) {
      const publish = !closed.publicationClaimed;
      closed.publicationClaimed = true;
      return { kind: "closed", reason: closed.reason, publish };
    }
    return this.setOrDisposeDuplicate(id, terminal, disposer)
      ? { kind: "registered" }
      : { kind: "duplicate" };
  }

  handleClose(terminal: T, reason: number | undefined): TerminalCloseResult {
    if (this.consumeSuppressedClose(terminal)) {
      this.closedTerminals.set(terminal, { reason, publicationClaimed: true });
      return { kind: "suppressed" };
    }
    const id = this.findId(terminal);
    if (id !== undefined) {
      this.terminals.delete(id);
      this.closedTerminals.set(terminal, { reason, publicationClaimed: true });
      return { kind: "tracked", id, reason };
    }
    if (!this.closedTerminals.has(terminal)) {
      this.closedTerminals.set(terminal, { reason, publicationClaimed: false });
    }
    return { kind: "pending" };
  }

  isClosed(terminal: T): boolean {
    return this.closedTerminals.has(terminal) || this.suppressedCloseEvents.has(terminal);
  }

  findId(terminal: T): string | undefined {
    for (const [id, candidate] of this.terminals) {
      if (candidate === terminal) {
        return id;
      }
    }
    return undefined;
  }

  delete(id: string): boolean {
    return this.terminals.delete(id);
  }

  consumeSuppressedClose(terminal: T): boolean {
    return this.suppressedCloseEvents.delete(terminal);
  }

  dispose(id: string, disposer: (terminal: T) => void): void {
    const terminal = this.terminals.get(id);
    if (!terminal) {
      return;
    }
    this.suppressedCloseEvents.add(terminal);
    this.terminals.delete(id);
    disposer(terminal);
  }

  disposeMissing(activeIds: ReadonlySet<string>, disposer: (terminal: T) => void): void {
    for (const id of [...this.terminals.keys()]) {
      if (!activeIds.has(id)) {
        this.dispose(id, disposer);
      }
    }
  }

  clear(): void {
    this.terminals.clear();
  }
}

export class ProfilePublicationGate {
  private inFlight = 0;
  private readonly pending = new Map<string, number>();

  begin(): () => void {
    this.inFlight += 1;
    let finished = false;
    return () => {
      if (finished) {
        return;
      }
      finished = true;
      this.inFlight -= 1;
    };
  }

  publish(taskId: string, deadline: number): void {
    this.pending.set(taskId, deadline);
  }

  consume(taskId: string): void {
    this.pending.delete(taskId);
  }

  reconcile(taskIds: ReadonlySet<string>, now: number): void {
    for (const [taskId, deadline] of this.pending) {
      if (!taskIds.has(taskId) || deadline <= now) {
        this.pending.delete(taskId);
      }
    }
  }

  canAutoOpen(taskId: string, _now: number): boolean {
    return this.inFlight === 0 && !this.pending.has(taskId);
  }

  clear(): void {
    this.pending.clear();
  }
}

export class CoalescingExecutor {
  private running: Promise<void> | undefined;
  private pending = false;
  private disposed = false;

  constructor(private readonly operation: () => Promise<void>) {}

  run(): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    if (this.running) {
      this.pending = true;
      return this.running;
    }
    const running = (async () => {
      do {
        this.pending = false;
        await this.operation();
      } while (this.pending && !this.disposed);
    })();
    this.running = running;
    const clear = () => {
      if (this.running === running) {
        this.running = undefined;
      }
    };
    running.then(clear, clear);
    return running;
  }

  dispose(): void {
    this.disposed = true;
    this.pending = false;
  }
}
