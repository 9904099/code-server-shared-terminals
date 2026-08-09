import assert from "node:assert/strict";
import test from "node:test";

import {
  CoalescingExecutor,
  LifecycleGuard,
  ProfilePublicationGate,
  requireCommandArgument,
  shouldReportUnavailableTerminal,
  shouldPublishTerminalClose,
  TerminalRegistry,
} from "../src/lifecycle";

test("lifecycle guard invalidates every token after disposal", () => {
  const lifecycle = new LifecycleGuard();
  const token = lifecycle.capture();
  assert.equal(lifecycle.isCurrent(token), true);
  lifecycle.dispose();
  assert.equal(lifecycle.isCurrent(token), false);
  assert.equal(lifecycle.disposed, true);
});

test("lifecycle guard compensates an operation that resolves after disposal", async () => {
  const lifecycle = new LifecycleGuard();
  const token = lifecycle.capture();
  let resolve!: (value: { id: string }) => void;
  const operation = new Promise<{ id: string }>((complete) => { resolve = complete; });
  const compensated: string[] = [];
  const result = lifecycle.resolveOrCompensate(token, operation, async (value) => {
    compensated.push(value.id);
  });

  lifecycle.dispose();
  resolve({ id: "created-after-dispose" });

  assert.equal(await result, undefined);
  assert.deepEqual(compensated, ["created-after-dispose"]);
});

test("coalescing executor runs one operation at a time and performs at most one pending rerun", async () => {
  let starts = 0;
  let releaseFirst!: () => void;
  const firstRelease = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const executor = new CoalescingExecutor(async () => {
    starts += 1;
    if (starts === 1) {
      await firstRelease;
    }
  });

  const first = executor.run();
  const second = executor.run();
  const third = executor.run();
  assert.equal(first, second);
  assert.equal(second, third);
  assert.equal(starts, 1);
  releaseFirst();
  await first;
  assert.equal(starts, 2);
});

test("coalescing executor disposal suppresses queued and future reruns", async () => {
  let starts = 0;
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => { release = resolve; });
  const executor = new CoalescingExecutor(async () => {
    starts += 1;
    await blocked;
  });

  const running = executor.run();
  executor.run();
  executor.dispose();
  release();
  await running;
  await executor.run();
  assert.equal(starts, 1);
});

test("coalescing executor releases its single-flight state after a rejected operation", async () => {
  let starts = 0;
  const executor = new CoalescingExecutor(async () => {
    starts += 1;
    if (starts === 1) {
      throw new Error("injected synchronization failure");
    }
  });

  await assert.rejects(() => executor.run(), /injected synchronization failure/);
  await executor.run();
  assert.equal(starts, 2);
  executor.dispose();
});

test("profile publication gate suppresses watcher auto-open across the registry publication race", () => {
  const gate = new ProfilePublicationGate();
  const finish = gate.begin();

  assert.equal(gate.canAutoOpen("task-1", 1_000), false, "watcher must not open a just-published task");
  gate.publish("task-1", 6_000);
  finish();

  assert.equal(gate.canAutoOpen("task-1", 1_000), false, "VS Code still owns creation of the profile tab");
  gate.reconcile(new Set(["task-1"]), 6_001);
  assert.equal(gate.canAutoOpen("task-1", 6_001), true, "expired profile publication may recover through auto-open");
});

test("terminal registry disposes mappings removed from the shared registry", () => {
  const registry = new TerminalRegistry<object>();
  const local = {};
  const remote = {};
  const disposed: object[] = [];
  registry.set("local", local);
  registry.set("remote", remote);

  registry.disposeMissing(new Set(["local"]), (terminal) => disposed.push(terminal));

  assert.deepEqual(disposed, [remote]);
  assert.equal(registry.get("local"), local);
  assert.equal(registry.get("remote"), undefined);
  assert.equal(registry.consumeSuppressedClose(remote), true);
  assert.equal(registry.consumeSuppressedClose(remote), false);
});

test("terminal registry suppression does not require a close event to release strong references", () => {
  const registry = new TerminalRegistry<object>();
  const terminal = {};
  registry.set("task", terminal);
  registry.dispose("task", () => undefined);

  assert.equal(registry.size, 0);
});

test("terminal registry never restores a programmatically disposed terminal before its close event", () => {
  const registry = new TerminalRegistry<object>();
  const terminal = {};
  let disposeCount = 0;
  registry.set("task", terminal);

  registry.dispose("task", () => { disposeCount += 1; });

  assert.equal(registry.isClosed(terminal), true);
  assert.equal(registry.set("task", terminal), false);
  assert.equal(registry.get("task"), undefined);
  assert.equal(disposeCount, 1);
  assert.deepEqual(registry.handleClose(terminal, 4), { kind: "suppressed" });
  assert.equal(registry.isClosed(terminal), true);
  assert.equal(registry.set("task", terminal), false);
});

test("terminal registry disposes a duplicate restored terminal without replacing the live mapping", () => {
  const registry = new TerminalRegistry<object>();
  const existing = {};
  const duplicate = {};
  const disposed: object[] = [];
  registry.set("task", existing);

  assert.equal(registry.setOrDisposeDuplicate("task", duplicate, (terminal) => disposed.push(terminal)), false);
  assert.equal(registry.get("task"), existing);
  assert.deepEqual(disposed, [duplicate]);
  assert.equal(registry.consumeSuppressedClose(duplicate), true);
});

test("terminal registry preserves a close that races ahead of asynchronous registration", () => {
  const registry = new TerminalRegistry<object>();
  const terminal = {};

  assert.deepEqual(registry.handleClose(terminal, 1), { kind: "pending" });
  assert.deepEqual(
    registry.registerAfterLookup("task", terminal, () => assert.fail("closed terminal must not be disposed again")),
    { kind: "closed", reason: 1, publish: true },
  );
  assert.deepEqual(
    registry.registerAfterLookup("task", terminal, () => assert.fail("closed terminal must not be disposed again")),
    { kind: "closed", reason: 1, publish: false },
  );
  assert.equal(registry.get("task"), undefined);
  assert.equal(registry.size, 0);
});

test("terminal registry never re-registers a tracked terminal after it closes", () => {
  const registry = new TerminalRegistry<object>();
  const terminal = {};
  registry.set("task", terminal);

  assert.deepEqual(registry.handleClose(terminal, 3), { kind: "tracked", id: "task", reason: 3 });
  assert.deepEqual(
    registry.registerAfterLookup("task", terminal, () => assert.fail("closed terminal must not be disposed again")),
    { kind: "closed", reason: 3, publish: false },
  );
  assert.deepEqual(
    registry.registerAfterLookup("task", terminal, () => assert.fail("closed terminal must not be disposed again")),
    { kind: "closed", reason: 3, publish: false },
  );
  assert.equal(registry.get("task"), undefined);
  assert.equal(registry.isClosed(terminal), true);
});

test("terminal registry disposes the same duplicate at most once while close delivery is pending", () => {
  const registry = new TerminalRegistry<object>();
  const existing = {};
  const duplicate = {};
  let disposeCount = 0;
  registry.set("task", existing);

  assert.deepEqual(
    registry.registerAfterLookup("task", duplicate, () => { disposeCount += 1; }),
    { kind: "duplicate" },
  );
  assert.deepEqual(
    registry.registerAfterLookup("task", duplicate, () => { disposeCount += 1; }),
    { kind: "duplicate" },
  );
  assert.equal(disposeCount, 1);
  assert.deepEqual(registry.handleClose(duplicate, 4), { kind: "suppressed" });
  assert.deepEqual(
    registry.registerAfterLookup("task", duplicate, () => { disposeCount += 1; }),
    { kind: "closed", reason: 4, publish: false },
  );
  assert.equal(disposeCount, 1);
});

test("terminal registry returns and removes a tracked terminal on close", () => {
  const registry = new TerminalRegistry<object>();
  const terminal = {};
  registry.set("task", terminal);

  assert.deepEqual(registry.handleClose(terminal, 2), { kind: "tracked", id: "task", reason: 2 });
  assert.equal(registry.get("task"), undefined);
});

test("only an explicit user terminal close publishes shared visibility", () => {
  const userReason = 1;
  assert.equal(shouldPublishTerminalClose(userReason, userReason), true);
  assert.equal(shouldPublishTerminalClose(2, userReason), false, "shutdown must stay browser-local");
  assert.equal(shouldPublishTerminalClose(3, userReason), false, "process exit is not user intent");
  assert.equal(shouldPublishTerminalClose(4, userReason), false, "extension disposal is not user intent");
  assert.equal(shouldPublishTerminalClose(undefined, userReason), false, "unknown close is not user intent");
});

test("only an interactive open reports a task that becomes unavailable during lookup", () => {
  assert.equal(shouldReportUnavailableTerminal(true), true);
  assert.equal(shouldReportUnavailableTerminal(false), false, "background synchronization must converge quietly");
});

test("context-only commands reject invocation without a selected task", () => {
  const task = { id: "task" };
  assert.equal(requireCommandArgument(task), task);
  assert.throws(() => requireCommandArgument(undefined), /请从共享终端任务列表选择任务/);
});
