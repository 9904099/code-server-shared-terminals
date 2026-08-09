import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { lstat, mkdtemp, readFile, rm } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const brokerScript = join(process.cwd(), "scripts", "pty_broker.py");

function frame(type: string, payload: Buffer = Buffer.alloc(0)): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt8(type.charCodeAt(0), 0);
  header.writeUInt32BE(payload.length, 1);
  return Buffer.concat([header, payload]);
}

async function waitFor(check: () => boolean | Promise<boolean>, label: string, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`timed out waiting for ${label}`);
}

async function status(socketPath: string): Promise<Record<string, number | string>> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    let incoming = Buffer.alloc(0);
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error("status timeout"));
    }, 2_000);
    socket.once("error", reject);
    socket.once("connect", () => socket.write(frame("S")));
    socket.on("data", (chunk) => {
      incoming = Buffer.concat([incoming, chunk]);
      if (incoming.length < 5) {
        return;
      }
      const size = incoming.readUInt32BE(1);
      if (incoming.length < 5 + size) {
        return;
      }
      clearTimeout(timeout);
      const type = String.fromCharCode(incoming.readUInt8(0));
      socket.destroy();
      assert.equal(type, "S");
      resolve(JSON.parse(incoming.subarray(5, 5 + size).toString("utf8")));
    });
  });
}

function stopFrame(current: Record<string, number | string>): Buffer {
  return frame("K", Buffer.from(JSON.stringify({
    taskId: current.taskId,
    brokerPid: current.brokerPid,
    brokerStartTicks: current.brokerStartTicks,
    instanceNonce: current.instanceNonce,
    controlSocketDev: current.controlSocketDev,
    controlSocketIno: current.controlSocketIno,
  })));
}

async function connectWhenReady(socketPath: string) {
  let socket: ReturnType<typeof createConnection> | undefined;
  await waitFor(async () => {
    socket = createConnection(socketPath);
    return new Promise<boolean>((resolve) => {
      socket!.once("connect", () => resolve(true));
      socket!.once("error", () => resolve(false));
    });
  }, "broker socket");
  return socket!;
}

async function processStartTicks(pid: number): Promise<number | undefined> {
  try {
    const statLine = await readFile(`/proc/${pid}/stat`, "utf8");
    const commandEnd = statLine.lastIndexOf(")");
    const fields = statLine.slice(commandEnd + 2).trim().split(/\s+/);
    const ticks = Number(fields[19]);
    return Number.isInteger(ticks) && ticks > 0 ? ticks : undefined;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

test("PTY broker returns to resource baseline after churn and malformed clients", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-resource-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const broker = spawn("python3", [
    brokerScript,
    "serve",
    "--socket", socketPath,
    "--control-socket", controlSocketPath,
    "--cwd", dir,
    "--shell", "/bin/sh",
    "--task-id", "resource-test",
    "--replay-bytes", "65536",
    "--max-clients", "4",
    "--max-client-input-bytes", "65536",
    "--max-client-output-bytes", "262144",
    "--max-pty-input-bytes", "65536",
  ], { stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  let gracefulStopTimedOut = false;
  broker.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

  try {
    await waitFor(async () => {
      try {
        return (await status(controlSocketPath)).taskId === "resource-test";
      } catch {
        return false;
      }
    }, "initial status");
    const baseline = await status(controlSocketPath);

    for (let index = 0; index < 100; index += 1) {
      const client = await connectWhenReady(socketPath);
      client.write(frame("A"));
      client.destroy();
    }
    for (let index = 0; index < 50; index += 1) {
      const malformed = await connectWhenReady(socketPath);
      const header = Buffer.alloc(5);
      header.writeUInt8("I".charCodeAt(0), 0);
      header.writeUInt32BE(2 * 1024 * 1024, 1);
      malformed.write(header);
      await new Promise((resolve) => malformed.once("close", resolve));
    }

    await waitFor(async () => Number((await status(controlSocketPath)).clients) === 0, "client count baseline");
    const after = await status(controlSocketPath);
    assert.equal(after.threads, 1);
    assert.ok(Number(after.fdCount) <= Number(baseline.fdCount) + 2, `${JSON.stringify({ baseline, after })}`);
    assert.ok(Number(after.rssBytes) <= Number(baseline.rssBytes) + 8 * 1024 * 1024, `${JSON.stringify({ baseline, after })}`);
    assert.ok(
      Number(after.disconnectedClients) + Number(after.rejectedClients) >= 150,
      JSON.stringify({ after }),
    );
    assert.equal(after.ptyInputBytes, 0);
    assert.ok(Number(after.queuedClientOutputBytes) <= 262144);

    const probe = await connectWhenReady(controlSocketPath);
    let probeReply = Buffer.alloc(0);
    probe.on("data", (chunk) => { probeReply = Buffer.concat([probeReply, chunk]); });
    probe.write(frame("P"));
    await waitFor(() => probeReply.length >= 5, "probe after churn");
    assert.equal(String.fromCharCode(probeReply[0]), "P");
    probe.destroy();
  } finally {
    if (broker.exitCode === null) {
      const current = await status(controlSocketPath).catch(() => undefined);
      const stopper = current ? await connectWhenReady(controlSocketPath).catch(() => undefined) : undefined;
      if (stopper && current) {
        stopper.write(stopFrame(current));
      }
      const stopped = await Promise.race([
        new Promise<boolean>((resolve) => broker.once("exit", () => resolve(true))),
        new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 2_000)),
      ]);
      gracefulStopTimedOut = !stopped;
    }
    if (broker.exitCode === null) {
      broker.kill("SIGKILL");
    }
    await rm(dir, { recursive: true, force: true });
  }
  assert.equal(gracefulStopTimedOut, false, "resource test required SIGKILL after graceful stop timeout");
  assert.equal(stderr, "");
});

test("PTY broker stop reaps descendants that escape into an independent session", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-descendants-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const childPidPath = join(dir, "escaped.pid");
  const broker = spawn("python3", [
    brokerScript,
    "serve",
    "--socket", socketPath,
    "--control-socket", controlSocketPath,
    "--cwd", dir,
    "--shell", "/bin/sh",
    "--task-id", "descendant-test",
  ], { stdio: ["ignore", "ignore", "pipe"] });
  let escapedPid = -1;
  let escapedStartTicks: number | undefined;

  try {
    const client = await connectWhenReady(socketPath);
    client.write(frame("A"));
    const code = [
      "import os,pathlib,signal,time",
      "signal.signal(signal.SIGHUP, signal.SIG_IGN)",
      "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
      `pathlib.Path(${JSON.stringify(childPidPath)}).write_text(str(os.getpid()))`,
      "time.sleep(300)",
    ].join(";");
    client.write(frame("I", Buffer.from(
      `setsid python3 -c '${code}' >/dev/null 2>&1 &\n`,
    )));
    await waitFor(async () => {
      try {
        escapedPid = Number((await readFile(childPidPath, "utf8")).trim());
        escapedStartTicks = await processStartTicks(escapedPid);
        return Number.isInteger(escapedPid) && escapedPid > 0 && escapedStartTicks !== undefined;
      } catch {
        return false;
      }
    }, "escaped descendant PID");

    const current = await status(controlSocketPath);
    const stopper = await connectWhenReady(controlSocketPath);
    stopper.write(stopFrame(current));
    const exitCode = await new Promise<number | null>((resolve) => broker.once("exit", resolve));
    assert.equal(exitCode, 0);
    await waitFor(
      async () => await processStartTicks(escapedPid) !== escapedStartTicks,
      "escaped descendant cleanup",
    );
    client.destroy();
    stopper.destroy();
  } finally {
    if (broker.exitCode === null) {
      broker.kill("SIGKILL");
    }
    if (escapedPid > 0 && await processStartTicks(escapedPid) === escapedStartTicks) {
      process.kill(escapedPid, "SIGKILL");
    }
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker cleans a published socket when shell initialization raises", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-init-failure-"));
  const socketPath = join(dir, "broker.sock");
  const harness = [
    "import importlib.util,sys",
    `spec=importlib.util.spec_from_file_location('broker', ${JSON.stringify(brokerScript)})`,
    "module=importlib.util.module_from_spec(spec)",
    "sys.modules[spec.name]=module",
    "spec.loader.exec_module(module)",
    `broker=module.Broker(${JSON.stringify(socketPath)}, ${JSON.stringify(dir)}, '/bin/sh', 'init-failure', 0, 1, 1024, 1024, 1024)`,
    "broker._start_shell=lambda: (_ for _ in ()).throw(RuntimeError('injected shell failure'))",
    "broker.run()",
  ].join(";");

  try {
    const failed = spawn("python3", ["-c", harness], { stdio: ["ignore", "ignore", "ignore"] });
    const exitCode = await new Promise<number | null>((resolve) => failed.once("exit", resolve));
    assert.notEqual(exitCode, 0);
    await assert.rejects(() => lstat(socketPath), { code: "ENOENT" });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker fails closed before starting a shell when required cgroup setup fails", () => {
  const harness = String.raw`
import importlib.util,json,sys
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
broker=module.Broker("/tmp/not-used.sock","/tmp","/bin/sh","cgroup-required",0,1,1024,1024,1024,None,True)
started=[]
broker._prepare_cgroup=lambda: (_ for _ in ()).throw(RuntimeError("no delegated cgroup"))
broker._start_shell=lambda: started.append(True)
broker._cleanup=lambda: None
error=None
try:
    broker.run()
except BaseException as caught:
    error=str(caught)
print(json.dumps({"error":error,"shellStarted":bool(started)}))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout.trim()), {
    error: "no delegated cgroup",
    shellStarted: false,
  });
});

test("PTY broker removes a bound socket when listener publication fails", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-listener-failure-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const childPidPath = join(dir, "child.pid");
  const harness = [
    "import importlib.util,pathlib,sys",
    `spec=importlib.util.spec_from_file_location('broker', ${JSON.stringify(brokerScript)})`,
    "module=importlib.util.module_from_spec(spec)",
    "sys.modules[spec.name]=module",
    "spec.loader.exec_module(module)",
    `broker=module.Broker(${JSON.stringify(socketPath)}, ${JSON.stringify(dir)}, '/bin/sh', 'listener-failure', 0, 1, 1024, 1024, 1024, ${JSON.stringify(controlSocketPath)})`,
    "original_chmod=module.os.chmod",
    `module.os.chmod=lambda path,mode: (pathlib.Path(${JSON.stringify(childPidPath)}).write_text(str(broker.child_pid)), (_ for _ in ()).throw(RuntimeError('injected chmod failure')))[1] if path == ${JSON.stringify(controlSocketPath)} else original_chmod(path,mode)`,
    "broker.run()",
  ].join(";");

  try {
    const failed = spawn("python3", ["-c", harness], { stdio: ["ignore", "ignore", "pipe"] });
    const exitCode = await new Promise<number | null>((resolve) => failed.once("exit", resolve));
    assert.notEqual(exitCode, 0);
    const childPid = Number((await readFile(childPidPath, "utf8")).trim());
    assert.equal(await processStartTicks(childPid), undefined, "listener failure left the shell process alive");
    await assert.rejects(() => lstat(socketPath), { code: "ENOENT" });
    await assert.rejects(() => lstat(controlSocketPath), { code: "ENOENT" });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker records a bound socket when the primary identity syscall fails", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-listener-identity-fallback-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const harness = [
    "import importlib.util,sys",
    `spec=importlib.util.spec_from_file_location('broker', ${JSON.stringify(brokerScript)})`,
    "module=importlib.util.module_from_spec(spec)",
    "sys.modules[spec.name]=module",
    "spec.loader.exec_module(module)",
    `broker=module.Broker(${JSON.stringify(socketPath)}, ${JSON.stringify(dir)}, '/bin/sh', 'identity-fallback', 0, 1, 1024, 1024, 1024, ${JSON.stringify(controlSocketPath)})`,
    "original_lstat=module.os.lstat",
    `module.os.lstat=lambda path: (_ for _ in ()).throw(OSError('injected lstat failure')) if path == ${JSON.stringify(controlSocketPath)} and module.os.path.exists(path) else original_lstat(path)`,
    "original_chmod=module.os.chmod",
    `module.os.chmod=lambda path,mode: (_ for _ in ()).throw(RuntimeError('injected post-identity failure')) if path == ${JSON.stringify(controlSocketPath)} else original_chmod(path,mode)`,
    "broker.run()",
  ].join(";");

  try {
    const failed = spawn("python3", ["-c", harness], { stdio: ["ignore", "ignore", "pipe"] });
    const exitCode = await new Promise<number | null>((resolve) => failed.once("exit", resolve));
    assert.notEqual(exitCode, 0);
    await assert.rejects(() => lstat(socketPath), { code: "ENOENT" });
    await assert.rejects(() => lstat(controlSocketPath), { code: "ENOENT" });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker control probe cannot be starved by idle data connections", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-control-capacity-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const broker = spawn("python3", [
    brokerScript,
    "serve",
    "--socket", socketPath,
    "--control-socket", controlSocketPath,
    "--cwd", dir,
    "--shell", "/bin/sh",
    "--max-clients", "1",
  ], { stdio: ["ignore", "ignore", "pipe"] });
  const clients: Array<Awaited<ReturnType<typeof connectWhenReady>>> = [];

  try {
    const attached = await connectWhenReady(socketPath);
    clients.push(attached);
    attached.write(frame("A"));
    for (let index = 0; index < 4; index += 1) {
      clients.push(await connectWhenReady(socketPath));
    }
    const probe = await connectWhenReady(controlSocketPath);
    clients.push(probe);
    let response = Buffer.alloc(0);
    probe.on("data", (chunk) => { response = Buffer.concat([response, chunk]); });
    probe.write(frame("P"));
    await waitFor(() => response.length >= 5, "control probe under idle connection pressure");
    assert.equal(String.fromCharCode(response[0]), "P");
  } finally {
    for (const client of clients) {
      client.destroy();
    }
    if (broker.exitCode === null) {
      broker.kill("SIGTERM");
      await new Promise((resolve) => broker.once("exit", resolve));
    }
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker control requests survive slowloris churn on the control socket", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-control-slowloris-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const broker = spawn("python3", [
    brokerScript, "serve", "--socket", socketPath, "--control-socket", controlSocketPath,
    "--cwd", dir, "--shell", "/bin/sh", "--task-id", "control-slowloris",
  ], { stdio: ["ignore", "ignore", "pipe"] });
  const idle: Array<ReturnType<typeof createConnection>> = [];
  let flooding = true;
  const flood = (async () => {
    while (flooding) {
      const client = createConnection(controlSocketPath);
      client.on("error", () => undefined);
      idle.push(client);
      if (idle.length > 64) {
        idle.shift()?.destroy();
      }
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
  })();

  try {
    await waitFor(async () => {
      try {
        return (await status(controlSocketPath)).taskId === "control-slowloris";
      } catch {
        return false;
      }
    }, "control slowloris broker readiness");
    for (let attempt = 0; attempt < 20; attempt += 1) {
      assert.equal((await status(controlSocketPath)).taskId, "control-slowloris");
    }
  } finally {
    flooding = false;
    await flood;
    for (const client of idle) {
      client.destroy();
    }
    if (broker.exitCode === null) {
      broker.kill("SIGTERM");
      await new Promise((resolve) => broker.once("exit", resolve));
    }
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker event-loop lag measures the timeout actually requested", () => {
  const harness = String.raw`
import importlib.util,json,sys,time
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
broker=module.Broker("/tmp/not-used.sock","/tmp","/bin/sh","lag-test",0,1,1024,1024,1024)
class FakeSelector:
    def select(self, timeout):
        time.sleep(0.03)
        broker.stopping=True
        return []
broker.selector=FakeSelector()
broker.pending_clients.add(object())
broker._enable_subreaper=lambda: None
broker._start_shell=lambda: None
broker._start_listeners=lambda: None
broker._cleanup=lambda: None
broker.run()
print(json.dumps({"lagMs":broker.max_event_loop_lag_ms}))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as { lagMs: number };
  assert.ok(outcome.lagMs >= 20, JSON.stringify(outcome));
});

test("PTY broker refuses to unlink a socket owned by a live broker", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-socket-owner-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const first = spawn("python3", [
    brokerScript, "serve", "--socket", socketPath, "--control-socket", controlSocketPath,
    "--cwd", dir, "--shell", "/bin/sh", "--task-id", "first",
  ], { stdio: ["ignore", "ignore", "pipe"] });
  let second: ReturnType<typeof spawn> | undefined;

  try {
    const firstClient = await connectWhenReady(socketPath);
    firstClient.destroy();
    second = spawn("python3", [
      brokerScript, "serve", "--socket", socketPath, "--control-socket", controlSocketPath,
      "--cwd", dir, "--shell", "/bin/sh", "--task-id", "second",
    ], { stdio: ["ignore", "ignore", "pipe"] });
    const secondExit = await Promise.race([
      new Promise<number | null>((resolve) => second!.once("exit", resolve)),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 1_000)),
    ]);
    assert.notEqual(secondExit, "timeout", "second broker replaced the live socket instead of failing closed");
    assert.notEqual(secondExit, 0);

    const statusAfter = await status(controlSocketPath);
    assert.equal(statusAfter.taskId, "first");
  } finally {
    if (second?.exitCode === null) {
      second.kill("SIGKILL");
    }
    if (first.exitCode === null) {
      first.kill("SIGTERM");
      await new Promise((resolve) => first.once("exit", resolve));
    }
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker preserves owned sockets when process cleanup is incomplete", () => {
  const harness = String.raw`
import importlib.util,json,os,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-incomplete-cleanup-") as directory:
    data_path=os.path.join(directory,"broker.sock")
    control_path=os.path.join(directory,"broker.control.sock")
    broker=module.Broker(data_path,directory,"/bin/sh","incomplete-cleanup",0,1,1024,1024,1024,control_socket_path=control_path)
    broker.data_listener=broker._bind_listener(data_path,"data-listener")
    broker.control_listener=broker._bind_listener(control_path,"control-listener")
    broker._terminate_owned_processes=lambda: False
    broker._cleanup()
    print(json.dumps({
        "cleanupOk":broker.cleanup_ok,
        "dataPathExists":os.path.lexists(data_path),
        "controlPathExists":os.path.lexists(control_path),
    },separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as Record<string, boolean>;
  assert.equal(outcome.cleanupOk, false, JSON.stringify(outcome));
  assert.equal(outcome.dataPathExists, true, JSON.stringify(outcome));
  assert.equal(outcome.controlPathExists, true, JSON.stringify(outcome));
});

test("PTY broker cleanup restores a replacement socket instead of unlinking it", () => {
  const harness = String.raw`
import importlib.util,json,os,socket,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-cleanup-race-") as directory:
    path=os.path.join(directory,"broker.sock")
    backup=os.path.join(directory,"owned.sock")
    broker=module.Broker(path,directory,"/bin/sh","cleanup-race",0,1,1024,1024,1024)
    owned=broker._bind_listener(path,"data-listener")
    broker.data_listener=owned
    original_info=broker._socket_path_info
    replacement=[]
    triggered=False
    def racing_info(target):
        global triggered
        info=original_info(target)
        if target == path and not triggered:
            triggered=True
            os.rename(path,backup)
            listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
            listener.bind(path)
            listener.listen(1)
            replacement.append(listener)
        return info
    broker._socket_path_info=racing_info
    broker._unlink_owned_socket(path)
    result={"replacementPathExists":os.path.exists(path),"ownedBackupExists":os.path.exists(backup)}
    for listener in replacement:
        listener.close()
    owned.close()
    print(json.dumps(result,separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as Record<string, boolean>;
  assert.equal(outcome.replacementPathExists, true, JSON.stringify(outcome));
  assert.equal(outcome.ownedBackupExists, true, JSON.stringify(outcome));
});

test("PTY broker cleanup preserves a replacement swapped after the final identity check", () => {
  const harness = String.raw`
import importlib.util,json,os,socket,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-cleanup-final-race-") as directory:
    path=os.path.join(directory,"broker.sock")
    backup=os.path.join(directory,"owned.sock")
    broker=module.Broker(path,directory,"/bin/sh","cleanup-final-race",0,1,1024,1024,1024)
    owned=broker._bind_listener(path,"data-listener")
    broker.data_listener=owned
    original_info=broker._socket_path_info
    replacement=[]
    reads=0
    def racing_info(target):
        global reads
        info=original_info(target)
        if target == path:
            reads += 1
            if reads == 2:
                os.rename(path,backup)
                listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
                listener.bind(path)
                listener.listen(1)
                replacement.append(listener)
        return info
    broker._socket_path_info=racing_info
    broker._unlink_owned_socket(path)
    replacement_exists=os.path.exists(path)
    owned_backup_exists=os.path.exists(backup)
    for listener in replacement:
        listener.close()
    owned.close()
    print(json.dumps({
        "replacementPathExists":replacement_exists,
        "ownedBackupExists":owned_backup_exists,
    },separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as Record<string, boolean>;
  assert.equal(outcome.replacementPathExists, true, JSON.stringify(outcome));
  assert.equal(outcome.ownedBackupExists, true, JSON.stringify(outcome));
});

test("PTY broker remnant recovery removes only registered socket identities after broker exit", () => {
  const harness = String.raw`
import contextlib,importlib.util,io,json,os,socket,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-recover-") as directory:
    data_path=os.path.join(directory,"data.sock")
    control_path=os.path.join(directory,"control.sock")
    data=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); data.bind(data_path); os.chmod(data_path,0o600)
    control=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); control.bind(control_path); os.chmod(control_path,0o600)
    data_info=os.lstat(data_path); control_info=os.lstat(control_path)
    data.close(); control.close()
    output=io.StringIO()
    with contextlib.redirect_stdout(output):
        code=module.run_recover(
            data_path,data_info.st_dev,data_info.st_ino,
            control_path,control_info.st_dev,control_info.st_ino,
            "recover-test",999999999,1,"",None,None,
        )
    acknowledgement=json.loads(output.getvalue())

    owned_path=os.path.join(directory,"owned.sock")
    replacement_path=os.path.join(directory,"replacement.sock")
    owned=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); owned.bind(owned_path); os.chmod(owned_path,0o600)
    owned_info=os.lstat(owned_path)
    os.rename(owned_path,replacement_path)
    replacement=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); replacement.bind(owned_path); os.chmod(owned_path,0o600)
    drift_refused=False
    try:
        module.run_recover(
            owned_path,owned_info.st_dev,owned_info.st_ino,
            control_path,control_info.st_dev,control_info.st_ino,
            "recover-test",999999999,1,"",None,None,
        )
    except RuntimeError:
        drift_refused=True

    live_path=os.path.join(directory,"live.sock")
    live=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); live.bind(live_path); os.chmod(live_path,0o600)
    live_info=os.lstat(live_path)
    live_refused=False
    try:
        module.run_recover(
            live_path,live_info.st_dev,live_info.st_ino,
            control_path,control_info.st_dev,control_info.st_ino,
            "recover-test",os.getpid(),module.Broker._process_start_ticks(),"",None,None,
        )
    except RuntimeError:
        live_refused=True
    result={
        "code":code,
        "dataRemoved":not os.path.lexists(data_path),
        "controlRemoved":not os.path.lexists(control_path),
        "ackTaskId":acknowledgement["taskId"],
        "driftRefused":drift_refused,
        "replacementPreserved":os.path.lexists(owned_path),
        "liveRefused":live_refused,
        "livePreserved":os.path.lexists(live_path),
    }
    replacement.close(); owned.close(); live.close()
    print(json.dumps(result,separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as Record<string, boolean | number | string>;
  assert.equal(outcome.code, 0, JSON.stringify(outcome));
  assert.equal(outcome.dataRemoved, true, JSON.stringify(outcome));
  assert.equal(outcome.controlRemoved, true, JSON.stringify(outcome));
  assert.equal(outcome.ackTaskId, "recover-test", JSON.stringify(outcome));
  assert.equal(outcome.driftRefused, true, JSON.stringify(outcome));
  assert.equal(outcome.replacementPreserved, true, JSON.stringify(outcome));
  assert.equal(outcome.liveRefused, true, JSON.stringify(outcome));
  assert.equal(outcome.livePreserved, true, JSON.stringify(outcome));
});

test("PTY broker accepts only one control frame per connection with a fixed small output queue", () => {
  const harness = String.raw`
import importlib.util,json,socket,sys
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
broker=module.Broker("/tmp/not-used.sock","/tmp","/bin/sh","control-budget",0,1,1024,8*1024*1024,1024)
left,right=socket.socketpair()
client=module.Client(left,output_buffer=module.ChunkQueue(8*1024*1024),control=True)
broker.clients.add(client)
broker._update_client_events=lambda _client: None
broker._status=lambda _client: {"payload":"x"*32768}
for _ in range(64):
    broker._handle_control_frame(client,b"S",b"")
print(json.dumps({
    "queued":client.output_buffer.size,
    "limit":module.MAX_CONTROL_OUTPUT_BYTES,
    "closeAfterFlush":client.close_after_flush,
},separators=(",",":")))
left.close(); right.close(); broker.selector.close()
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as { queued: number; limit: number; closeAfterFlush: boolean };
  assert.equal(outcome.limit, 64 * 1024, JSON.stringify(outcome));
  assert.ok(outcome.queued > 0 && outcome.queued <= outcome.limit, JSON.stringify(outcome));
  assert.equal(outcome.closeAfterFlush, true, JSON.stringify(outcome));
});

test("PTY broker captures and cleans a bound socket when lstat/stat path reads fail", () => {
  const harness = String.raw`
import importlib.util,json,os,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-opath-identity-") as directory:
    path=os.path.join(directory,"broker.sock")
    broker=module.Broker(path,directory,"/bin/sh","opath",0,1,1024,1024,1024)
    real_lstat=module.os.lstat
    real_stat=module.os.stat
    module.os.lstat=lambda *_args,**_kwargs: (_ for _ in ()).throw(OSError("injected lstat failure"))
    module.os.stat=lambda *_args,**_kwargs: (_ for _ in ()).throw(OSError("injected stat failure"))
    listener=None
    error=None
    try:
        listener=broker._bind_listener(path,"data-listener")
    except Exception as raised:
        error=type(raised).__name__
    finally:
        module.os.lstat=real_lstat
        module.os.stat=real_stat
    captured=path in broker.socket_identities
    if listener is not None:
        listener.close()
        broker._unlink_owned_socket(path)
    path_exists=os.path.lexists(path)
    print(json.dumps({"captured":captured,"pathExists":path_exists,"error":error},separators=(",",":")))
    if path_exists:
        os.unlink(path)
    broker.selector.close()
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as { captured: boolean; pathExists: boolean; error: string | null };
  assert.equal(outcome.error, null, JSON.stringify(outcome));
  assert.equal(outcome.captured, true, JSON.stringify(outcome));
  assert.equal(outcome.pathExists, false, JSON.stringify(outcome));
});

test("PTY broker stale-socket preparation fails closed without probing or moving the path", () => {
  const harness = String.raw`
import importlib.util,json,os,socket,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-prepare-race-") as directory:
    path=os.path.join(directory,"broker.sock")
    stale=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    stale.bind(path)
    stale.close()
    before=os.lstat(path)
    module.Broker._socket_is_live=staticmethod(lambda target: (_ for _ in ()).throw(AssertionError("probe called")))
    error=None
    try:
        module.Broker._prepare_socket_path(path)
    except Exception as raised:
        error=type(raised).__name__
    after=os.lstat(path)
    result={"pathExists":os.path.exists(path),"sameIdentity":(before.st_dev,before.st_ino)==(after.st_dev,after.st_ino),"error":error}
    print(json.dumps(result,separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as Record<string, boolean | string | null>;
  assert.equal(outcome.pathExists, true, JSON.stringify(outcome));
  assert.equal(outcome.sameIdentity, true, JSON.stringify(outcome));
  assert.equal(outcome.error, "RuntimeError", JSON.stringify(outcome));
});

test("PTY broker never removes or quarantines an existing stale socket path", () => {
  const harness = String.raw`
import importlib.util,json,os,socket,sys,tempfile
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix="pty-broker-stale-fail-closed-") as directory:
    path=os.path.join(directory,"broker.sock")
    stale=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    stale.bind(path)
    stale.close()
    before=os.lstat(path)
    error=None
    try:
        module.Broker._prepare_socket_path(path)
    except Exception as raised:
        error=type(raised).__name__
    after=os.lstat(path)
    quarantines=[name for name in os.listdir(directory) if ".remove-" in name]
    print(json.dumps({
        "error":error,
        "sameIdentity":(before.st_dev,before.st_ino)==(after.st_dev,after.st_ino),
        "quarantines":quarantines,
    },separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as {
    error: string | null;
    sameIdentity: boolean;
    quarantines: string[];
  };
  assert.equal(outcome.error, "RuntimeError", JSON.stringify(outcome));
  assert.equal(outcome.sameIdentity, true, JSON.stringify(outcome));
  assert.deepEqual(outcome.quarantines, [], JSON.stringify(outcome));
});

test("PTY broker event-loop lag includes callback processing time", () => {
  const harness = String.raw`
import importlib.util,json,selectors,sys,time,types
spec=importlib.util.spec_from_file_location("broker", ${JSON.stringify(brokerScript)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
broker=module.Broker("/tmp/not-used.sock","/tmp","/bin/sh","lag-callback-probe",0,1,1024,1024,1024)
class FakeSelector:
    def select(self, timeout):
        return [(types.SimpleNamespace(data="pty"), selectors.EVENT_READ)]
broker.selector=FakeSelector()
broker._enable_subreaper=lambda: None
broker._prepare_cgroup=lambda: None
broker._start_shell=lambda: None
broker._start_listeners=lambda: None
broker._reap_children=lambda: None
broker._cleanup=lambda: None
def slow_callback():
    time.sleep(0.04)
    broker.stopping=True
broker._read_pty=slow_callback
started=time.monotonic()
broker.run()
elapsed_ms=(time.monotonic()-started)*1000
print(json.dumps({"wallLoopMs":elapsed_ms,"reportedMaxEventLoopLagMs":broker.max_event_loop_lag_ms},separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const outcome = JSON.parse(result.stdout.trim()) as Record<string, number>;
  assert.ok(outcome.wallLoopMs >= 35, JSON.stringify(outcome));
  assert.ok(outcome.reportedMaxEventLoopLagMs >= 35, JSON.stringify(outcome));
});
