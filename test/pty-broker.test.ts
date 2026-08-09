import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createConnection, Socket } from "node:net";
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

class ProtocolClient {
  private buffer = Buffer.alloc(0);
  private output = Buffer.alloc(0);
  private readonly frames: string[] = [];
  private readonly payloads = new Map<string, Buffer[]>();

  private constructor(readonly socket: Socket) {
    socket.on("error", () => undefined);
    socket.on("data", (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      while (this.buffer.length >= 5) {
        const size = this.buffer.readUInt32BE(1);
        if (this.buffer.length < 5 + size) {
          break;
        }
        const type = String.fromCharCode(this.buffer.readUInt8(0));
        const payload = this.buffer.subarray(5, 5 + size);
        this.buffer = this.buffer.subarray(5 + size);
        this.frames.push(type);
        this.payloads.set(type, [...(this.payloads.get(type) ?? []), Buffer.from(payload)]);
        if (type === "O") {
          this.output = Buffer.concat([this.output, payload]);
        }
      }
    });
  }

  static connect(socketPath: string): Promise<ProtocolClient> {
    return new Promise((resolve, reject) => {
      const socket = createConnection(socketPath);
      socket.once("error", reject);
      socket.once("connect", () => {
        socket.off("error", reject);
        resolve(new ProtocolClient(socket));
      });
    });
  }

  attach(rows = 24, columns = 80): void {
    this.socket.write(frame("A"));
    this.resize(rows, columns);
  }

  input(text: string): void {
    this.socket.write(frame("I", Buffer.from(text)));
  }

  resize(rows: number, columns: number): void {
    const payload = Buffer.alloc(4);
    payload.writeUInt16BE(rows, 0);
    payload.writeUInt16BE(columns, 2);
    this.socket.write(frame("R", payload));
  }

  control(type: "P" | "K" | "S", payload: Buffer = Buffer.alloc(0)): void {
    this.socket.write(frame(type, payload));
  }

  text(): string {
    return this.output.toString("utf8");
  }

  hasFrame(type: string): boolean {
    return this.frames.includes(type);
  }

  framesWithPayload(type: string): Buffer[] {
    return this.payloads.get(type) ?? [];
  }

  close(): void {
    this.socket.destroy();
  }

  closed(): boolean {
    return this.socket.destroyed;
  }
}

async function waitFor(check: () => boolean, label: string, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`timed out waiting for ${label}`);
}

async function connectWhenReady(socketPath: string): Promise<ProtocolClient> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try {
      return await ProtocolClient.connect(socketPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" && (error as NodeJS.ErrnoException).code !== "ECONNREFUSED") {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  assert.fail("broker socket did not become ready");
}

test("PTY broker broadcasts one shell to two clients and arbitrates terminal size by active input", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-test-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const broker = spawn("python3", [
    brokerScript,
    "serve",
    "--socket", socketPath,
    "--control-socket", controlSocketPath,
    "--cwd", dir,
    "--shell", "/bin/sh",
    "--replay-bytes", "65536",
  ], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  broker.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

  const clients: ProtocolClient[] = [];
  try {
    const first = await connectWhenReady(socketPath);
    const second = await ProtocolClient.connect(socketPath);
    clients.push(first, second);
    first.attach(31, 101);
    second.attach(40, 120);

    first.input("printf '\\n__FROM_FIRST__\\n'\n");
    await waitFor(() => first.text().includes("\r\n__FROM_FIRST__\r\n"), "first client output");
    await waitFor(() => second.text().includes("\r\n__FROM_FIRST__\r\n"), "broadcast to second client");

    first.input("stty size; printf '__SIZE_FIRST_DONE__\\n'\n");
    await waitFor(() => first.text().includes("31 101") && first.text().includes("__SIZE_FIRST_DONE__"), "first client size");

    second.input(":\n");
    second.input("stty size; printf '__SIZE_SECOND_DONE__\\n'\n");
    await waitFor(() => second.text().includes("40 120") && second.text().includes("__SIZE_SECOND_DONE__"), "active second client size");
    await waitFor(() => first.text().includes("40 120") && first.text().includes("__SIZE_SECOND_DONE__"), "second client output broadcast");

    const replay = await ProtocolClient.connect(socketPath);
    clients.push(replay);
    replay.attach();
    await waitFor(() => replay.text().includes("__FROM_FIRST__") && replay.text().includes("__SIZE_SECOND_DONE__"), "bounded replay on attach");

    const probe = await ProtocolClient.connect(controlSocketPath);
    clients.push(probe);
    probe.control("P");
    await waitFor(() => probe.hasFrame("P"), "probe response");

    const statusClient = await ProtocolClient.connect(controlSocketPath);
    clients.push(statusClient);
    statusClient.control("S");
    await waitFor(() => statusClient.hasFrame("S"), "status before identity-bound stop");
    const status = JSON.parse(statusClient.framesWithPayload("S")[0].toString("utf8"));
    assert.match(status.controlSocketDev, /^\d+$/);
    assert.match(status.controlSocketIno, /^\d+$/);
    assert.equal(status.processBoundary, "subreaper");

    const stopper = await ProtocolClient.connect(controlSocketPath);
    clients.push(stopper);
    stopper.control("K", Buffer.from(JSON.stringify({
      taskId: status.taskId,
      brokerPid: status.brokerPid,
      brokerStartTicks: status.brokerStartTicks,
      instanceNonce: status.instanceNonce,
      controlSocketDev: status.controlSocketDev,
      controlSocketIno: status.controlSocketIno,
    })));
    await waitFor(() => stopper.hasFrame("K"), "identity-bound stop acknowledgement");
    const acknowledgement = JSON.parse(stopper.framesWithPayload("K")[0].toString("utf8"));
    assert.deepEqual(acknowledgement, {
      taskId: status.taskId,
      brokerPid: status.brokerPid,
      brokerStartTicks: status.brokerStartTicks,
      instanceNonce: status.instanceNonce,
      controlSocketDev: status.controlSocketDev,
      controlSocketIno: status.controlSocketIno,
    });
    const exitCode = await new Promise<number | null>((resolve) => broker.once("exit", resolve));
    assert.equal(exitCode, 0, stderr);
  } finally {
    for (const client of clients) {
      client.close();
    }
    if (broker.exitCode === null) {
      broker.kill("SIGKILL");
    }
    await rm(dir, { recursive: true, force: true });
  }
});

test("PTY broker bounds clients and partial protocol input without losing liveness", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-limits-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const broker = spawn("python3", [
    brokerScript,
    "serve",
    "--socket", socketPath,
    "--control-socket", controlSocketPath,
    "--cwd", dir,
    "--shell", "/bin/sh",
    "--task-id", "limits-test",
    "--max-clients", "1",
    "--max-client-input-bytes", "64",
  ], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  broker.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
  const clients: ProtocolClient[] = [];
  try {
    const first = await connectWhenReady(socketPath);
    clients.push(first);
    first.attach();

    const probeAtCapacity = await ProtocolClient.connect(controlSocketPath);
    clients.push(probeAtCapacity);
    probeAtCapacity.control("P");
    await waitFor(() => probeAtCapacity.hasFrame("P"), "probe at attached-client capacity");

    const rejected = await ProtocolClient.connect(socketPath);
    clients.push(rejected);
    rejected.attach();
    await waitFor(() => rejected.closed(), "client above max-clients to close");

    first.input("printf '__FIRST_STILL_ALIVE__\\n'\n");
    await waitFor(() => first.text().includes("__FIRST_STILL_ALIVE__"), "existing client after rejection");

    first.close();
    const oversized = await connectWhenReady(socketPath);
    clients.push(oversized);
    const partial = Buffer.alloc(70);
    partial.writeUInt8("I".charCodeAt(0), 0);
    partial.writeUInt32BE(1024, 1);
    oversized.socket.write(partial);
    await waitFor(() => oversized.closed(), "oversized partial frame buffer to close");

    const probe = await connectWhenReady(controlSocketPath);
    clients.push(probe);
    probe.control("P");
    await waitFor(() => probe.hasFrame("P"), "probe after malformed client");
  } finally {
    for (const client of clients) {
      client.close();
    }
    if (broker.exitCode === null) {
      broker.kill("SIGTERM");
      await new Promise((resolve) => broker.once("exit", resolve));
    }
    await rm(dir, { recursive: true, force: true });
  }
  assert.equal(stderr, "");
});

test("PTY broker exposes bounded non-sensitive status and rejects oversized PTY input", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pty-broker-status-"));
  const socketPath = join(dir, "broker.sock");
  const controlSocketPath = join(dir, "broker.control.sock");
  const broker = spawn("python3", [
    brokerScript,
    "serve",
    "--socket", socketPath,
    "--control-socket", controlSocketPath,
    "--cwd", dir,
    "--shell", "/bin/sh",
    "--task-id", "status-test",
    "--max-pty-input-bytes", "1024",
  ], { stdio: ["ignore", "pipe", "pipe"] });
  const clients: ProtocolClient[] = [];
  try {
    const statusClient = await connectWhenReady(controlSocketPath);
    clients.push(statusClient);
    statusClient.control("S");
    await waitFor(() => statusClient.hasFrame("S"), "status response");
    const status = JSON.parse(statusClient.framesWithPayload("S")[0].toString("utf8")) as Record<string, unknown>;
    assert.equal(status.taskId, "status-test");
    assert.equal(status.version, "0.3.0");
    assert.equal(status.threads, 1);
    assert.equal(status.processBoundary, "subreaper");
    assert.equal(typeof status.rssBytes, "number");
    assert.equal(typeof status.fdCount, "number");
    assert.equal("cwd" in status, false);
    assert.equal("environment" in status, false);
    assert.equal("replay" in status, false);

    const wrongStop = await connectWhenReady(controlSocketPath);
    clients.push(wrongStop);
    wrongStop.control("K", Buffer.from(JSON.stringify({
      taskId: status.taskId,
      brokerPid: status.brokerPid,
      brokerStartTicks: status.brokerStartTicks,
      instanceNonce: "0".repeat(64),
      controlSocketDev: status.controlSocketDev,
      controlSocketIno: status.controlSocketIno,
    })));
    await waitFor(() => wrongStop.hasFrame("E"), "wrong stop identity rejection");

    const noisy = await connectWhenReady(socketPath);
    clients.push(noisy);
    noisy.attach();
    noisy.input("x".repeat(1025));
    await waitFor(() => noisy.closed(), "oversized PTY input producer to close");

    const probe = await connectWhenReady(controlSocketPath);
    clients.push(probe);
    probe.control("P");
    await waitFor(() => probe.hasFrame("P"), "probe after PTY input rejection");
  } finally {
    for (const client of clients) {
      client.close();
    }
    if (broker.exitCode === null) {
      broker.kill("SIGTERM");
      await new Promise((resolve) => broker.once("exit", resolve));
    }
    await rm(dir, { recursive: true, force: true });
  }
});
