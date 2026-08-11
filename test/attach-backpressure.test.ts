import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const brokerScript = join(process.cwd(), "scripts", "pty_broker.py");
const mebibyte = 1024 * 1024;

function frame(type: string, payload: Buffer = Buffer.alloc(0)): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt8(type.charCodeAt(0), 0);
  header.writeUInt32BE(payload.length, 1);
  return Buffer.concat([header, payload]);
}

async function withTimeout<T>(promise: Promise<T>, label: string, timeoutMs = 10_000): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function sendFrames(socket: Socket, payloads: readonly Buffer[]): Promise<void> {
  for (const payload of payloads) {
    if (!socket.write(frame("O", payload))) {
      await withTimeout(once(socket, "drain").then(() => undefined), "fake broker output drain");
    }
  }
  await withTimeout(new Promise<void>((resolve, reject) => {
    if (socket.writableLength === 0) {
      resolve();
      return;
    }
    once(socket, "drain").then(() => resolve(), reject);
  }), "fake broker writable queue drain");
}

interface AttachResult {
  outputBytes: number;
  outputTail: string;
  sawTruncationWarning: boolean;
  pausedRssBytes: number;
  stderr: string;
  exitCode: number | null;
}

async function exerciseBackpressuredAttach(
  maxOutputBytes: number,
  payloads: readonly Buffer[],
): Promise<AttachResult> {
  const dir = await mkdtemp(join(tmpdir(), "shared-terminal-attach-backpressure-"));
  const socketPath = join(dir, "broker.sock");
  const server = createServer();
  let accepted: Socket | undefined;
  let child: ReturnType<typeof spawn> | undefined;
  try {
    const connection = new Promise<Socket>((resolve, reject) => {
      server.once("connection", (socket) => {
        accepted = socket;
        socket.once("error", reject);
        let handshakeBytes = 0;
        socket.on("data", (chunk) => {
          handshakeBytes += chunk.length;
          if (handshakeBytes >= 14) {
            resolve(socket);
          }
        });
      });
      server.once("error", reject);
    });
    await new Promise<void>((resolve, reject) => {
      server.listen(socketPath, resolve);
      server.once("error", reject);
    });

    child = spawn("python3", [
      brokerScript,
      "attach",
      "--socket", socketPath,
      "--max-output-bytes", String(maxOutputBytes),
    ], { stdio: ["pipe", "pipe", "pipe"] });
    let stderr = "";
    child.stderr!.on("data", (chunk) => { stderr += chunk.toString(); });

    const socket = await withTimeout(connection, "attach protocol handshake");
    await sendFrames(socket, payloads);
    const processStatus = await readFile(`/proc/${child.pid}/status`, "utf8");
    const rssKiB = Number(/^VmRSS:\s+(\d+)\s+kB$/m.exec(processStatus)?.[1]);
    assert.ok(Number.isFinite(rssKiB), processStatus);
    const pausedRssBytes = rssKiB * 1024;

    let outputBytes = 0;
    let outputTail = "";
    let outputScan = "";
    let sawTruncationWarning = false;
    child.stdout!.on("data", (chunk: Buffer) => {
      outputBytes += chunk.length;
      outputTail = (outputTail + chunk.toString("utf8")).slice(-4096);
      outputScan += chunk.toString("utf8");
      sawTruncationWarning ||= /共享终端.*丢弃.*历史/.test(outputScan);
      outputScan = outputScan.slice(-512);
    });
    socket.end();
    const exitCode = await withTimeout(
      new Promise<number | null>((resolve, reject) => {
        child!.once("exit", resolve);
        child!.once("error", reject);
      }),
      "attach output flush and exit",
    );
    return { outputBytes, outputTail, sawTruncationWarning, pausedRssBytes, stderr, exitCode };
  } finally {
    accepted?.destroy();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
    }
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(dir, { recursive: true, force: true });
  }
}

test("attach keeps draining broker output while its native PTY stdout is backpressured", async () => {
  const payloads = Array.from({ length: 32 }, (_unused, index) => {
    const payload = Buffer.alloc(mebibyte, 0x78);
    if (index === 0) {
      payload.write("__BACKPRESSURE_BEGIN__", 0, "utf8");
    }
    if (index === 31) {
      payload.write("__BACKPRESSURE_END__", payload.length - 20, "utf8");
    }
    return payload;
  });

  const result = await exerciseBackpressuredAttach(100 * mebibyte, payloads);

  assert.equal(result.exitCode, 0);
  assert.equal(result.stderr, "");
  assert.equal(result.outputBytes, 32 * mebibyte);
  assert.match(result.outputTail, /__BACKPRESSURE_END__/);
});

test("attach bounds paused output and keeps the latest terminal state visible after overflow", async () => {
  const payloads = Array.from({ length: 8 }, (_unused, index) => {
    const payload = Buffer.alloc(mebibyte, 0x79);
    if (index === 7) {
      payload.write("__LATEST_TERMINAL_STATE__", payload.length - 25, "utf8");
    }
    return payload;
  });

  const result = await exerciseBackpressuredAttach(2 * mebibyte, payloads);

  assert.equal(result.exitCode, 0);
  assert.equal(result.stderr, "");
  assert.ok(result.outputBytes <= 2 * mebibyte, JSON.stringify(result));
  assert.ok(result.pausedRssBytes <= 64 * mebibyte, JSON.stringify(result));
  assert.equal(result.sawTruncationWarning, true);
  assert.match(result.outputTail, /__LATEST_TERMINAL_STATE__/);
});
