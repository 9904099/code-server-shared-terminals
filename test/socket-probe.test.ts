import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { probeBrokerSocket } from "../src/task-store";

test("broker liveness probe accepts a fragmented protocol response", async () => {
  const dir = await mkdtemp(join(tmpdir(), "broker-probe-test-"));
  const socketPath = join(dir, "probe.sock");
  const server = createServer((connection) => {
    connection.once("data", () => {
      connection.write(Buffer.from([0x50]));
      setTimeout(() => connection.end(Buffer.from([0, 0, 0, 0])), 10);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });

  try {
    assert.equal(await probeBrokerSocket(socketPath), true);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(dir, { recursive: true, force: true });
  }
});
