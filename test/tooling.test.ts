import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

function runMissingIdentityHarness(script: string, args: string[], statusFunction: string): Record<string, unknown> {
  const harness = String.raw`
import importlib.util,json,sys,time
spec=importlib.util.spec_from_file_location("tool", ${JSON.stringify(script)})
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
spawned=[]
original_popen=module.subprocess.Popen
def capture_popen(*args,**kwargs):
    process=original_popen(*args,**kwargs)
    spawned.append(process)
    return process
module.subprocess.Popen=capture_popen
original_status=getattr(module,${JSON.stringify(statusFunction)})
def missing_identity(*args,**kwargs):
    current=original_status(*args,**kwargs)
    current.pop("taskId",None)
    return current
setattr(module,${JSON.stringify(statusFunction)},missing_identity)
sys.argv=[${JSON.stringify(script)},*${JSON.stringify(args)}]
outcome={"returnCode":None,"error":None,"survivorBeforeHarnessCleanup":None,"survivorAfterHarnessCleanup":None}
try:
    outcome["returnCode"]=module.main()
except BaseException as error:
    outcome["error"]=type(error).__name__
finally:
    process=spawned[0] if spawned else None
    time.sleep(0.2)
    outcome["survivorBeforeHarnessCleanup"]=process is not None and process.poll() is None
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except module.subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
    outcome["survivorAfterHarnessCleanup"]=process is not None and process.poll() is None
    print("__HARNESS__"+json.dumps(outcome,separators=(",",":")))
`;
  const result = spawnSync("python3", ["-c", harness], { encoding: "utf8", timeout: 30_000 });
  assert.equal(result.status, 0, JSON.stringify({ stdout: result.stdout, stderr: result.stderr }));
  const line = result.stdout.split("\n").find((candidate) => candidate.startsWith("__HARNESS__"));
  assert.ok(line, result.stdout);
  return JSON.parse(line.slice("__HARNESS__".length)) as Record<string, unknown>;
}

function runJson(script: string, args: string[], timeout: number): Record<string, unknown> {
  const result = spawnSync("python3", [script, ...args], {
    cwd: process.cwd(),
    encoding: "utf8",
    timeout,
  });
  assert.equal(result.status, 0, JSON.stringify({ stdout: result.stdout, stderr: result.stderr, signal: result.signal }));
  const lines = result.stdout.trim().split("\n").filter(Boolean);
  assert.equal(lines.length, 1, result.stdout);
  return JSON.parse(lines[0]) as Record<string, unknown>;
}

test("soak gate exercises hot I/O and requires identity-bound graceful stop", () => {
  const summary = runJson("scripts/soak_pty_broker.py", [
    "--duration", "1",
    "--interval", "0.1",
    "--churn-per-interval", "2",
    "--hot-output-bytes", "524288",
    "--hot-every", "2",
    "--max-control-latency-ms", "500",
    "--max-event-loop-lag-ms", "500",
  ], 30_000);

  assert.equal(summary.passed, true);
  assert.equal(summary.stopAcknowledged, true);
  assert.equal(summary.emergencyCleanup, false);
  assert.ok(Number(summary.hotOutputBursts) >= 1, JSON.stringify(summary));
  assert.ok(Number(summary.outputBytesDelta) >= 524288, JSON.stringify(summary));
  assert.ok(Number(summary.inputBytesDelta) > 0, JSON.stringify(summary));
  assert.ok(Number(summary.slowClientsDelta) >= 1, JSON.stringify(summary));
  assert.ok(Number(summary.controlLatencyMaxMs) <= 500, JSON.stringify(summary));
});

test("benchmark requires the six-field graceful stop acknowledgement", () => {
  const summary = runJson("scripts/benchmark_pty_broker.py", [
    "--samples", "10",
    "--bulk-bytes", "65536",
  ], 30_000);

  assert.equal(summary.stopAcknowledged, true);
  assert.equal(summary.emergencyCleanup, false);
});

test("soak cleans its exact broker when status identity is incomplete", () => {
  const outcome = runMissingIdentityHarness("scripts/soak_pty_broker.py", [
    "--duration", "1",
    "--interval", "0.1",
    "--churn-per-interval", "1",
    "--hot-output-bytes", "65536",
    "--hot-every", "2",
  ], "status_retry");
  assert.equal(outcome.survivorBeforeHarnessCleanup, false, JSON.stringify(outcome));
  assert.equal(outcome.survivorAfterHarnessCleanup, false, JSON.stringify(outcome));
  assert.ok(outcome.error !== null || Number(outcome.returnCode) !== 0, JSON.stringify(outcome));
});

test("benchmark cleans its exact broker when status identity is incomplete", () => {
  const outcome = runMissingIdentityHarness("scripts/benchmark_pty_broker.py", [
    "--samples", "10",
    "--bulk-bytes", "65536",
  ], "status");
  assert.equal(outcome.survivorBeforeHarnessCleanup, false, JSON.stringify(outcome));
  assert.equal(outcome.survivorAfterHarnessCleanup, false, JSON.stringify(outcome));
  assert.ok(outcome.error !== null || Number(outcome.returnCode) !== 0, JSON.stringify(outcome));
});
