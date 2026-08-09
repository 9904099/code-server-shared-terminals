import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { processStartTicks, ProcessRunner, StartedProcess, TaskStore, writeFileAtomically } from "../src/task-store";

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

function describeError(error: unknown): string {
  if (error instanceof AggregateError) {
    return `${error.name}: ${error.message}\n${error.errors.map(describeError).join("\n")}`;
  }
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

test("processStartTicks treats a zombie process as exited", async () => {
  const parent = spawn("/usr/bin/python3", ["-c", [
    "import os, sys, time",
    "child = os.fork()",
    "if child == 0:",
    "    os._exit(0)",
    "deadline = time.monotonic() + 5",
    "while time.monotonic() < deadline:",
    "    with open(f'/proc/{child}/stat', encoding='utf-8') as stat_file:",
    "        stat_line = stat_file.read()",
    "    if stat_line[stat_line.rfind(')') + 2:].split()[0] == 'Z':",
    "        break",
    "    time.sleep(0.01)",
    "else:",
    "    raise RuntimeError('child did not become a zombie')",
    "print(child, flush=True)",
    "sys.stdin.readline()",
    "os.waitpid(child, 0)",
  ].join("\n")], { stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  parent.stderr!.setEncoding("utf8");
  parent.stderr!.on("data", (chunk: string) => { stderr += chunk; });

  try {
    const zombiePid = await new Promise<number>((resolve, reject) => {
      let stdout = "";
      parent.stdout!.setEncoding("utf8");
      parent.stdout!.on("data", (chunk: string) => {
        stdout += chunk;
        const lineEnd = stdout.indexOf("\n");
        if (lineEnd >= 0) {
          resolve(Number(stdout.slice(0, lineEnd)));
        }
      });
      parent.once("error", reject);
      parent.once("exit", (code) => reject(new Error(`zombie parent exited early (code=${code}): ${stderr}`)));
    });
    assert.ok(Number.isInteger(zombiePid) && zombiePid > 0, stderr);
    assert.equal(await processStartTicks(zombiePid), undefined);
  } finally {
    parent.stdin!.end();
    if (parent.exitCode === null) {
      await new Promise<void>((resolve) => parent.once("exit", () => resolve()));
    }
  }
});

test("TaskStore starts a real broker and the packaged attach client reaches its shell", async () => {
  const dir = await mkdtemp(join(tmpdir(), "task-store-integration-"));
  const script = join(process.cwd(), "scripts", "pty_broker.py");
  const store = new TaskStore(join(dir, "tasks.json"), new ProcessRunner(), {
    tmuxPath: "tmux",
    socketName: "legacy-test",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: script,
    socketDirectory: join(dir, "sockets"),
    shellPath: "/bin/sh",
    requireCgroup: false,
    environment: {
      HOME: dir,
      USER: process.env.USER || "tester",
      LOGNAME: process.env.LOGNAME || process.env.USER || "tester",
      PATH: process.env.PATH || "/usr/bin:/bin",
      SHELL: "/bin/sh",
    },
  });
  let taskId = "";
  let terminal: ReturnType<typeof spawn> | undefined;

  try {
    const task = await store.create("集成测试", dir);
    taskId = task.id;
    assert.equal((await store.listWithStatus())[0].alive, true);

    terminal = spawn("/usr/bin/python3", [
      script, "attach", "--socket", store.brokerSocketPath(task),
    ], { stdio: ["pipe", "pipe", "pipe"] });
    let output = "";
    let errors = "";
    terminal.stdout!.on("data", (chunk) => { output += chunk.toString(); });
    terminal.stderr!.on("data", (chunk) => { errors += chunk.toString(); });
    terminal.stdin!.write("printf '\\n__PACKAGED_ATTACH_OK__\\n'\n");
    await waitFor(() => output.includes("\r\n__PACKAGED_ATTACH_OK__\r\n"), `attach output (${errors})`);

    await store.delete(task.id);
    taskId = "";
    const exitCode = terminal.exitCode ?? await new Promise<number | null>((resolve) => terminal!.once("exit", resolve));
    assert.equal(exitCode, 0, errors);
    assert.deepEqual(await store.list(), []);
  } finally {
    if (taskId) {
      await store.delete(taskId);
    }
    if (terminal && terminal.exitCode === null) {
      terminal.kill("SIGKILL");
    }
    await rm(dir, { recursive: true, force: true });
  }
});

test("broker startup gate does not release a shell before PID identity is durable", async () => {
  const dir = await mkdtemp(join(tmpdir(), "ts-startup-gate-"));
  const script = join(process.cwd(), "scripts", "pty_broker.py");
  const shellScript = join(dir, "must-not-start.sh");
  const shellMarker = join(dir, "shell-started");
  const socketDirectory = join(dir, "sockets");
  let writes = 0;
  await writeFile(shellScript, [
    "#!/bin/sh",
    `printf started > ${JSON.stringify(shellMarker)}`,
    "while :; do sleep 1; done",
    "",
  ].join("\n"));
  await chmod(shellScript, 0o700);
  const store = new TaskStore(join(dir, "tasks.json"), new ProcessRunner(), {
    tmuxPath: "tmux",
    socketName: "legacy-test",
    pythonPath: "/usr/bin/python3",
    brokerScriptPath: script,
    socketDirectory,
    shellPath: shellScript,
    requireCgroup: false,
    environment: {
      HOME: dir,
      USER: process.env.USER || "tester",
      LOGNAME: process.env.LOGNAME || process.env.USER || "tester",
      PATH: process.env.PATH || "/usr/bin:/bin",
      SHELL: shellScript,
    },
  }, undefined, {
    registryWriter: async (path, content) => {
      writes += 1;
      if (writes === 2) {
        throw new Error("injected PID publication failure");
      }
      await writeFileAtomically(path, content);
    },
  });

  try {
    await assert.rejects(() => store.create("启动门", dir), /PID publication failure/);
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(existsSync(shellMarker), false, "shell started before PID identity became durable");
    const artifacts = existsSync(socketDirectory) ? await readdir(socketDirectory) : [];
    assert.deepEqual(
      artifacts.filter((name) => name.endsWith(".sock") || name.endsWith(".ctl")),
      [],
      "broker created sockets before PID identity became durable",
    );
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("registry publication failure uses authenticated broker stop and leaves no process or socket", async () => {
  const dir = await mkdtemp(join(tmpdir(), "ts-rollback-"));
  const sourceScript = join(process.cwd(), "scripts", "pty_broker.py");
  const wrapperScript = join(dir, "signal-resistant-broker.py");
  const shellScript = join(dir, "cleanup-shell.sh");
  const shellPidFile = join(dir, "shell.pid");
  const descendantPidFile = join(dir, "descendant.pid");
  const socketDirectory = join(dir, "sockets");
  const registryPath = join(dir, "tasks.json");
  let started: StartedProcess | undefined;
  let shellPid = 0;
  let descendantPid = 0;
  let registryWrites = 0;

  class RecordingRunner extends ProcessRunner {
    override async start(
      command: string,
      args: string[],
      options: { cwd: string; env: NodeJS.ProcessEnv },
    ): Promise<StartedProcess> {
      started = await super.start(command, args, options);
      return started;
    }
  }

  try {
    await writeFile(wrapperScript, [
      "import importlib.util, sys",
      `spec = importlib.util.spec_from_file_location('broker_impl', ${JSON.stringify(sourceScript)})`,
      "module = importlib.util.module_from_spec(spec)",
      "sys.modules[spec.name] = module",
      "spec.loader.exec_module(module)",
      "module.Broker._request_stop = lambda self, signum, frame: None",
      "raise SystemExit(module.main())",
      "",
    ].join("\n"));
    await writeFile(shellScript, [
      "#!/bin/sh",
      "trap '' HUP TERM",
      "printf '%s' \"$$\" > \"$SHELL_PID_FILE\"",
      "setsid /bin/sh -c 'trap \"\" HUP TERM; printf \"%s\" \"$$\" > \"$DESCENDANT_PID_FILE\"; while :; do sleep 1; done' &",
      "while :; do sleep 1; done",
      "",
    ].join("\n"));
    await chmod(shellScript, 0o700);

    const store = new TaskStore(registryPath, new RecordingRunner(), {
      tmuxPath: "tmux",
      socketName: "legacy-test",
      pythonPath: "/usr/bin/python3",
      brokerScriptPath: wrapperScript,
      socketDirectory,
      shellPath: shellScript,
      requireCgroup: false,
      environment: {
        HOME: dir,
        USER: process.env.USER || "tester",
        LOGNAME: process.env.LOGNAME || process.env.USER || "tester",
        PATH: process.env.PATH || "/usr/bin:/bin",
        SHELL: shellScript,
        SHELL_PID_FILE: shellPidFile,
        DESCENDANT_PID_FILE: descendantPidFile,
      },
    }, undefined, {
      registryWriter: async (path, content) => {
        registryWrites += 1;
        if (registryWrites === 3) {
          await waitFor(
            () => existsSync(shellPidFile) && existsSync(descendantPidFile),
            "cleanup test shell and descendant PID files",
          );
          throw new Error("injected registry publication failure");
        }
        await writeFileAtomically(path, content);
      },
    });

    const creationError = await store.create("发布失败回滚", dir).then(
      () => undefined,
      (error: unknown) => error,
    );
    const creationErrorEvidence = describeError(creationError);
    assert.equal(creationError instanceof AggregateError, false, creationErrorEvidence);
    assert.match(creationErrorEvidence, /injected registry publication failure/i);
    const evidenceFiles = await readdir(dir);
    assert.ok(evidenceFiles.includes("shell.pid"), JSON.stringify({ creationError: String(creationError), evidenceFiles }));
    assert.ok(evidenceFiles.includes("descendant.pid"), JSON.stringify({ creationError: String(creationError), evidenceFiles }));
    shellPid = Number(await readFile(shellPidFile, "utf8"));
    descendantPid = Number(await readFile(descendantPidFile, "utf8"));
    assert.equal(await processStartTicks(started!.pid), undefined, "broker survived registry rollback");
    assert.equal(await processStartTicks(shellPid), undefined, "shell survived registry rollback");
    assert.equal(await processStartTicks(descendantPid), undefined, "independent session descendant survived registry rollback");
    const socketArtifacts = (await readdir(socketDirectory)).filter(
      (name) => name.endsWith(".sock") || name.endsWith(".ctl") || name.includes(".remove-"),
    );
    assert.deepEqual(socketArtifacts, [], "registry rollback left Unix sockets");
  } finally {
    for (const pid of [descendantPid, shellPid, started?.pid ?? 0]) {
      if (pid > 0 && await processStartTicks(pid) !== undefined) {
        process.kill(pid, "SIGKILL");
      }
    }
    await rm(dir, { recursive: true, force: true });
  }
});
