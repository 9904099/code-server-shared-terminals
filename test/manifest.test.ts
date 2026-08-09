import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";

test("extension manifest limits host execution settings in untrusted workspaces", async () => {
  const manifest = JSON.parse(await readFile(join(process.cwd(), "package.json"), "utf8"));

  assert.equal(manifest.name, "code-server-shared-terminals");
  assert.equal(manifest.displayName, "code-server-shared-terminals");
  assert.equal(manifest.publisher, "9904099");
  assert.equal(manifest.version, "0.3.0");
  assert.deepEqual(manifest.extensionKind, ["workspace"]);
  assert.equal(manifest.capabilities.untrustedWorkspaces.supported, "limited");
  assert.deepEqual(
    new Set(manifest.capabilities.untrustedWorkspaces.restrictedConfigurations),
    new Set([
      "sharedTerminals.registryPath",
      "sharedTerminals.defaultCwd",
      "sharedTerminals.tmuxPath",
      "sharedTerminals.socketName",
      "sharedTerminals.pythonPath",
      "sharedTerminals.socketDirectory",
      "sharedTerminals.shellPath",
      "sharedTerminals.environment",
      "sharedTerminals.maxTasks",
      "sharedTerminals.maxClientsPerTask",
      "sharedTerminals.replayBytes",
      "sharedTerminals.maxClientInputBytes",
      "sharedTerminals.maxClientOutputBytes",
      "sharedTerminals.maxPtyInputBytes",
    ]),
  );
  for (const key of manifest.capabilities.untrustedWorkspaces.restrictedConfigurations) {
    assert.equal(manifest.contributes.configuration.properties[key].scope, "machine");
  }
  assert.equal(manifest.contributes.configuration.properties["sharedTerminals.registryPath"].default, "");
  assert.equal(manifest.contributes.configuration.properties["sharedTerminals.defaultCwd"].default, "");
  assert.equal(manifest.contributes.configuration.properties["sharedTerminals.pythonPath"].default, "python3");
  assert.equal(manifest.contributes.configuration.properties["sharedTerminals.socketDirectory"].default, "");
  assert.deepEqual(manifest.contributes.terminal.profiles, [
    { id: "sharedTerminals.fast", title: "共享终端（快速）", icon: "terminal" },
  ]);
  assert.equal(
    Object.hasOwn(manifest.contributes, "configurationDefaults"),
    false,
    "the extension must not replace the browser-local default terminal profile",
  );
  assert.equal(manifest.activationEvents.includes("onTerminalProfile:sharedTerminals.fast"), true);
  assert.equal(manifest.files.includes("scripts/pty_broker.py"), true);
  assert.deepEqual(manifest.contributes.menus.commandPalette, [
    { command: "sharedTerminals.open", when: "false" },
    { command: "sharedTerminals.rename", when: "false" },
    { command: "sharedTerminals.delete", when: "false" },
  ]);
});
