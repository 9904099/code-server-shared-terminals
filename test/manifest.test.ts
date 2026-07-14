import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";

test("extension manifest is portable and enabled in untrusted workspaces", async () => {
  const manifest = JSON.parse(await readFile(join(process.cwd(), "package.json"), "utf8"));

  assert.equal(manifest.name, "code-server-shared-terminals");
  assert.equal(manifest.displayName, "code-server-shared-terminals");
  assert.equal(manifest.publisher, "9904099");
  assert.equal(manifest.version, "0.2.2");
  assert.deepEqual(manifest.extensionKind, ["workspace"]);
  assert.equal(manifest.capabilities.untrustedWorkspaces.supported, true);
  assert.equal(manifest.contributes.configuration.properties["sharedTerminals.registryPath"].default, "");
  assert.equal(manifest.contributes.configuration.properties["sharedTerminals.defaultCwd"].default, "");
});
