#!/usr/bin/env node
// moonjupyter launcher: starts the MoonBit kernel for a Jupyter connection file.
// Invoked by kernelspec via `node launcher.mjs <connection-file>`.
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const connectionFile = process.argv[2];
if (!connectionFile) {
  console.error("usage: node launcher.mjs <connection-file>");
  process.exit(1);
}

const command = `moon run main --target js -- -f "${connectionFile}"`;
const child = spawn(command, {
  cwd: root,
  shell: true,          // shell:true for Windows PATH resolution of `moon`
  stdio: ["ignore", "inherit", "inherit"],
});

child.on("exit", (code) => process.exit(code ?? 1));
