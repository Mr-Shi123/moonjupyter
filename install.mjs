#!/usr/bin/env node
// Registers the MoonBit kernel with Jupyter (kernelspec installation).
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));

const kernelSpec = {
  argv: [process.execPath, path.join(root, "launcher.mjs"), "{connection_file}"],
  display_name: "MoonBit",
  language: "moonbit",
  interrupt_mode: "message",
};

function kernelsDir() {
  const probe = spawnSync("jupyter", ["--paths", "--json"], { encoding: "utf8" });
  if (probe.status === 0) {
    try {
      const j = JSON.parse(probe.stdout);
      if (j.data && j.data.length > 0) {
        return path.join(j.data[0], "kernels");
      }
    } catch (e) {
      /* fall through */
    }
  }
  if (process.platform === "win32" && process.env.APPDATA) {
    return path.join(process.env.APPDATA, "jupyter", "kernels");
  }
  return path.join(os.homedir(), ".local", "share", "jupyter", "kernels");
}

const dir = path.join(kernelsDir(), "moonbit");
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(path.join(dir, "kernel.json"), JSON.stringify(kernelSpec, null, 2));

console.log(`MoonBit kernel installed at ${dir}`);
console.log("Restart JupyterLab / Jupyter Notebook and select the 'MoonBit' kernel.");
console.log("Make sure the `moon` CLI is on your PATH.");
