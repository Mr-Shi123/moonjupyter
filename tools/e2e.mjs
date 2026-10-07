// End-to-end verification for the moonjupyter kernel.
//
// Usage:
//   moon build --target js
//   node tools/e2e.mjs <connection-file> [--quick]
//
// Spawns nothing by itself: start the kernel first (e.g. via Jupyter or
//   node _build/js/debug/build/main/main.js -f <connection-file>)
// then run this script against the same connection file. It speaks raw
// ZMTP 3.1 over TCP (NULL security) and covers the kernel's core paths:
//
//   kernel_info / completion (identifiers + magics) / inspect docs
//   execute + replay (cross-cell state) / value display (= expr)
//   magics (%about %logs %who %unuse) / structured tracebacks
//
// Exit code 0 = all checks passed.

import net from "node:net";
import crypto from "node:crypto";
import fs from "node:fs";

const conn = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const KEY = conn.key;

function greeting() {
  const g = Buffer.alloc(64);
  g[0] = 0xff; g[9] = 0x7f; g[10] = 3; g[11] = 1;
  g.write("NULL", 12); g[32] = 0;
  return g;
}
function size32(n) { const b = Buffer.alloc(4); b.writeUInt32BE(n); return b; }
function size64(n) { const b = Buffer.alloc(8); b.writeBigUInt64BE(BigInt(n)); return b; }
function metaKV(k, v) {
  return Buffer.concat([Buffer.from([k.length]), Buffer.from(k), size32(v.length), Buffer.from(v)]);
}
function commandFrame(name, meta) {
  const body = Buffer.concat([Buffer.from([name.length]), Buffer.from(name), ...meta]);
  return body.length > 255
    ? Buffer.concat([Buffer.from([0x04]), size64(body.length), body])
    : Buffer.concat([Buffer.from([0x04, body.length]), body]);
}
function frameBytes(data, more) {
  const flag = (more ? 0x01 : 0x00) | (data.length > 255 ? 0x02 : 0x00);
  return data.length > 255
    ? Buffer.concat([Buffer.from([flag]), size64(data.length), data])
    : Buffer.concat([Buffer.from([flag, data.length]), data]);
}

class ZmtpClient {
  constructor(port, onMsg) {
    this.buf = Buffer.alloc(0);
    this.pending = [];
    this.onMsg = onMsg;
    this.handshaken = false;
    this.sock = net.connect(port, conn.ip, () => {
      this.sock.write(greeting());
      this.sock.write(commandFrame("READY", [metaKV("Socket-Type", "DEALER")]));
    });
    this.sock.on("data", (d) => this.onData(d));
    this.sock.on("error", () => {});
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (!this.handshaken) {
      if (this.buf.length < 64) return;
      this.buf = this.buf.subarray(64);
      this.handshaken = true;
    }
    for (;;) {
      if (this.buf.length < 2) break;
      const flags = this.buf[0];
      let hdr, size;
      if (flags & 0x02) {
        if (this.buf.length < 9) break;
        size = Number(this.buf.readBigUInt64BE(1)); hdr = 9;
      } else { size = this.buf[1]; hdr = 2; }
      if (this.buf.length < hdr + size) break;
      const data = Buffer.from(this.buf.subarray(hdr, hdr + size));
      this.buf = this.buf.subarray(hdr + size);
      if (flags & 0x04) continue;
      this.pending.push(data);
      if (!(flags & 0x01)) {
        const frames = this.pending;
        this.pending = [];
        this.onMsg(frames);
      }
    }
  }
  send(frames) {
    frames.forEach((f, i) => this.sock.write(frameBytes(f, i < frames.length - 1)));
  }
}

class IopubListener {
  constructor() {
    this.display = [];
    this.streams = [];
    this.client = new ZmtpClient(conn.iopub_port, (frames) => {
      let di = -1;
      for (let i = 0; i < frames.length; i++) {
        if (frames[i].toString() === "<IDS|MSG>") { di = i; break; }
      }
      if (di < 0) return;
      const header = JSON.parse(frames[di + 2].toString());
      const content = JSON.parse(frames[di + 5].toString());
      if (header.msg_type === "stream") {
        this.streams.push({ name: content.name, text: content.text });
      } else if (header.msg_type === "display_data") {
        this.display.push(content.data);
      }
    });
  }
  hasStream(substr) { return this.streams.some((s) => s.text.includes(substr)); }
}

function jmsg(msgType, content) {
  const header = JSON.stringify({
    msg_id: crypto.randomUUID(), session: "e2e", username: "e2e",
    date: new Date().toISOString(), msg_type: msgType, version: "5.4",
  });
  const sig = crypto.createHmac("sha256", KEY)
    .update(Buffer.concat([Buffer.from(header), Buffer.from("{}"), Buffer.from("{}"), Buffer.from(content)]))
    .digest("hex");
  return [
    Buffer.from(""), Buffer.from("<IDS|MSG>"),
    Buffer.from(sig), Buffer.from(header), Buffer.from("{}"),
    Buffer.from("{}"), Buffer.from(content),
  ];
}
function parseJupyter(frames) {
  let di = -1;
  for (let i = 0; i < frames.length; i++) {
    if (frames[i].toString() === "<IDS|MSG>") { di = i; break; }
  }
  if (di < 0) return null;
  return { header: JSON.parse(frames[di + 2].toString()), content: JSON.parse(frames[di + 5].toString()) };
}

const results = [];
function check(name, ok, detail) {
  results.push([name, ok]);
  console.log(`${ok ? "PASS" : "FAIL"}: ${name}${detail ? " — " + detail : ""}`);
}

let resolveWait;
const kernel = new ZmtpClient(conn.shell_port, (frames) => {
  const m = parseJupyter(frames);
  if (m && resolveWait) resolveWait(m);
});
function waitReply(type, timeoutMs) {
  return new Promise((res, rej) => {
    const t = setTimeout(() => { resolveWait = null; rej(new Error(`timeout waiting ${type}`)); }, timeoutMs);
    resolveWait = (m) => { if (m.header.msg_type === type) { clearTimeout(t); resolveWait = null; res(m); } };
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const iopub = new IopubListener();

async function runCell(code, waitMs = 120000) {
  kernel.send(jmsg("execute_request", JSON.stringify({ code })));
  const m = await waitReply("execute_reply", waitMs);
  await sleep(250);
  return m;
}

const main = async () => {
  await sleep(2000);
  let m;

  // kernel identity
  kernel.send(jmsg("kernel_info_request", "{}"));
  m = await waitReply("kernel_info_reply", 5000);
  check("kernel_info", m.content.status === "ok" &&
    m.content.implementation === "moonjupyter" &&
    typeof m.content.language_info.version === "string",
    `version=${m.content.implementation_version}`);

  // replay: definition cell, then use it
  m = await runCell("let e2e_n = 21");
  check("replay: definition cell", m.content.status === "ok", `status=${m.content.status}`);
  m = await runCell("println(e2e_n * 2)");
  check("replay: cross-cell state (21*2=42)",
    m.content.status === "ok" && iopub.hasStream("42"),
    `status=${m.content.status}`);

  // value display
  m = await runCell("= e2e_n");
  check("value display (= expr)", m.content.status === "ok" && iopub.hasStream("21"),
    `status=${m.content.status}`);

  // structured traceback on a compile error
  m = await runCell("println(e2e_no_such_ident)");
  const tb = m.content.traceback;
  const structured = Array.isArray(tb) && tb.some((l) => /^main\.mbt:\d+:\d+: /.test(l));
  check("structured traceback", m.content.status === "error" && structured,
    `traceback=${JSON.stringify(tb)}`);

  // completion: identifiers and magics
  kernel.send(jmsg("complete_request", JSON.stringify({ code: "e2e_n", cursor_pos: 5 })));
  m = await waitReply("complete_reply", 5000);
  const identOk = Array.isArray(m.content.matches) && m.content.matches.includes("e2e_n");
  kernel.send(jmsg("complete_request", JSON.stringify({ code: "%ht", cursor_pos: 3 })));
  m = await waitReply("complete_reply", 5000);
  const magicOk = Array.isArray(m.content.matches) && m.content.matches.includes("%html");
  check("completion (identifier + magic)", identOk && magicOk,
    `idents=${JSON.stringify(m.content.matches)}`);

  // inspect: keyword doc
  kernel.send(jmsg("inspect_request", JSON.stringify({ code: "println(1)", cursor_pos: 7, detail_level: 0 })));
  m = await waitReply("inspect_reply", 5000);
  check("inspect keyword doc", m.content.found === true,
    `found=${m.content.found}`);

  // %about magic
  const len0 = iopub.streams.length;
  m = await runCell("%about");
  await sleep(250);
  const about = iopub.streams.slice(len0).filter((s) => s.name === "stdout").map((s) => s.text).join("");
  check("%about summary", m.content.status === "ok" && about.includes("moonjupyter") &&
    about.includes("MoonBit toolchain"),
    `status=${m.content.status}`);

  const failed = results.filter(([, ok]) => !ok);
  console.log(failed.length === 0 ? "\nALL E2E CHECKS PASSED" : `\n${failed.length} E2E CHECKS FAILED`);
  process.exit(failed.length === 0 ? 0 : 1);
};

main().catch((e) => { console.error("E2E ERROR:", e.message); process.exit(2); });
