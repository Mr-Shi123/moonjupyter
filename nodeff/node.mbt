/// Node.js transport layer for the MoonBit Jupyter kernel.
///
/// All JavaScript FFI is concentrated in this package. The rest of the
/// project only sees the safe wrappers below (TCP servers keyed by
/// connection id, zlib inflate, filesystem helpers and process spawning).

// External JS values (opaque to MoonBit).
#external
priv type JsSocket

// ---- raw FFI ----

extern "js" fn js_server(
  host : String,
  port : Int,
  on_conn : (JsSocket) -> Unit,
) -> Unit =
  #|(host, port, on_conn) => {
  #|  const net = require("net");
  #|  const server = net.createServer((socket) => on_conn(socket));
  #|  server.listen(port, host);
  #|}

extern "js" fn js_attach(
  sock : JsSocket,
  on_data : (Bytes) -> Unit,
  on_close : () -> Unit,
) -> Unit =
  #|(sock, on_data, on_close) => {
  #|  sock.on("data", (d) => on_data(new Uint8Array(d)));
  #|  sock.on("close", () => on_close());
  #|  sock.on("error", () => { try { sock.destroy(); } catch (e) {} });
  #|}

extern "js" fn js_sock_write(sock : JsSocket, data : Bytes) -> Unit =
  #|(sock, data) => { sock.write(data); }

extern "js" fn js_sock_close(sock : JsSocket) -> Unit =
  #|(sock) => { try { sock.destroy(); } catch (e) {} }

extern "js" fn js_inflate(data : Bytes) -> Bytes =
  #|(data) => {
  #|  // Incoming may be plain JSON or zlib-compressed; fall back to empty on failure.
  #|  try { return require("zlib").inflateSync(data); }
  #|  catch (e) { return new Uint8Array(0); }
  #|}

extern "js" fn js_random_bytes(n : Int) -> Bytes =
  #|(n) => require("crypto").randomBytes(n)

extern "js" fn js_read_text(path : String) -> String =
  #|(p) => require("fs").readFileSync(p, "utf8")

extern "js" fn js_write_text(path : String, content : String) -> Unit =
  #|(p, c) => {
  #|  const fs = require("fs");
  #|  fs.mkdirSync(require("path").dirname(p), { recursive: true });
  #|  fs.writeFileSync(p, c);
  #|}

extern "js" fn js_mkdtemp() -> String =
  #|(() => {
  #|  const path = require("path");
  #|  const os = require("os");
  #|  return require("fs").mkdtempSync(path.join(os.tmpdir(), "moonbit-cell-"));
  #|})

extern "js" fn js_rmrf(path : String) -> Unit =
  #|(p) => {
  #|  try { require("fs").rmSync(p, { recursive: true, force: true }); } catch (e) {}
  #|}

extern "js" fn js_spawn(
  cwd : String,
  command : String,
  on_out : (String) -> Unit,
  on_err : (String) -> Unit,
  on_exit : (Int) -> Unit,
) -> Unit =
  #|(cwd, command, on_out, on_err, on_exit) => {
  #|  const cp = require("child_process");
  #|  const p = cp.spawn(command, { cwd: cwd, shell: true });
  #|  p.stdout.on("data", (d) => on_out(d.toString()));
  #|  p.stderr.on("data", (d) => on_err(d.toString()));
  #|  p.on("error", (e) => { on_err(String(e)); on_exit(-1); });
  #|  p.on("exit", (code) => on_exit(code === null ? -1 : code));
  #|}

extern "js" fn js_iso_now() -> String =
  #|(() => new Date().toISOString())

extern "js" fn js_username() -> String =
  #|(() => process.env.USER || process.env.USERNAME || "user")

extern "js" fn js_last_arg() -> String =
  #|(() => {
  #|  const a = process.argv;
  #|  return a.length > 0 ? a[a.length - 1] : "";
  #|})

extern "js" fn js_exit(code : Int) -> Unit =
  #|(c) => process.exit(c)

extern "js" fn js_eprint(s : String) -> Unit =
  #|(s) => process.stderr.write(s + "\n")

extern "js" fn js_later(ms : Int, cb : () -> Unit) -> Unit =
  #|(ms, cb) => setTimeout(cb, ms)

// ---- connection registry ----

let socks : Map[Int, JsSocket] = Map([])
let counter : Array[Int] = [0]

/// Listen on host:port. Each accepted connection gets a unique id; data and
/// close events are reported through the callbacks.
pub fn tcp_server(
  host : String,
  port : Int,
  on_connect : (Int) -> Unit,
  on_data : (Int, Bytes) -> Unit,
  on_close : (Int) -> Unit,
) -> Unit {
  js_server(host, port, fn(sock) {
    let id = counter[0]
    counter[0] = id + 1
    socks[id] = sock
    js_attach(sock, fn(data) {
      on_data(id, data)
    }, fn() {
      let _ = socks.remove(id)
      on_close(id)
    })
    on_connect(id)
  })
}

/// Write raw bytes to a connection.
pub fn tcp_send(id : Int, data : Bytes) -> Unit {
  match socks.get(id) {
    Some(sock) => js_sock_write(sock, data)
    None => ()
  }
}

/// Close a connection.
pub fn tcp_close(id : Int) -> Unit {
  match socks.get(id) {
    Some(sock) => {
      js_sock_close(sock)
      let _ = socks.remove(id)
    }
    None => ()
  }
}

// ---- zlib ----

/// True if the bytes look like a zlib stream (header byte 0x78).
pub fn is_zlib(b : Bytes) -> Bool {
  b.length() > 0 && b[0].to_int() == 0x78
}

/// Inflate a zlib stream; returns empty bytes on failure.
pub fn inflate(b : Bytes) -> Bytes {
  js_inflate(b)
}

// ---- misc ----

pub fn random_bytes(n : Int) -> Bytes {
  js_random_bytes(n)
}

pub fn read_text(path : String) -> String {
  js_read_text(path)
}

pub fn write_text(path : String, content : String) -> Unit {
  js_write_text(path, content)
}

/// Create a fresh temporary directory and return its path.
pub fn mkdtemp() -> String {
  js_mkdtemp()
}

pub fn rmrf(path : String) -> Unit {
  js_rmrf(path)
}

/// Run a shell command in `cwd`, streaming stdout/stderr chunks and the
/// exit code through callbacks.
pub fn spawn_process(
  cwd : String,
  command : String,
  on_out : (String) -> Unit,
  on_err : (String) -> Unit,
  on_exit : (Int) -> Unit,
) -> Unit {
  js_spawn(cwd, command, on_out, on_err, on_exit)
}

pub fn iso_now() -> String {
  js_iso_now()
}

pub fn username() -> String {
  js_username()
}

/// The last command-line argument (the connection file path).
pub fn last_arg() -> String {
  js_last_arg()
}

pub fn exit(code : Int) -> Unit {
  js_exit(code)
}

pub fn eprint(s : String) -> Unit {
  js_eprint(s)
}

/// Run `cb` after `ms` milliseconds.
pub fn later(ms : Int, cb : () -> Unit) -> Unit {
  js_later(ms, cb)
}
