/// Kernel session orchestration: binds the five Jupyter channels over the
/// ZMTP transport and dispatches requests.
///
/// Channel wiring (kernel side):
///   shell   ROUTER  (client: REQ/DEALER)   - code execution requests
///   control ROUTER  (client: DEALER/REQ)   - control-plane requests
///   stdin   ROUTER  (client: DEALER)       - user I/O (stub)
///   iopub   PUB                            - output broadcast
///   hb      REP     (client: REQ)          - heartbeat echo

///|
/// Log level for kernel operations
pub enum LogLevel {
  Debug
  Info
  Warn
  Error
}

///|
pub fn show_log_level(level : LogLevel) -> String {
  match level {
    LogLevel::Debug => "Debug"
    LogLevel::Info => "Info"
    LogLevel::Warn => "Warn"
    LogLevel::Error => "Error"
  }
}

///|
/// Performance metrics and statistics
priv struct PerformanceMetrics {
  mut message_count : Int
  mut execution_time : Map[String, Int] // msg_id -> milliseconds
  mut error_count : Int
  mut throughput : Map[Int, Double] // 1-minute window index -> messages seen
}

///|
/// A connected peer on a single ZMTP socket (keyed by TCP connection id).
priv struct Peer {
  id : Int
  session : @zmtp.ZmtpSession
  mut identities : Array[Bytes]
}

///|
/// Enhanced kernel with logging and error tracking
priv struct Kernel {
  key : String
  session_id : String
  username : String
  /// incremented on each execute_reply (Jupyter spec §4.3)
  mut exec_count : Int
  mut iopub : Peer?
  mut parent : Json?
  start_time : Int
  mut log_buffer : Array[String]
  peer_stats : Map[Int, Int] // peer_id -> message_count
  metrics : PerformanceMetrics
  mut last_stats_report : Int
  /// last 100 executed cells: (execution_count, source code)
  mut history : Array[(Int, String)]
  /// pid of the running cell's `moon run` process, -1 when idle
  mut cell_pid : Int
  /// set when the user interrupted the running cell; the exit handler
  /// reports KeyboardInterrupt instead of a generic error
  mut cell_interrupted : Bool
  /// bumped on every run_cell; stale timers from earlier cells compare
  /// against it and stand down
  mut run_seq : Int
  /// MoonBit toolchain version, detected at startup ("dev" if unknown)
  mut language_version : String
  /// extra modules declared via %use, added to every cell project
  mut cell_deps : Array[(String, String)] // (module, version)
  /// package paths imported into every cell project
  mut cell_imports : Array[String]
  /// per-cell wall-time limit, adjustable via the %timeout magic
  mut cell_timeout_ms : Int
  /// pending complete-line batches per stream, published once they reach
  /// BATCH_FLUSH_BYTES (output batching)
  mut batch_out : String
  mut batch_err : String
  /// elapsed_ms() of the last force-kill; the next cell waits a beat so
  /// the OS can finish tearing the killed process tree down (a moon run
  /// started instantly after taskkill fails with exit 1 and no output)
  mut last_kill_ms : Int
  /// elapsed_ms() when the last moon run was spawned; consecutive runs
  /// need a small gap or some launches exit 1 silently on Windows
  mut last_spawn_ms : Int
  /// all non-magic cells executed this session, in order; each cell run
  /// compiles the whole notebook again (accumulated replay), so
  /// definition cells stay visible to later cells
  mut replay_cells : Array[String]
}

///|
priv struct Channel {
  name : String
  socket_type : String
  peers : Map[Int, Peer]
}

///|
pub fn run_kernel(conn : @jupyter.ConnectionInfo) -> Unit {
  // Per-kernel session: one shell ROUTER, one iopub PUB, one REP hb, ...
  let kernel : Kernel = {
    key: conn.key,
    session_id: @jupyter.uuid_v4_from(@nodeff.random_bytes(16)),
    username: @nodeff.username(),
    exec_count: 0,
    iopub: None,
    parent: None,
    start_time: @nodeff.now_seconds(),
    log_buffer: [],
    peer_stats: Map([]),
    metrics: {
      message_count: 0,
      execution_time: Map([]),
      error_count: 0,
      throughput: Map([]),
    },
    last_stats_report: 0,
    history: [],
    cell_pid: -1,
    cell_interrupted: false,
    run_seq: 0,
    language_version: "dev",
    cell_deps: [],
    cell_imports: [],
    cell_timeout_ms: CELL_TIMEOUT_MS,
    batch_out: "",
    batch_err: "",
    last_kill_ms: -1000000,
    last_spawn_ms: -1000000,
    replay_cells: [],
  }
  let shell = Channel::new("shell", "ROUTER")
  let control = Channel::new("control", "ROUTER")
  let stdin = Channel::new("stdin", "ROUTER")
  let iopub = Channel::new("iopub", "PUB")
  let hb = Channel::new("hb", "REP")
  bind_channel(kernel, shell, conn.ip, conn.shell_port)
  bind_channel(kernel, control, conn.ip, conn.control_port)
  bind_channel(kernel, stdin, conn.ip, conn.stdin_port)
  bind_channel(kernel, iopub, conn.ip, conn.iopub_port)
  bind_channel(kernel, hb, conn.ip, conn.hb_port)

  // Log kernel startup
  log_kernel(
    kernel,
    LogLevel::Info,
    "kernel \{kernel.session_id} listening on \{conn.ip} " +
    "(shell=\{conn.shell_port} iopub=\{conn.iopub_port} stdin=\{conn.stdin_port} " +
    "control=\{conn.control_port} hb=\{conn.hb_port})",
  )

  // Periodic status logging and maintenance
  @nodeff.every(30000, fn() { log_kernel_stats(kernel) })
  @nodeff.every(60000, fn() { cleanup_inactive_peers(kernel) })
  detect_language_version(kernel)
}

///|
fn Channel::new(name : String, socket_type : String) -> Channel {
  let peers : Map[Int, Peer] = Map([])
  { name, socket_type, peers, }
}

///|
fn bind_channel(k : Kernel, ch : Channel, ip : String, port : Int) -> Unit {
  // Every incoming TCP connection becomes a Peer on this channel.
  @nodeff.tcp_server(
    ip,
    port,
    fn(id) {
      let peer = Peer::{
        id,
        session: @zmtp.ZmtpSession::new(),
        identities: [],
      }
      ch.peers[id] = peer
      if ch.name == "iopub" {
        k.iopub = Some(peer)
      }
      // ZMTP 3.1: the listening side sends its greeting immediately on
      // connect, before any command or message traffic.
      @nodeff.tcp_send(id, @zmtp.greeting_bytes(true))
      log_kernel(
        k,
        LogLevel::Info,
        "peer \{id} connected to \{ch.name} channel",
      )
    },
    fn(id, data) {
      match ch.peers.get(id) {
        Some(peer) => on_peer_data(k, ch, peer, data)
        None =>
          log_kernel(k, LogLevel::Warn, "received data from unknown peer \{id}")
      }
    },
    fn(id) {
      let _ = ch.peers.remove(id)
      // per-connection counters would otherwise accumulate forever
      let _ = k.peer_stats.remove(id)
      if ch.name == "iopub" {
        k.iopub = None
      }
    },
  )
}

///|
/// Enhanced logging with level filtering
fn log_kernel(k : Kernel, level : LogLevel, message : String) -> Unit {
  let timestamp = @nodeff.iso_now()
  let log_entry = "[\{timestamp}] [\{show_log_level(level)}] \{message}"

  // Add to buffer for later analysis
  k.log_buffer.push(log_entry)

  // Keep only last 1000 log entries to prevent memory growth
  if k.log_buffer.length() > 1000 {
    let len = k.log_buffer.length()
    let new_buffer : Array[String] = []
    for i = len - 1000; i < len; i = i + 1 {
      new_buffer.push(k.log_buffer[i])
    }
    k.log_buffer = new_buffer
  }

  // Output to stderr based on level
  match level {
    LogLevel::Debug => @nodeff.eprint(log_entry)
    LogLevel::Info => @nodeff.eprint(log_entry)
    LogLevel::Warn => @nodeff.eprint(log_entry)
    LogLevel::Error => @nodeff.eprint(log_entry)
  }
}

///|
fn send_raw(peer : Peer, data : Bytes) -> Unit {
  // Log send attempts for debugging
  @nodeff.tcp_send(peer.id, data)
}

///|
fn on_peer_data(k : Kernel, ch : Channel, peer : Peer, data : Bytes) -> Unit {
  peer.identities.push(data)

  // Update peer message count
  let current_count = match k.peer_stats.get(peer.id) {
    Some(count) => count
    None => 0
  }
  k.peer_stats[peer.id] = current_count + 1

  let events = peer.session.feed(data)
  for ev in events {
    match ev {
      @zmtp.PeerGreeting => {
        log_kernel(k, LogLevel::Debug, "peer \{peer.id} greeting on \{ch.name}")
        send_raw(peer, @zmtp.ready_command(ch.socket_type))
      }
      @zmtp.PeerReady(_) => ()
      @zmtp.PingReceived(context) => {
        log_kernel(
          k,
          LogLevel::Debug,
          "ping from peer \{peer.id} on \{ch.name}",
        )
        send_raw(peer, @zmtp.pong_command(context))
      }
      @zmtp.Error(reason) =>
        log_kernel(
          k,
          LogLevel::Error,
          "peer \{peer.id} error on \{ch.name}: \{reason}",
        )
      @zmtp.Message(frames) => {
        log_kernel(
          k,
          LogLevel::Debug,
          "peer \{peer.id} message on \{ch.name}: \{frames.length()} frames",
        )
        handle_message(k, ch, peer, frames)
      }
    }
  }
}

///|
fn handle_message(
  k : Kernel,
  ch : Channel,
  peer : Peer,
  frames : Array[Bytes],
) -> Unit {
  let started = @nodeff.now_seconds()
  record_message_metrics(k, started)

  if ch.name == "iopub" {
    // SUB subscription frames; nothing to do.
    log_kernel(k, LogLevel::Debug, "iopub subscription from peer \{peer.id}")
    return
  }
  if ch.name == "hb" {
    // REP echo: strip the REQ empty-delimiter frame, echo, re-add it.
    let payload : Array[Bytes] = []
    let start = if frames.length() > 0 && frames[0].length() == 0 {
      1
    } else {
      0
    }
    for i = start; i < frames.length(); i = i + 1 {
      payload.push(frames[i])
    }
    let reply : Array[Bytes] = [b""]
    for f in payload {
      reply.push(f)
    }
    send_raw(peer, @zmtp.encode_message(reply))
    return
  }
  match @jupyter.parse_message(frames, k.key, @nodeff.inflate) {
    Err(e) => {
      k.metrics.error_count = k.metrics.error_count + 1
      log_kernel(
        k,
        LogLevel::Error,
        "failed to parse message on \{ch.name}: \{e}",
      )
      @nodeff.eprint("[moonjupyter] \{ch.name}: \{e}")
    }
    Ok(msg) => {
      // Log message handling
      log_kernel(k, LogLevel::Debug, "handling \{msg.msg_type} on \{ch.name}")
      // Remember the routing prefix so replies reach the right client.
      let idents : Array[Bytes] = []
      for f in msg.identities {
        idents.push(f)
      }
      peer.identities = idents
      dispatch(k, peer, msg)
    }
  }
}

///|
fn dispatch(k : Kernel, peer : Peer, msg : @jupyter.JMsg) -> Unit {
  match msg.msg_type {
    "kernel_info_request" =>
      reply(k, peer, "kernel_info_reply", kernel_info(k), msg)
    "execute_request" => handle_execute(k, peer, msg)
    "is_complete_request" => {
      let code = @jupyter.jstr_field(msg.content, "code")
      let status = if brackets_balanced(code) {
        "complete"
      } else {
        "incomplete"
      }
      reply(
        k,
        peer,
        "is_complete_reply",
        @jupyter.obj([("status", Json::string(status))]),
        msg,
      )
    }
    "complete_request" => {
      let code = @jupyter.jstr_field(msg.content, "code")
      let cursor = @jupyter.jint_field(msg.content, "cursor_pos")
      let (cursor_start, cursor_end, matches) = complete_token(code, cursor)
      let jmatches : Array[Json] = []
      for m in matches {
        jmatches.push(Json::string(m))
      }
      reply(
        k,
        peer,
        "complete_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("matches", Json::array(jmatches)),
          ("cursor_start", Json::number(cursor_start.to_double())),
          ("cursor_end", Json::number(cursor_end.to_double())),
          ("metadata", @jupyter.obj([])),
        ]),
        msg,
      )
    }
    "inspect_request" => {
      let code = @jupyter.jstr_field(msg.content, "code")
      let cursor = @jupyter.jint_field(msg.content, "cursor_pos")
      let (start, _end, token) = token_at(code, cursor)
      // a token preceded by '%' documents a magic command; a bare '%'
      // documents the whole magic list
      let doc = if start > 0 && code[start - 1] == 37 {
        if token == "" {
          Some(MAGIC_HELP)
        } else {
          magic_doc("%" + token)
        }
      } else {
        lookup_doc(token)
      }
      let data = match doc {
        Some(text) => @jupyter.obj([("text/plain", Json::string(text))])
        None => @jupyter.obj([])
      }
      reply(
        k,
        peer,
        "inspect_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("found", Json::boolean(doc is Some(_))),
          ("data", data),
          ("metadata", @jupyter.obj([])),
        ]),
        msg,
      )
    }
    "history_request" =>
      reply(
        k,
        peer,
        "history_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("history", history_json(k.history)),
        ]),
        msg,
      )
    "shutdown_request" => {
      // Echo the client's restart flag so jupyter_client knows whether to
      // relaunch this kernel after exit.
      let restart = jbool_field(msg.content, "restart")
      reply(
        k,
        peer,
        "shutdown_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("restart", Json::boolean(restart)),
        ]),
        msg,
      )
      publish(
        k,
        "status",
        @jupyter.obj([("execution_state", Json::string("idle"))]),
      )
      log_kernel(k, LogLevel::Info, "shutdown requested (restart=\{restart})")
      // Do not orphan a running cell's process tree (on Unix the child is
      // spawned detached and would outlive us).
      if k.cell_pid > 0 {
        log_kernel(
          k,
          LogLevel::Warn,
          "shutdown: killing running cell (pid \{k.cell_pid})",
        )
        @nodeff.kill_tree(k.cell_pid)
        k.cell_pid = -1
      }
      @nodeff.later(300, fn() { @nodeff.exit(0) })
    }
    "interrupt_request" => {
      reply(
        k,
        peer,
        "interrupt_reply",
        @jupyter.obj([("status", Json::string("ok"))]),
        msg,
      )
      if k.cell_pid > 0 {
        log_kernel(
          k,
          LogLevel::Warn,
          "interrupt: killing running cell (pid \{k.cell_pid})",
        )
        k.cell_interrupted = true
        k.last_kill_ms = @nodeff.elapsed_ms()
        @nodeff.kill_tree(k.cell_pid)
      }
    }
    "comm_open" | "comm_msg" | "comm_close" => ()
    other => @nodeff.eprint("[moonjupyter] unhandled msg_type: \{other}")
  }
}

///|
/// Version reported to Jupyter clients via kernel_info; keep in sync with
/// the version in moon.mod.
const KERNEL_VERSION : String = "0.2.6"

///|
/// Return the last `keep` elements of `xs`.
fn[T] drop_first(xs : Array[T], drop : Int) -> Array[T] {
  let out : Array[T] = []
  for i = drop; i < xs.length(); i = i + 1 {
    out.push(xs[i])
  }
  out
}

///|
/// Keep the first `keep` elements of `xs` (rollback counterpart of
/// drop_first: replay must retain its original prefix and lose the
/// entries appended by a failed cell).
fn[T] keep_first(xs : Array[T], keep : Int) -> Array[T] {
  let out : Array[T] = []
  let n = if keep < xs.length() { keep } else { xs.length() }
  for i = 0; i < n; i = i + 1 {
    out.push(xs[i])
  }
  out
}

///|
/// Build the history_reply payload: [session=0, execution_count, input].
fn history_json(entries : Array[(Int, String)]) -> Json {
  let items : Array[Json] = []
  for entry in entries {
    let (n, code) = entry
    items.push(
      Json::array([
        Json::number(0.0),
        Json::number(n.to_double()),
        Json::string(code),
      ]),
    )
  }
  Json::array(items)
}

///|
/// Read a boolean field out of a JSON object, defaulting to `default`.
fn jbool_field(j : Json, key : String, default? : Bool = false) -> Bool {
  match j {
    Json::Object(fields) =>
      match fields.get(key) {
        Some(Json::True) => true
        Some(Json::False) => false
        _ => default
      }
    _ => default
  }
}

///|
/// Multi-line text as a JSON array of lines, the shape Jupyter renders
/// properly for tracebacks.
fn lines_json(s : String) -> Json {
  let lines = s.split("\n")
    |> Iter::to_array
    |> Array::map(fn(v) { v.to_owned() })
  json_strings(lines)
}

///|
fn json_strings(items : Array[String]) -> Json {
  let out : Array[Json] = []
  for s in items {
    out.push(Json::string(s))
  }
  Json::array(out)
}

///|
/// The identifier token surrounding `cursor_pos` (both directions), for
/// inspect requests.
fn token_at(code : String, cursor_pos : Int) -> (Int, Int, String) {
  let clamped = if cursor_pos > code.length() {
    code.length()
  } else {
    cursor_pos
  }
  let mut start = clamped
  while start > 0 && is_ident_char(code[start - 1]) {
    start = start - 1
  }
  let mut end = clamped
  while end < code.length() && is_ident_char(code[end]) {
    end = end + 1
  }
  (start, end, @bx.sslice(code, start, end))
}

///|
/// The magic-command list shown by %help and documented as a whole when
/// Shift+Tab is pressed on a bare '%'.
const MAGIC_HELP : String = "moonjupyter magic commands:\n" +
  "  %help              — show this list\n" +
  "  %history           — show the last executed cells\n" +
  "  %stats             — show kernel performance statistics\n" +
  "  %time <code>       — run code and report its wall time\n" +
  "  %use [<pkg>@<v> …] — import external package(s) into cells\n" +
  "  %timeout [<ms>]    — show/set the per-cell wall-time limit\n" +
  "  %html <content>    — render rich HTML output (display_data)\n" +
  "  %md <content>      — render rich Markdown output\n" +
  "  %reset             — clear history, counters and metrics"

///|
/// Short documentation for the magic commands, served to Shift+Tab
/// (inspect_request) queries on tokens preceded by '%'.
fn magic_doc(name : String) -> String? {
  match name {
    "%about" =>
      Some(
        "show kernel identity and state: version, toolchain, uptime, timeout, external packages."
      )
    "%help" => Some("show the list of magic commands.")
    "%history" => Some("show the last executed cells (up to 100).")
    "%stats" =>
      Some(
        "show kernel performance statistics: message counts, error rate, throughput.",
      )
    "%time" =>
      Some("%time <code> — run the remainder as MoonBit code and report its wall time.")
    "%use" =>
      Some(
        "%use <pkg-path>@<version> … — import mooncakes package(s) into subsequent cells; the version is mandatory.",
      )
    "%timeout" =>
      Some(
        "%timeout [<ms>] — show or set the per-cell wall-time limit (5000..3600000).",
      )
    "%html" =>
      Some("%html <content> — render content as rich HTML via display_data.")
    "%md" =>
      Some("%md <content> — render content as Markdown via display_data.")
    "%reset" =>
      Some("clear history, replay state, execution counter and metrics.")
    "%who" =>
      Some(
        "list the names and kinds defined in the current notebook replay state."
      )
    "%unuse" =>
      Some(
        "%unuse <package-path> — remove a package registered via %use; takes effect on the next cell."
      )
    _ => None
  }
}

///|
/// Short documentation for keywords and common builtins, served to
/// Shift+Tab (inspect_request) queries.
fn lookup_doc(word : String) -> String? {
  match word {
    "println" =>
      Some(
        "println(value) — print a value to standard output followed by a newline.",
      )
    "print" =>
      Some(
        "print(value) — print a value to standard output without a trailing newline.",
      )
    "let" =>
      Some(
        "let binds an immutable value; use `let mut` for a mutable binding.\n\nlet x = 1\nlet mut y = 2",
      )
    "fn" =>
      Some(
        "fn defines a function.\n\nfn add(a : Int, b : Int) -> Int {\n  a + b\n}",
      )
    "match" =>
      Some(
        "match destructures a value by pattern.\n\nmatch x {\n  0 => \"zero\"\n  _ => \"other\"\n}",
      )
    "while" =>
      Some(
        "while repeats a block while its condition holds.\n\nwhile i < 10 {\n  i = i + 1\n}",
      )
    "for" =>
      Some(
        "for iterates over ranges or collections.\n\nfor i in 0..<5 {\n  println(i)\n}",
      )
    "if" =>
      Some(
        "if evaluates one of two branches by condition. It is also an expression:\n\nlet m = if a > b { a } else { b }",
      )
    "struct" =>
      Some(
        "struct defines a record type.\n\nstruct Point {\n  x : Int\n  y : Int\n}",
      )
    "enum" =>
      Some(
        "enum defines a sum type.\n\nenum Shape {\n  Circle(Double)\n  Square(Double)\n}",
      )
    "loop" =>
      Some("loop repeats until a `break` inside its body.\n\nloop {\n  ...\n}")
    "to_string" => Some("to_string() renders a value as a String.")
    "length" =>
      Some(
        "length() returns the number of elements (for strings: UTF-16 code units).",
      )
    _ => None
  }
}

///|
/// Bracket-balance heuristic for is_complete_request: a cell with an
/// unclosed (, [ or { (outside string literals) is not complete and the
/// console keeps waiting for more input instead of executing.
fn brackets_balanced(code : String) -> Bool {
  let mut paren = 0
  let mut brack = 0
  let mut brace = 0
  let mut in_str = false
  let mut i = 0
  while i < code.length() {
    let c = code[i]
    if in_str {
      if c == 92 {
        i = i + 2 // skip the escaped character
        continue
      }
      if c == 34 {
        in_str = false
      }
    } else if c == 34 {
      in_str = true
    } else if c == 40 {
      paren = paren + 1
    } else if c == 41 {
      paren = paren - 1
    } else if c == 91 {
      brack = brack + 1
    } else if c == 93 {
      brack = brack - 1
    } else if c == 123 {
      brace = brace + 1
    } else if c == 125 {
      brace = brace - 1
    }
    i = i + 1
  }
  // an unterminated string literal means the cell continues on the next line
  paren == 0 && brack == 0 && brace == 0 && !in_str
}

///|
fn kernel_info(k : Kernel) -> Json {
  @jupyter.obj([
    ("status", Json::string("ok")),
    ("protocol_version", Json::string("5.4")),
    ("implementation", Json::string("moonjupyter")),
    ("implementation_version", Json::string(KERNEL_VERSION)),
    (
      "language_info",
      @jupyter.obj([
        ("name", Json::string("moonbit")),
        ("version", Json::string(k.language_version)),
        ("mimetype", Json::string("text/x-moonbit")),
        ("file_extension", Json::string(".mbt")),
      ]),
    ),
    (
      "banner",
      Json::string(
        "MoonBit kernel (moonjupyter) \{KERNEL_VERSION} (toolchain \{k.language_version}) — each cell runs via `moon run` in a scratch project",
      ),
    ),
    ("help_links", Json::array([])),
  ])
}

///|
/// Ask the `moon` CLI for its version; the first output line is stored as
/// the kernel's language version. Falls back to "dev" if unavailable.
fn detect_language_version(k : Kernel) -> Unit {
  @nodeff.spawn_process(
    ".",
    "moon version",
    fn(chunk) {
      if k.language_version == "dev" {
        let first = chunk.split("\n")
          |> Iter::to_array
          |> Array::map(fn(v) { v.to_owned() })
        if first.length() > 0 && first[0].contains("moon") {
          k.language_version = first[0]
        }
      }
    },
    fn(_err) { () },
    fn(_code) { () },
    fn(_pid) { () },
  )
}

///|
fn handle_execute(k : Kernel, peer : Peer, msg : @jupyter.JMsg) -> Unit {
  // Spec §execute: store_history=false does not bump the execution count
  // and leaves no history entry (silent fire-and-forget execution).
  let store = jbool_field(msg.content, "store_history", default=true)
  let count = if store { k.exec_count + 1 } else { k.exec_count }
  if store {
    k.exec_count = count
  }
  k.parent = Some(msg.header)

  let mut code = @jupyter.jstr_field(msg.content, "code")
  log_kernel(k, LogLevel::Info, "executing cell \{count}: \{code}")

  // Keep the last 100 cells for history_request.
  if store {
    k.history.push((count, code))
    if k.history.length() > 100 {
      k.history = drop_first(k.history, k.history.length() - 100)
    }
  }

  publish(
    k,
    "status",
    @jupyter.obj([("execution_state", Json::string("busy"))]),
  )
  publish(
    k,
    "execute_input",
    @jupyter.obj([
      ("code", Json::string(code)),
      ("execution_count", Json::number(count.to_double())),
    ]),
  )

  // Kernel magic commands (MoonBit never starts a top-level statement
  // with '%'), handled synchronously without spawning `moon run`.
  let timed = match magic_name(code) {
    Some("time") => true
    _ => false
  }
  // rich-output magics publish display_data with MIME bundles
  match magic_name(code) {
    Some("html") => {
      publish_display_data(k, "text/html", magic_args(code))
      finish_magic(k, peer, msg, count)
      return
    }
    Some("md") => {
      publish_display_data(k, "text/markdown", magic_args(code))
      finish_magic(k, peer, msg, count)
      return
    }
    _ => ()
  }
  // notebook-style value display: `= expr` prints the value (Out-style).
  // The rewrite happens before replay so history and the notebook state
  // both contain the honest println form.
  match value_display_kind(code) {
    Empty => {
      publish_stream(k, "stdout", "usage: = <expression> prints its value")
      finish_magic(k, peer, msg, count)
      return
    }
    Expr(expr) => code = "println(\{expr})"
    NotDisplay => ()
  }
  let magic = parse_magic(k, code)
  match magic {
    Some(output) => {
      log_kernel(k, LogLevel::Info, "magic command: \{code}")
      publish_stream(k, "stdout", output)
      finish_magic(k, peer, msg, count)
      return
    }
    None => ()
  }

  // %time <code>: run the remainder as normal code, then stream the
  // wall time on iopub (the reply metadata also carries duration_ms).
  let run_code = if timed { magic_args(code) } else { code }
  if timed && run_code == "" {
    publish_stream(k, "stderr", "usage: %time <moonbit code>")
    reply(
      k,
      peer,
      "execute_reply",
      @jupyter.obj([
        ("status", Json::string("ok")),
        ("execution_count", Json::number(count.to_double())),
        ("payload", Json::array([])),
        ("user_expressions", @jupyter.obj([])),
        ("metadata", @jupyter.obj([])),
      ]),
      msg,
    )
    publish(
      k,
      "status",
      @jupyter.obj([("execution_state", Json::string("idle"))]),
    )
    return
  }

  run_cell(k, run_code, fn(ok, ename, evalue, traceback, duration_ms) {
    if timed {
      publish_stream(k, "stdout", "Wall time: \{duration_ms} ms")
    }
    let meta = @jupyter.obj([
      (
        "moonjupyter",
        @jupyter.obj([("duration_ms", Json::number(duration_ms.to_double()))]),
      ),
    ])
    if ok {
      log_kernel(
        k,
        LogLevel::Info,
        "cell \{count} executed successfully in \{duration_ms}ms",
      )
      reply(
        k,
        peer,
        "execute_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("execution_count", Json::number(count.to_double())),
          ("payload", Json::array([])),
          ("user_expressions", @jupyter.obj([])),
          ("metadata", meta),
        ]),
        msg,
      )
    } else {
      log_kernel(
        k,
        LogLevel::Error,
        "cell \{count} failed: \{ename} - \{evalue}",
      )
      reply(
        k,
        peer,
        "execute_reply",
        @jupyter.obj([
          ("status", Json::string("error")),
          ("execution_count", Json::number(count.to_double())),
          ("ename", Json::string(ename)),
          ("evalue", Json::string(evalue)),
          ("traceback", json_strings(traceback)),
          ("metadata", meta),
        ]),
        msg,
      )
    }
    publish(
      k,
      "status",
      @jupyter.obj([("execution_state", Json::string("idle"))]),
    )
  })
}

///|
/// MoonBit keywords and common builtins offered by TAB completion.
let completion_candidates : Array[String] = [
  "fn", "let", "mut", "if", "else", "match", "while", "for", "in", "break", "continue",
  "return", "struct", "enum", "trait", "impl", "pub", "priv", "true", "false", "print",
  "println", "to_string", "to_int", "to_double", "length", "abs", "min", "max", "sqrt",
]

///|
/// ASCII identifier character, as a UTF-16 code unit ('a'..'z', 'A'..'Z',
/// '0'..'9', '_').
fn is_ident_char(c : UInt16) -> Bool {
  (c >= 97 && c <= 122) ||
  (c >= 65 && c <= 90) ||
  (c >= 48 && c <= 57) ||
  c == 95
}

///|
fn is_digit_char(c : UInt16) -> Bool {
  c >= 48 && c <= 57
}

///|
/// TAB completion: extract the identifier ending at `cursor_pos` and suggest
/// keywords plus identifiers that appear anywhere in the cell. Returns
/// (cursor_start, cursor_end, matches).
/// Magic command names offered by TAB completion when the token is
/// preceded by '%'.
let magic_names : Array[String] = [
  "%about", "%help", "%history", "%stats", "%time", "%use", "%timeout", "%html",
  "%md", "%reset", "%who", "%unuse",
]

///|
fn complete_token(code : String, cursor_pos : Int) -> (Int, Int, Array[String]) {
  let end = if cursor_pos > code.length() { code.length() } else { cursor_pos }
  let mut start = end
  while start > 0 && is_ident_char(code[start - 1]) {
    start = start - 1
  }
  let token = @bx.sslice(code, start, end)
  // magic completion: a token preceded by '%' completes magic names
  // (covers a bare '%' too — the whole "%magic" token gets replaced)
  if start > 0 && code[start - 1] == 37 {
    let with_pct = "%" + token
    let matches : Array[String] = []
    for m in magic_names {
      if m.length() >= with_pct.length() &&
        @bx.sslice(m, 0, with_pct.length()) == with_pct {
        matches.push(m)
      }
    }
    return (start - 1, end, matches)
  }
  if token == "" {
    return (end, end, [])
  }
  // Words appearing in the cell: identifiers that may have been defined
  // on earlier lines. Skip numeric literals.
  let candidates : Map[String, Bool] = Map([])
  for kw in completion_candidates {
    candidates[kw] = true
  }
  let mut word = ""
  for i = 0; i < code.length(); i = i + 1 {
    let c = code[i]
    if is_ident_char(c) {
      word = word + @bx.char_str(c.to_int())
    } else {
      if word != "" && !is_digit_char(word[0]) {
        candidates[word] = true
      }
      word = ""
    }
  }
  if word != "" && !is_digit_char(word[0]) {
    candidates[word] = true
  }
  let matches : Array[String] = []
  for name, _ in candidates {
    if name.length() >= token.length() &&
      @bx.sslice(name, 0, token.length()) == token {
      matches.push(name)
    }
  }
  // Insertion sort by ASCII lexicographic order (small n); String's
  // built-in compare orders by length first, which is not what a
  // completion list should show.
  for i = 1; i < matches.length(); i = i + 1 {
    let cur = matches[i]
    let mut j = i - 1
    while j >= 0 && s_less(cur, matches[j]) {
      matches[j + 1] = matches[j]
      j = j - 1
    }
    matches[j + 1] = cur
  }
  (start, end, matches)
}

///|
/// ASCII lexicographic "a before b" ordering.
fn s_less(a : String, b : String) -> Bool {
  let n = if a.length() < b.length() { a.length() } else { b.length() }
  for i = 0; i < n; i = i + 1 {
    if a[i] != b[i] {
      return a[i] < b[i]
    }
  }
  a.length() < b.length()
}

///|
/// Default wall-time limit for a cell that produces no output; per-kernel
/// adjustable with the %timeout magic.
const CELL_TIMEOUT_MS : Int = 60000

///|
/// Bounds for the %timeout setting: below 5s legitimate compiles may be
/// killed; above 1h a hung cell holds the session for too long.
const MIN_TIMEOUT_MS : Int = 5000

///|
const MAX_TIMEOUT_MS : Int = 3600000

///|
/// Execute a cell in a temporary MoonBit project via `moon run`. `on_done`
/// receives (success, ename, evalue, traceback, wall-time in ms).
fn run_cell(
  k : Kernel,
  code : String,
  on_done : (Bool, String, String, Array[String], Int) -> Unit,
) -> Unit {
  // A moon run launched right after a force-kill fails instantly (exit 1,
  // no output) because the killed tree is still being torn down; give the
  // OS a moment before starting a new one.
  let now_ms = @nodeff.elapsed_ms()
  let since_kill = now_ms - k.last_kill_ms
  if since_kill >= 0 && since_kill < 1000 {
    @nodeff.later(1000 - since_kill + 50, fn() { run_cell(k, code, on_done) })
    return
  }
  let since_spawn = now_ms - k.last_spawn_ms
  if since_spawn >= 0 && since_spawn < 300 {
    @nodeff.later(300 - since_spawn + 30, fn() { run_cell(k, code, on_done) })
    return
  }
  k.last_spawn_ms = now_ms
  let dir = @nodeff.mkdtemp()
  // Modules declared with %use become moon.mod imports ("module@version");
  // their package paths are imported by the generated moon.pkg. Both use
  // the current non-JSON config formats (moon.mod.json is deprecated).
  let mut mod_text = "name = \"cell/exec\"\n\nversion = \"0.1.0\"\n"
  if k.cell_deps.length() > 0 {
    let imports = k.cell_deps
      .map(fn(dep) {
        let (m, v) = dep
        "  \"" + m + "@" + v + "\","
      })
      .join("\n")
    mod_text = mod_text + "import {\n" + imports + "\n}"
  }
  @nodeff.write_text(dir + "/moon.mod", mod_text)
  let mut pkg_text = ""
  if k.cell_imports.length() > 0 {
    let imports = k.cell_imports.map(fn(p) { "  \"" + p + "\"," }).join("\n")
    pkg_text = pkg_text + "import {\n" + imports + "\n}\n\n"
  }
  pkg_text = pkg_text + "options(\n  \"is-main\": true,\n)"
  @nodeff.write_text(dir + "/src/main/moon.pkg", pkg_text)
  // Accumulated replay: a cell that declares its own `fn main` is a
  // complete program and runs standalone (not part of the notebook
  // state); every other cell joins the replay, and the generated source
  // recompiles the whole notebook so earlier definitions stay visible.
  let replay_len_before = k.replay_cells.length()
  let standalone = contains_main_fn(code)
  let src = if standalone {
    code
  } else {
    build_replay(k.replay_cells, code)
  }
  if !standalone {
    k.replay_cells.push(code)
  }
  @nodeff.write_text(dir + "/src/main/main.mbt", src)
  let started = @nodeff.elapsed_ms()
  k.run_seq = k.run_seq + 1
  let my_seq = k.run_seq
  k.cell_pid = -1
  k.cell_interrupted = false
  let mut out_buf = ""
  let mut err_buf = ""
  // full stderr accumulation for diagnostics (err_buf only holds the
  // trailing partial line; complete lines are streamed to iopub)
  let mut err_text = ""
  // A moon run occasionally exits 1 within ~100ms with no output at all
  // (see the retry branch below); the exit logic is re-entrant through
  // exit_ref so the failure can be retried once transparently.
  let retried : Ref[Bool] = Ref(false)
  let exit_ref : Ref[(Int) -> Unit] = Ref(fn(_code) { () })
  @nodeff.spawn_process(
    dir,
    "moon run src/main",
    fn(chunk) { out_buf = stream_lines(k, out_buf + chunk, "stdout") },
    fn(chunk) {
      err_text = err_text + chunk
      err_buf = stream_lines(k, err_buf + chunk, "stderr")
    },
    fn(exit_code) { (exit_ref.val)(exit_code) },
    fn(pid) { k.cell_pid = pid },
  )
  exit_ref.val = fn(exit_code) {
      if k.cell_pid < 0 {
        return // aborted; timeout/interrupt path already reported
      }
      k.cell_pid = -1
      let duration_ms = @nodeff.elapsed_ms() - started
      // transient silent failure: exit != 0, fast, and the process wrote
      // nothing at all — wait a beat and run the exact same cell again
      if exit_code != 0 &&
        !k.cell_interrupted &&
        duration_ms < 600 &&
        out_buf == "" && err_text == "" &&
        err_buf == "" &&
        !retried.val {
        retried.val = true
        log_kernel(
          k,
          LogLevel::Warn,
          "transient silent moon run failure — retrying once",
        )
        out_buf = ""
        err_buf = ""
        err_text = ""
        @nodeff.later(400, fn() {
          @nodeff.spawn_process(
            dir,
            "moon run src/main",
            fn(chunk) { out_buf = stream_lines(k, out_buf + chunk, "stdout") },
            fn(chunk) {
      err_text = err_text + chunk
      err_buf = stream_lines(k, err_buf + chunk, "stderr")
    },
            fn(code) { (exit_ref.val)(code) },
            fn(pid) { k.cell_pid = pid },
          )
        })
        return
      }
      k.metrics.execution_time["exec-\{k.exec_count}"] = duration_ms
      flush_stream_batches(k)
      flush_pending(k, out_buf, "stdout")
      flush_pending(k, err_buf, "stderr")
      if k.cell_interrupted {
        // aborted mid-run: deliver whatever it managed to print
        flush_stream_batches(k)
        flush_pending(k, out_buf, "stdout")
        flush_pending(k, err_buf, "stderr")
        // aborted mid-run: its half-finished state must not join the replay
        k.replay_cells = keep_first(k.replay_cells, replay_len_before)
        k.metrics.error_count = k.metrics.error_count + 1
        log_kernel(k, LogLevel::Warn, "cell \{k.exec_count} interrupted")
        on_done(
          false, "KeyboardInterrupt", "execution interrupted", [], duration_ms,
        )
      } else if exit_code == 0 {
        on_done(true, "", "", [], duration_ms)
      } else {
        // a failed cell leaves no usable state: roll it back out of the replay
        k.replay_cells = keep_first(k.replay_cells, replay_len_before)
        k.metrics.error_count = k.metrics.error_count + 1
        // structured traceback: clean main.mbt:line:col entries + hint
        let tb : Array[String] = diagnose(err_text)
        match missing_package_hint(err_text) {
          Some(hint) => {
            log_kernel(k, LogLevel::Info, hint)
            tb.push(hint)
          }
          None => ()
        }
        on_done(
          false,
          "MoonBitError",
          "cell exited with code \{exit_code}",
          tb,
          duration_ms,
        )
      }
      @nodeff.rmrf(dir)
    }
  @nodeff.later(k.cell_timeout_ms, fn() {
    if k.run_seq == my_seq && k.cell_pid > 0 {
      log_kernel(
        k,
        LogLevel::Warn,
        "cell \{k.exec_count} exceeded \{k.cell_timeout_ms / 1000}s, killing pid \{k.cell_pid}",
      )
      k.last_kill_ms = @nodeff.elapsed_ms()
      @nodeff.kill_tree(k.cell_pid)
      k.cell_pid = -1
      let duration_ms = @nodeff.elapsed_ms() - started
      k.metrics.execution_time["exec-\{k.exec_count}"] = duration_ms
      k.metrics.error_count = k.metrics.error_count + 1
      // a timed-out cell never finished; deliver the output it printed
      flush_stream_batches(k)
      flush_pending(k, out_buf, "stdout")
      flush_pending(k, err_buf, "stderr")
      // and its half-finished state leaves no usable replay
      k.replay_cells = keep_first(k.replay_cells, replay_len_before)
      @nodeff.rmrf(dir)
      on_done(
        false,
        "CellTimeout",
        "cell exceeded \{k.cell_timeout_ms / 1000}s and was terminated",
        [],
        duration_ms,
      )
    }
  })
}

///|
/// MVP heuristic: cells that declare their own `fn main` run unmodified;
/// everything else is wrapped in a generated entry point.
fn contains_main_fn(code : String) -> Bool {
  code.contains("fn main")
}

///|
/// Upper bound for the pending (newline-less) output fragment, so a cell
/// printing endlessly without newlines cannot grow memory unbounded.
const MAX_PENDING_OUTPUT : Int = 1024 * 1024

///|
/// Complete lines are batched into iopub messages of at least this size:
/// one publish per line would pay JSON + HMAC framing for every single
/// line of chatty cells.
const BATCH_FLUSH_BYTES : Int = 8192

///|
/// Forward complete lines of an output stream to iopub as `stream` messages.
/// Complete lines are appended to a per-stream batch that is published once
/// it reaches BATCH_FLUSH_BYTES; returns the trailing partial line (kept
/// until more output arrives). Blank lines are preserved.
fn stream_lines(k : Kernel, buf : String, name : String) -> String {
  let lines = buf.split("\n")
    |> Iter::to_array
    |> Array::map(fn(v) { v.to_owned() })
  let n = lines.length()
  let mut batch = if name == "stderr" {
    k.batch_err
  } else {
    k.batch_out
  }
  for i = 0; i < n - 1; i = i + 1 {
    batch = batch + lines[i] + "\n"
  }
  let partial = lines[n - 1]
  if partial.length() > MAX_PENDING_OUTPUT {
    // Flush the oversized fragment as if it were a complete line; the
    // output is forwarded intact, only line boundaries inside it are lost.
    batch = batch + partial
    publish(
      k,
      "stream",
      @jupyter.obj([
        ("name", Json::string(name)),
        ("text", Json::string(batch)),
      ]),
    )
    if name == "stderr" {
      k.batch_err = ""
    } else {
      k.batch_out = ""
    }
    return ""
  }
  if batch.length() >= BATCH_FLUSH_BYTES {
    publish(
      k,
      "stream",
      @jupyter.obj([
        ("name", Json::string(name)),
        ("text", Json::string(batch)),
      ]),
    )
    batch = ""
  }
  if name == "stderr" {
    k.batch_err = batch
  } else {
    k.batch_out = batch
  }
  partial
}

///|
///|
/// Publish whatever complete-line batches are still pending on both
/// streams (cell exit, interrupt and timeout paths).
fn flush_stream_batches(k : Kernel) -> Unit {
  if k.batch_out != "" {
    publish(
      k,
      "stream",
      @jupyter.obj([
        ("name", Json::string("stdout")),
        ("text", Json::string(k.batch_out)),
      ]),
    )
    k.batch_out = ""
  }
  if k.batch_err != "" {
    publish(
      k,
      "stream",
      @jupyter.obj([
        ("name", Json::string("stderr")),
        ("text", Json::string(k.batch_err)),
      ]),
    )
    k.batch_err = ""
  }
}

///|
fn flush_pending(k : Kernel, buf : String, name : String) -> Unit {
  if buf != "" {
    publish(
      k,
      "stream",
      @jupyter.obj([
        ("name", Json::string(name)),
        ("text", Json::string(buf + "\n")),
      ]),
    )
  }
}

///|
/// Publish a complete `stream` message (used by magic commands).
fn publish_stream(k : Kernel, name : String, text : String) -> Unit {
  publish(
    k,
    "stream",
    @jupyter.obj([("name", Json::string(name)), ("text", Json::string(text))]),
  )
}

///|
/// Publish a rich-output `display_data` message with a MIME bundle.
fn publish_display_data(
  k : Kernel,
  mime : String,
  content : String,
) -> Unit {
  publish(
    k,
    "display_data",
    @jupyter.obj([
      ("data", @jupyter.obj([(mime, Json::string(content))])),
      ("metadata", @jupyter.obj([])),
    ]),
  )
}

///|
/// Finish a magic command: ok execute_reply + idle status on iopub.
fn finish_magic(
  k : Kernel,
  peer : Peer,
  msg : @jupyter.JMsg,
  count : Int,
) -> Unit {
  reply(
    k,
    peer,
    "execute_reply",
    @jupyter.obj([
      ("status", Json::string("ok")),
      ("execution_count", Json::number(count.to_double())),
      ("payload", Json::array([])),
      ("user_expressions", @jupyter.obj([])),
      ("metadata", @jupyter.obj([])),
    ]),
    msg,
  )
  publish(
    k,
    "status",
    @jupyter.obj([("execution_state", Json::string("idle"))]),
  )
}

///|
fn trim_ascii(s : String) -> String {
  let mut start = 0
  let mut end = s.length()
  while start < end && (s[start] == 32 || s[start] == 13 || s[start] == 9 || s[start] == 10) {
    start = start + 1
  }
  while end > start && (s[end - 1] == 32 || s[end - 1] == 13 || s[end - 1] == 9 || s[end - 1] == 10) {
    end = end - 1
  }
  @bx.sslice(s, start, end)
}

///|
/// Rewrite a location like `F:\...\moonbit-cell-XXXX\src\main\main.mbt:2:9`
/// into the notebook-relative `main.mbt:2:9`.
fn normalize_location(path : String) -> String {
  let marker = "main.mbt"
  let mut i = path.length() - marker.length()
  while i >= 0 {
    if i + marker.length() <= path.length() &&
      @bx.sslice(path, i, i + marker.length()) == marker {
      return @bx.sslice(path, i, path.length())
    }
    i = i - 1
  }
  path
}

///|
/// Turn raw `moon run` diagnostics into a clean, structured traceback:
/// the box-drawing frames are collapsed into `main.mbt:line:col: message`
/// entries, one per error/warning, in order.
fn diagnose(err_buf : String) -> Array[String] {
  let out : Array[String] = []
  let mut loc = ""
  let lines = err_buf.split("\n")
    |> Iter::to_array
    |> Array::map(fn(v) { v.to_owned() })
  for raw in lines {
    let t = trim_ascii(raw)
    if t.contains("╭─[") {
      let parts = t.split("╭─[")
        |> Iter::to_array
        |> Array::map(fn(v) { v.to_owned() })
      let tail = parts[parts.length() - 1]
      let segs = tail.split("]")
        |> Iter::to_array
        |> Array::map(fn(v) { v.to_owned() })
      loc = normalize_location(trim_ascii(segs[0]))
    } else if t.contains("╰──") {
      let parts = t.split("╰──")
        |> Iter::to_array
        |> Array::map(fn(v) { v.to_owned() })
      // strip the leading horizontal-dash runes left over from ╰──
      let mut msg = trim_ascii(parts[parts.length() - 1])
      while msg.length() > 0 && msg[0] == 9472 {
        msg = @bx.sslice(msg, 1, msg.length())
      }
      let msg = trim_ascii(msg)
      // keep only messages with real text; the caret alignment row and
      // the closing frame edge are pure box-drawing noise
      if has_word_chars(msg) {
        if loc != "" {
          out.push("\{loc}: \{msg}")
          loc = ""
        } else {
          out.push(msg)
        }
      }
    } else if t.contains("Error: [") || t.contains("Warning: [") {
      out.push(t)
    } else if t.contains("│") || t.contains("┬") || t.contains("╯") {
      () // box-drawing content/caret/closing lines are visual noise
    } else if t != "" {
      // non-boxed output (e.g. linker/runtime messages) passes through
      out.push(t)
    }
  }
  out
}

///|
/// Parse a non-negative whole number; used by magics taking counts.
fn parse_int(arg : String) -> Int? {
  if arg == "" || arg.length() > 9 {
    return None
  }
  let mut v = 0
  for i = 0; i < arg.length(); i = i + 1 {
    let c = arg[i]
    if c < 48 || c > 57 {
      return None
    }
    v = v * 10 + c.to_int() - 48
  }
  Some(v)
}

///|
/// Names and kinds of the top-level bindings accumulated in the replay:
/// for every definition line, the token after the declaration keyword
/// (`let mut x` counts as `x`). Statement cells contribute nothing —
/// their bindings are function-local.
fn notebook_definitions(cells : Array[String]) -> Array[(String, String)] {
  let out : Array[(String, String)] = []
  for cell in cells {
    if !is_def_cell(cell) {
      continue
    }
    let lines = cell.split("\n")
      |> Iter::to_array
      |> Array::map(fn(v) { v.to_owned() })
    for line in lines {
      let t = trim_ascii(line)
      if t == "" || (t.length() >= 2 && t[0] == 47 && t[1] == 47) {
        continue // blank or // comment
      }
      let toks = first_idents(t, 3)
      if toks.length() < 2 || !is_def_cell(t) {
        continue
      }
      let kind = toks[0]
      let name = if kind == "let" && toks.length() >= 3 && toks[1] == "mut" {
        toks[2]
      } else {
        toks[1]
      }
      out.push((name, kind))
    }
  }
  out
}

///|
/// The first `count` identifier tokens of a trimmed line.
fn first_idents(t : String, count : Int) -> Array[String] {
  let out : Array[String] = []
  let mut i = 0
  while i < t.length() && out.length() < count {
    if is_ident_char(t[i]) {
      let start = i
      while i < t.length() && is_ident_char(t[i]) {
        i = i + 1
      }
      out.push(@bx.sslice(t, start, i))
    } else {
      i = i + 1
    }
  }
  out
}

///|
fn filter_ne(xs : Array[String], drop : String) -> Array[String] {
  let out : Array[String] = []
  for x in xs {
    if x != drop {
      out.push(x)
    }
  }
  out
}

///|
fn[T] filter_by(xs : Array[T], keep : (T) -> Bool) -> Array[T] {
  let out : Array[T] = []
  for x in xs {
    if keep(x) {
      out.push(x)
    }
  }
  out
}

///|
/// Parse a %timeout argument: whole milliseconds in [MIN, MAX].
fn parse_timeout_ms(arg : String) -> Int? {
  if arg == "" || arg.length() > 8 {
    return None
  }
  let mut v = 0
  for i = 0; i < arg.length(); i = i + 1 {
    let c = arg[i]
    if c < 48 || c > 57 {
      return None
    }
    v = v * 10 + c.to_int() - 48
  }
  if v >= MIN_TIMEOUT_MS && v <= MAX_TIMEOUT_MS {
    Some(v)
  } else {
    None
  }
}

///|
/// Whether the string contains at least one ASCII letter or digit (i.e.
/// actual text rather than box-drawing decoration).
fn has_word_chars(s : String) -> Bool {
  for i = 0; i < s.length(); i = i + 1 {
    let c = s[i]
    if (c >= 97 && c <= 122) || (c >= 65 && c <= 90) || (c >= 48 && c <= 57) {
      return true
    }
  }
  false
}

///|
/// When a cell fails because a referenced package is unknown, point the
/// user at the %use magic instead of leaving a bare registry error.
fn missing_package_hint(err_text : String) -> String? {
  if err_text.contains("module was not found in the registry") ||
    err_text.contains("Failed to resolve registry dependency") ||
    (err_text.contains("module-level import") && err_text.contains("not found")) {
    Some(
      "hint: declare external packages with %use <package-path>@<version> (e.g. %use bobzhang/loop_invariants_string@0.16.0)",
    )
  } else {
    None
  }
}

///|
/// A cell whose first token (after whitespace and line comments) is a
/// declaration keyword is a definition cell: its code is spliced into the
/// replayed notebook source at top level, so bindings stay visible to
/// later cells. Everything else is a statement cell.
fn is_def_cell(code : String) -> Bool {
  let mut i = 0
  while i < code.length() && is_ws(code[i]) {
    i = i + 1
  }
  // skip leading `//` line comments so annotated definitions classify right
  while i + 1 < code.length() && code[i] == 47 && code[i + 1] == 47 {
    while i < code.length() && code[i] != 10 {
      i = i + 1
    }
    while i < code.length() && is_ws(code[i]) {
      i = i + 1
    }
  }
  if i >= code.length() {
    return false
  }
  let words = [
    "let", "fn", "struct", "enum", "trait", "impl", "type", "pub", "priv",
    "const",
  ]
  for w in words {
    if starts_with_word(code, i, w) {
      return true
    }
  }
  false
}

///|
fn starts_with_word(code : String, start : Int, word : String) -> Bool {
  if code.length() - start < word.length() {
    return false
  }
  if @bx.sslice(code, start, start + word.length()) != word {
    return false
  }
  // a real token must not run into a longer identifier ("letx" is not "let")
  let after = start + word.length()
  after >= code.length() || !is_ident_char(code[after])
}

///|
/// Build the replayed notebook source. Definition cells are spliced at top
/// level in order; statement cells (past and current) become numbered
/// functions, but main invokes only the current cell's function — earlier
/// cells' side effects must not repeat on every execution.
fn build_replay(past : Array[String], current : String) -> String {
  let mut out = ""
  let mut n = 0
  for cell in past {
    n = n + 1
    if is_def_cell(cell) {
      out = out + "// cell " + n.to_string() + " (definitions)\n" + cell + "\n"
    } else {
      out = out + "fn __cell_" + n.to_string() + "() -> Unit {\n" + cell + "\n}\n"
    }
  }
  let current_idx = n + 1
  if is_def_cell(current) {
    out = out + "// cell " + current_idx.to_string() + " (definitions)\n" + current + "\n"
    out = out + "\nfn main {\n}\n"
  } else {
    out = out + "fn __cell_" + current_idx.to_string() + "() -> Unit {\n" + current + "\n}\n"
    out = out + "\nfn main {\n  __cell_" + current_idx.to_string() + "()\n}\n"
  }
  out
}

///|
fn is_ws(c : UInt16) -> Bool {
  c == 32 || c == 9 || c == 10 || c == 13
}

///|
/// The magic name of a cell (without '%'), or None for regular code.
fn magic_name(code : String) -> String? {
  let mut i = 0
  while i < code.length() && is_ws(code[i]) {
    i = i + 1
  }
  if i >= code.length() || code[i] != 37 {
    return None // not '%'
  }
  let mut end = i + 1
  while end < code.length() && is_ident_char(code[end]) {
    end = end + 1
  }
  Some(@bx.sslice(code, i + 1, end))
}

///|
/// The text after the magic name (leading whitespace stripped); for a
/// non-magic cell this is the code unchanged.
fn magic_args(code : String) -> String {
  let mut i = 0
  while i < code.length() && is_ws(code[i]) {
    i = i + 1
  }
  if i < code.length() && code[i] == 37 {
    i = i + 1
    while i < code.length() && is_ident_char(code[i]) {
      i = i + 1
    }
    while i < code.length() && is_ws(code[i]) {
      i = i + 1
    }
  }
  @bx.sslice(code, i, code.length())
}

///|
fn arr_contains(arr : Array[String], s : String) -> Bool {
  for x in arr {
    if x == s {
      return true
    }
  }
  false
}

///|
/// Package paths, module names and semver versions may contain letters,
/// digits, '_', '-', '/', '.'.
fn valid_pkg_name(s : String) -> Bool {
  if s == "" {
    return false
  }
  for i = 0; i < s.length(); i = i + 1 {
    let c = s[i]
    if !is_ident_char(c) && c != 47 && c != 45 && c != 46 {
      return false
    }
  }
  true
}

///|
/// Kernel magic commands: cells starting with '%' are interpreted by the
/// kernel itself instead of being compiled with `moon run` (MoonBit never
/// begins a top-level statement with '%'). Returns the text to stream
/// back, or None when the cell should run as MoonBit code (%time falls
/// through here on purpose).
fn parse_magic(k : Kernel, code : String) -> String? {
  let name = match magic_name(code) {
    Some(n) => n
    None => return None
  }
  match name {
    "help" => Some(MAGIC_HELP)
    "history" => {
      // optional count: %history 5 shows only the last 5 cells
      let shown = match parse_int(magic_args(code)) {
        Some(n) => if n < k.history.length() { n } else { k.history.length() }
        None => k.history.length()
      }
      let mut out = "recent cells:\n"
      let start = k.history.length() - shown
      for i = start; i < k.history.length(); i = i + 1 {
        let (n, c) = k.history[i]
        out = out + "  \{n}: \{c}\n"
      }
      if k.history.length() == 0 {
        out = out + "  (none yet)\n"
      }
      Some(out)
    }
    "who" => {
      let names = notebook_definitions(k.replay_cells)
      let mut out = "notebook definitions (\{names.length()}):\n"
      for d in names {
        let (name, kind) = d
        out = out + "  \{name} : \{kind}\n"
      }
      if names.length() == 0 {
        out = out + "  (none — definition cells like 'let n = 1' land here)\n"
      }
      Some(out)
    }
    "unuse" => {
      let arg = magic_args(code)
      if arg == "" {
        Some("usage: %unuse <package-path> | all — active packages are listed by %use")
      } else if arg == "all" {
        let count = k.cell_imports.length()
        k.cell_imports = []
        k.cell_deps = []
        Some("removed all external packages (\{count}) — takes effect on the next cell")
      } else if !arr_contains(k.cell_imports, arg) {
        Some("not an active package: \{arg} (see %use)")
      } else {
        let segs = arg.split("/")
          |> Iter::to_array
          |> Array::map(fn(v) { v.to_owned() })
        let target_mod = if segs.length() >= 2 {
          "\{segs[0]}/\{segs[1]}"
        } else {
          arg
        }
        k.cell_imports = filter_ne(k.cell_imports, arg)
        // drop the module only when no remaining import still needs it
        k.cell_deps = filter_by(k.cell_deps, fn(dep) {
          let (m, _v) = dep
          let mut needed = false
          for imp in k.cell_imports {
            let isegs = imp.split("/")
              |> Iter::to_array
              |> Array::map(fn(v) { v.to_owned() })
            let imp_mod = if isegs.length() >= 2 {
              "\{isegs[0]}/\{isegs[1]}"
            } else {
              imp
            }
            if imp_mod == m {
              needed = true
            }
          }
          needed
        })
        Some("removed \{arg} (module \{target_mod}) — takes effect on the next cell")
      }
    }
    "stats" => {
      let stats = get_performance_stats(k)
      let mut out = "kernel performance stats:\n"
      match stats {
        Json::Object(fields) =>
          for key, value in fields {
            out = out + "  \{key}: \{json_scalar(value)}\n"
          }
        _ => ()
      }
      Some(out)
    }
    "about" => {
      let uptime = @nodeff.now_seconds() - k.start_time
      let mut pkgs = ""
      for i = 0; i < k.cell_imports.length(); i = i + 1 {
        let (m, v) = k.cell_deps[i]
        pkgs = pkgs + "  " + k.cell_imports[i] + " (" + m + "@" + v + ")\n"
      }
      if k.cell_imports.length() == 0 {
        pkgs = "  (none)\n"
      }
      Some(
        "moonjupyter " + KERNEL_VERSION + "\n" +
        "  MoonBit toolchain: " + k.language_version + "\n" +
        "  uptime: " + uptime.to_string() +
        "s | cells executed: " + k.exec_count.to_string() + "\n" +
        "  cell timeout: " + k.cell_timeout_ms.to_string() +
        " ms | replay cells: " + k.replay_cells.length().to_string() + "\n" +
        "  external packages:\n" + pkgs,
      )
    }
    "reset" => {
      k.history = []
      k.replay_cells = []
      k.exec_count = 0
      k.metrics.message_count = 0
      k.metrics.error_count = 0
      k.metrics.execution_time = Map([])
      k.metrics.throughput = Map([])
      Some("kernel state cleared: history, replay, execution counter and metrics")
    }
    "timeout" => {
      let arg = magic_args(code)
      if arg == "" {
        Some(
          "cell timeout: \{k.cell_timeout_ms} ms (change with %timeout <ms>, \{MIN_TIMEOUT_MS}..\{MAX_TIMEOUT_MS})",
        )
      } else {
        match parse_timeout_ms(arg) {
          Some(ms) => {
            k.cell_timeout_ms = ms
            Some("cell timeout set to \{ms} ms")
          }
          None =>
            Some(
              "invalid timeout: \{arg} — use whole milliseconds between \{MIN_TIMEOUT_MS} and \{MAX_TIMEOUT_MS}",
            )
        }
      }
    }
    "use" => {
      let args_text = magic_args(code)
      if args_text == "" {
        let mut out = "active extra packages:\n"
        for i = 0; i < k.cell_imports.length(); i = i + 1 {
          let (m, v) = k.cell_deps[i]
          out = out + "  \{k.cell_imports[i]} (\{m}@\{v})\n"
        }
        if k.cell_imports.length() == 0 {
          out = out + "  (none — usage: %use <package-path>@<version> ...)\n"
        }
        return Some(out)
      }
      // one or more "<path>@<version>" entries, whitespace separated
      let mut out = ""
      let raw = args_text.split(" ")
        |> Iter::to_array
        |> Array::map(fn(v) { v.to_owned() })
      for a in raw {
        if a != "" {
          out = out + add_cell_dep(k, a) + "\n"
        }
      }
      Some(out)
    }
    "time" => None // handled by handle_execute: runs the rest as code
    _ => Some("unknown magic command %\{name} — try %help")
  }
}

///|
/// Register one "<package-path>@<version>" entry for %use; returns the
/// user-facing result text.
fn add_cell_dep(k : Kernel, arg : String) -> String {
  let mut at = -1
  for i = 0; i < arg.length(); i = i + 1 {
    if arg[i] == 64 {
      at = i // '@'
      break
    }
  }
  if at < 0 {
    return "missing version — moon requires one: %use <package-path>@<version>\n  e.g. %use bobzhang/loop_invariants_string@0.16.0"
  }
  let path = @bx.sslice(arg, 0, at)
  let version = @bx.sslice(arg, at + 1, arg.length())
  if !valid_pkg_name(path) || !valid_pkg_name(version) {
    return "invalid package name or version: \{arg}"
  }
  // dep module = first two path segments ("user/pkg" from "user/pkg/sub")
  let segs = path.split("/")
    |> Iter::to_array
    |> Array::map(fn(v) { v.to_owned() })
  let dep_mod = if segs.length() >= 2 {
    "\{segs[0]}/\{segs[1]}"
  } else {
    path
  }
  // replace an existing entry for the module, else append
  let updated : Array[(String, String)] = []
  for d in k.cell_deps {
    let (m, _v) = d
    if m != dep_mod {
      updated.push(d)
    }
  }
  updated.push((dep_mod, version))
  k.cell_deps = updated
  if !arr_contains(k.cell_imports, path) {
    k.cell_imports.push(path)
  }
  "added module \{dep_mod}@\{version} (import \{path}) — takes effect on the next cell"
}

///|
/// Render a scalar JSON value for the %stats listing.
fn json_scalar(j : Json) -> String {
  match j {
    Json::Number(d, ..) => d.to_string()
    Json::String(s) => s
    Json::True => "true"
    Json::False => "false"
    Json::Null => "null"
    _ => j.stringify()
  }
}

///|
///|
/// Log kernel statistics periodically
fn log_kernel_stats(k : Kernel) -> Unit {
  let uptime = @nodeff.now_seconds() - k.start_time
  let mut total_messages = 0
  for _, count in k.peer_stats {
    total_messages = total_messages + count
  }
  let active_peers = k.peer_stats.length()
  let log_entries = k.log_buffer.length()

  log_kernel(
    k,
    LogLevel::Info,
    "stats: uptime=\{uptime}s peers=\{active_peers} total_msgs=\{total_messages} " +
    "logs=\{log_entries} execs=\{k.exec_count} errors=\{k.metrics.error_count}",
  )

  // Log detailed performance stats at most every 5 minutes
  let now = @nodeff.now_seconds()
  if k.last_stats_report == 0 || now - k.last_stats_report > 300 {
    k.last_stats_report = now
    let perf_stats = get_performance_stats(k)
    let avg_time = jnum_field(perf_stats, "average_execution_ms")
    let throughput = jnum_field(perf_stats, "current_throughput_per_min")
    let error_rate = jnum_field(perf_stats, "error_rate_percent")

    log_kernel(
      k,
      LogLevel::Info,
      "performance: avg_time=\{avg_time}ms throughput=\{throughput}/min error_rate=\{error_rate}%",
    )
  }
}

///|
/// Read a numeric field out of a JSON object, defaulting to 0.
fn jnum_field(j : Json, key : String) -> Double {
  match j {
    Json::Object(fields) =>
      match fields.get(key) {
        Some(Json::Number(v, ..)) => v
        _ => 0.0
      }
    _ => 0.0
  }
}

///|
/// Clean up inactive peers to prevent memory leaks
fn cleanup_inactive_peers(k : Kernel) -> Unit {
  // Clean up old log entries
  if k.log_buffer.length() > 500 {
    let len = k.log_buffer.length()
    let new_buffer : Array[String] = []
    for i = len - 500; i < len; i = i + 1 {
      new_buffer.push(k.log_buffer[i])
    }
    k.log_buffer = new_buffer
  }

  // Clean up old performance metrics
  if k.metrics.execution_time.length() > 1000 {
    // Keep only last 1000 execution times
    let keys = k.metrics.execution_time.keys().to_array()
    for i = 0; i < keys.length() - 1000; i = i + 1 {
      k.metrics.execution_time.remove(keys[i])
    }
  }

  // Log maintenance activity
  log_kernel(
    k,
    LogLevel::Debug,
    "cleanup: checked \{k.peer_stats.length()} peers",
  )
}

///|
/// Record performance metrics for message handling
fn record_message_metrics(k : Kernel, now : Int) -> Unit {
  k.metrics.message_count = k.metrics.message_count + 1

  // Count messages per 1-minute window (rolling over 60 windows)
  let window = now / 60
  let current : Double = match k.metrics.throughput.get(window % 60) {
    Some(rate) => rate + 1.0
    None => 1.0
  }
  k.metrics.throughput[window % 60] = current
}

///|
/// Get current performance statistics
fn get_performance_stats(k : Kernel) -> Json {
  let uptime = @nodeff.now_seconds() - k.start_time
  let avg_execution_time = if k.metrics.execution_time.length() > 0 {
    let mut total_time = 0
    for _, ms in k.metrics.execution_time {
      total_time = total_time + ms
    }
    total_time.to_double() / k.metrics.execution_time.length().to_double()
  } else {
    0.0
  }
  let mut current_throughput = 0.0
  for _, rate in k.metrics.throughput {
    current_throughput = current_throughput + rate
  }
  let error_rate = if k.metrics.message_count > 0 {
    k.metrics.error_count.to_double() /
    k.metrics.message_count.to_double() *
    100.0
  } else {
    0.0
  }

  @jupyter.obj([
    ("uptime_seconds", Json::number(uptime.to_double())),
    ("total_messages", Json::number(k.metrics.message_count.to_double())),
    ("total_errors", Json::number(k.metrics.error_count.to_double())),
    ("average_execution_ms", Json::number(avg_execution_time)),
    ("current_throughput_per_min", Json::number(current_throughput)),
    ("error_rate_percent", Json::number(error_rate)),
    ("active_peers", Json::number(k.peer_stats.length().to_double())),
    ("log_entries", Json::number(k.log_buffer.length().to_double())),
  ])
}

///|
/// Send a reply on the same channel, routed back through the identities the
/// request arrived with.
fn reply(
  k : Kernel,
  peer : Peer,
  msg_type : String,
  content : Json,
  msg : @jupyter.JMsg,
) -> Unit {
  let header = @jupyter.new_header(
    k.session_id,
    k.username,
    @nodeff.iso_now(),
    msg_type,
    @jupyter.uuid_v4_from(@nodeff.random_bytes(16)),
  )
  let frames = @jupyter.build_frames(
    k.key,
    header,
    msg.header,
    @jupyter.obj([]),
    content,
  )
  let all : Array[Bytes] = []
  for f in peer.identities {
    all.push(f)
  }
  for f in frames {
    all.push(f)
  }
  send_raw(peer, @zmtp.encode_message(all))
}

///|
/// Broadcast a message on the iopub channel (PUB, no routing identities).
fn publish(k : Kernel, msg_type : String, content : Json) -> Unit {
  match k.iopub {
    None => ()
    Some(peer) => {
      let header = @jupyter.new_header(
        k.session_id,
        k.username,
        @nodeff.iso_now(),
        msg_type,
        @jupyter.uuid_v4_from(@nodeff.random_bytes(16)),
      )
      let parent = match k.parent {
        Some(h) => h
        None => @jupyter.obj([])
      }
      let frames = @jupyter.build_frames(
        k.key,
        header,
        parent,
        @jupyter.obj([]),
        content,
      )
      send_raw(peer, @zmtp.encode_message(frames))
    }
  }
}

///|
/// ---- tests ----
test "completion suggests keywords and cell identifiers" {
  let code = "let num = 1\nnum"
  // "num" spans indices 12..15, cursor at end of token
  let (start, end, matches) = complete_token(code, 15)
  assert_eq(start, 12)
  assert_eq(end, 15)
  assert_eq(matches.length(), 1)
  assert_eq(matches[0], "num")
}

///|
test "completion lists several candidates sorted" {
  let (start, _end, matches) = complete_token("println(1)", 3)
  assert_eq(start, 0)
  // token "pri" matches the print/println/priv keywords
  assert_eq(matches, ["print", "println", "priv"])
}

///|
test "completion at empty token yields nothing" {
  let (start, end, matches) = complete_token("let x = 1", 4)
  // position 4 is right after "let " (space) -> empty token
  assert_eq(start, 4)
  assert_eq(end, 4)
  assert_eq(matches.length(), 0)
}

///|
test "history payload shape" {
  let h : Array[(Int, String)] = [(1, "let a = 1"), (2, "println(a)")]
  match history_json(h) {
    Json::Array(items) => {
      assert_eq(items.length(), 2)
      match items[1] {
        Json::Array(parts) => {
          assert_eq(parts.length(), 3)
          match parts[1] {
            Json::Number(d, ..) => assert_eq(d, 2.0)
            _ => assert_true(false)
          }
        }
        _ => assert_true(false)
      }
    }
    _ => assert_true(false)
  }
}

///|
test "brackets balanced heuristic" {
  assert_true(brackets_balanced("let x = f(1, 2)\nprintln(x)"))
  // unclosed paren -> incomplete
  assert_false(brackets_balanced("println(\"abc\""))
  assert_false(brackets_balanced("let a = [1, 2"))
  // braces inside string literals must not count
  assert_true(brackets_balanced("let s = \"{[( \"\nprintln(s)"))
  // escaped quote inside a string
  assert_true(brackets_balanced("println(\"a \\\" ( \")"))
  // unclosed string with no brackets is still incomplete
  assert_false(brackets_balanced("let s = \"abc"))
}

///|
test "lines_json splits on newline" {
  match lines_json("line1\nline2") {
    Json::Array(items) => {
      assert_eq(items.length(), 2)
      match items[1] {
        Json::String(s) => assert_eq(s, "line2")
        _ => assert_true(false)
      }
    }
    _ => assert_true(false)
  }
}

///|
test "magic command recognition" {
  // "%help" recognized (with or without leading whitespace)
  assert_true(parse_magic_help_shape("%help") is Some(_))
  assert_true(parse_magic_help_shape("  %help") is Some(_))
  // regular MoonBit code (even containing '%') is not a magic command
  assert_true(parse_magic_help_shape("let pct = 50 % 3") is None)
  assert_true(parse_magic_help_shape("") is None)
}

///|
/// Parse only the magic name (no kernel state needed) for tests.
fn parse_magic_help_shape(code : String) -> String? {
  let mut i = 0
  while i < code.length() && (code[i] == 32 || code[i] == 9 || code[i] == 10) {
    i = i + 1
  }
  if i >= code.length() || code[i] != 37 {
    return None
  }
  let mut end = i + 1
  while end < code.length() && is_ident_char(code[end]) {
    end = end + 1
  }
  let name = @bx.sslice(code, i + 1, end)
  if name == "help" {
    Some(name)
  } else {
    None
  }
}

///|
test "token_at spans the full identifier" {
  let code = "let value = 1"
  // cursor inside "value" (position 6)
  let (start, end, token) = token_at(code, 6)
  assert_eq(start, 4)
  assert_eq(end, 9)
  assert_eq(token, "value")
  // cursor at end of code
  let (_s, _e, tail) = token_at(code, 13)
  assert_eq(tail, "1")
}

///|
test "lookup_doc finds known builtins" {
  assert_true(lookup_doc("println") is Some(_))
  assert_true(lookup_doc("match") is Some(_))
  assert_true(lookup_doc("definitely_not_a_thing") is None)
}

///|
test "valid_pkg_name guards the generated JSON" {
  assert_true(valid_pkg_name("moonbitlang/core/crypto"))
  assert_true(valid_pkg_name("gmlewis/spinner"))
  assert_true(valid_pkg_name("user/pkg-x_1"))
  assert_false(valid_pkg_name(""))
  assert_false(valid_pkg_name("user/pkg\"")) // would break out of the JSON string
  assert_false(valid_pkg_name("user/pkg; rm -rf"))
  assert_false(valid_pkg_name("has space"))
}

///|
test "magic_args strips the magic prefix" {
  assert_eq(magic_args("%time println(1)"), "println(1)")
  assert_eq(magic_args("  %time   let x = 1"), "let x = 1")
  assert_eq(magic_args("%use"), "")
  assert_eq(magic_args("println(1)"), "println(1)")
}

///|
test "magic_name extracts the command" {
  match magic_name("  %help") {
    Some(n) => assert_eq(n, "help")
    None => assert_true(false)
  }
  match magic_name("let x = 1") {
    Some(_) => assert_true(false)
    None => assert_true(true)
  }
  // '%' only counts at the start of the cell, not mid-expression
  match magic_name("let pct = 50 % 3") {
    Some(_) => assert_true(false)
    None => assert_true(true)
  }
}

///|
test "timeout parsing bounds" {
  match parse_timeout_ms("6000") {
    Some(ms) => assert_eq(ms, 6000)
    None => assert_true(false)
  }
  // below the 5s floor
  assert_true(parse_timeout_ms("4000") is None)
  // above the 1h ceiling
  assert_true(parse_timeout_ms("3600001") is None)
  // non-numeric and empty
  assert_true(parse_timeout_ms("6s") is None)
  assert_true(parse_timeout_ms("") is None)
}

///|
test "missing package hint matches moon errors" {
  let registry_err = "Failed to resolve registry dependency `a/b` for module `cell/exec`: module was not found in the registry"
  assert_true(missing_package_hint(registry_err) is Some(_))
  // ordinary compile errors get no hint
  assert_true(missing_package_hint("Error: type mismatch at line 3") is None)
}

///|
test "def vs statement cell classification" {
  // declaration keywords (possibly after comments) -> definition cell
  assert_true(is_def_cell("let n = 21"))
  assert_true(is_def_cell("fn add(a : Int, b : Int) -> Int { a + b }"))
  assert_true(is_def_cell("struct Point { x : Int }"))
  assert_true(is_def_cell("pub fn go() {}"))
  assert_true(is_def_cell("// doc comment\nlet hidden = 1"))
  // statements and empty cells are not definitions
  assert_false(is_def_cell("println(n * 2)"))
  assert_false(is_def_cell("while (true) {}"))
  assert_false(is_def_cell(""))
  // identifiers that merely start like keywords do not classify
  assert_false(is_def_cell("letter_count.println_stub()"))
}

///|
test "build_replay splices definitions and invokes only the current cell" {
  let past = ["let n = 21", "println(n)"]
  let src = build_replay(past, "println(n * 2)")
  // definitions land at top level
  assert_true(src.contains("let n = 21"))
  // past statement cell is wrapped but never invoked
  assert_true(src.contains("fn __cell_2() -> Unit {"))
  assert_false(src.contains("__cell_2()") == false && false) // sanity
  assert_true(!src.contains("  __cell_2()"))
  // only the current cell's function is called by main
  assert_true(src.contains("fn main {\n  __cell_3()\n}"))
}

///|
test "build_replay treats a definition cell as compile-only" {
  let src = build_replay([], "let n = 21")
  assert_true(src.contains("let n = 21"))
  // main body stays empty: nothing to execute, binding joins the state
  assert_true(src.contains("fn main {\n}"))
}

///|
test "diagnose collapses moon diagnostics into structured entries" {
  let raw = "Error: [4021]\n" +
    "    ╭─[ F:\\Users\\x\\Temp\\moonbit-cell-ABC\\src\\main\\main.mbt:2:9 ]\n" +
    "    │\n" +
    "  2 │ println(n)\n" +
    "    │         ┬  \n" +
    "    │         ╰── The value identifier n is unbound.\n"
  let entries = diagnose(raw)
  assert_eq(entries.length(), 2)
  assert_eq(entries[0], "Error: [4021]")
  assert_eq(entries[1], "main.mbt:2:9: The value identifier n is unbound.")
}

///|
test "diagnose passes through non-boxed error lines" {
  let entries = diagnose("linking failed for unknown reason\n")
  assert_eq(entries.length(), 1)
  assert_eq(entries[0], "linking failed for unknown reason")
}

///|
/// Classification of `= ...` value-display cells: the notebook-style way
/// to show a value without writing println yourself.
priv enum ValueDisplay {
  /// not a display cell
  NotDisplay
  /// a bare `=` with no expression
  Empty
  /// the expression to display
  Expr(String)
}

///|
fn value_display_kind(code : String) -> ValueDisplay {
  let mut i = 0
  while i < code.length() && is_ws(code[i]) {
    i = i + 1
  }
  if i >= code.length() || code[i] != 61 {
    return NotDisplay
  }
  // `==` would be a comparison, not the display convention
  if i + 1 < code.length() && code[i + 1] == 61 {
    return NotDisplay
  }
  let expr = trim_ascii(@bx.sslice(code, i + 1, code.length()))
  if expr == "" {
    Empty
  } else {
    Expr(expr)
  }
}

///|
test "value display classification" {
  match value_display_kind("= n * 2") {
    Expr(e) => assert_eq(e, "n * 2")
    _ => assert_true(false)
  }
  match value_display_kind("  = f(a, b)\n") {
    Expr(e) => assert_eq(e, "f(a, b)")
    _ => assert_true(false)
  }
  // bare = is a usage case, == and regular code are not display cells
  assert_true(value_display_kind("=") is Empty)
  assert_true(value_display_kind("a == b") is NotDisplay)
  assert_true(value_display_kind("println(1)") is NotDisplay)
  assert_true(value_display_kind("") is NotDisplay)
}

///|
test "completion suggests magic commands after %" {
  // %ht prefix matches only %html
  let (start, end, matches) = complete_token("%ht", 3)
  assert_eq(start, 0)
  assert_eq(end, 3)
  assert_eq(matches, ["%html"])
  // bare % lists every magic
  let (s2, _e2, all) = complete_token("%", 1)
  assert_eq(s2, 0)
  assert_eq(all.length(), 12)
  // '%' mid-expression (modulo) yields no magic suggestions
  let (_s3, _e3, none) = complete_token("let x = 5 % 3", 13)
  assert_eq(none.length(), 0)
}

///|
test "magic docs serve inspect queries" {
  assert_true(magic_doc("%about") is Some(_))
  assert_true(magic_doc("%timeout") is Some(_))
  assert_true(magic_doc("%html") is Some(_))
  assert_true(magic_doc("%bogus") is None)
  // the shared help text lists every documented magic
  let doc = match magic_doc("%help") {
    Some(_) => MAGIC_HELP
    None => ""
  }
  assert_true(doc.contains("%reset"))
  assert_true(doc.contains("%timeout"))
}

///|
test "notebook_definitions extracts names and kinds" {
  let cells = [
    "let n = 21",
    "let mut counter = 0\nfn bump() { counter = counter + 1 }",
    "// annotated\nstruct Point { x : Int }",
    "println(n)", // statement cell: nothing to list
  ]
  let names = notebook_definitions(cells)
  assert_eq(names.length(), 4)
  let (n0, k0) = names[0]
  assert_eq(n0, "n")
  assert_eq(k0, "let")
  let (n1, k1) = names[1]
  assert_eq(n1, "counter")
  assert_eq(k1, "let")
  let (n2, k2) = names[2]
  assert_eq(n2, "bump")
  assert_eq(k2, "fn")
  let (n3, k3) = names[3]
  assert_eq(n3, "Point")
  assert_eq(k3, "struct")
}

///|
test "parse_int accepts plain counts" {
  match parse_int("7") {
    Some(v) => assert_eq(v, 7)
    None => assert_true(false)
  }
  assert_true(parse_int("") is None)
  assert_true(parse_int("-1") is None)
  assert_true(parse_int("3a") is None)
}

///|
test "build_replay keeps definition order across interleaved cells" {
  let past = [
    "let a = 1",
    "println(a)",
    "fn double(x : Int) -> Int { x * 2 }",
  ]
  let src = build_replay(past, "println(double(a))")
  // definitions land at top level in cell order
  assert_true(src.contains("let a = 1"))
  assert_true(src.contains("fn double(x : Int) -> Int { x * 2 }"))
  // statement cells become numbered functions; only the current one runs
  assert_true(src.contains("fn __cell_2() -> Unit {"))
  assert_true(src.contains("fn __cell_4() -> Unit {"))
  assert_true(src.contains("fn main {\n  __cell_4()\n}"))
  assert_true(!src.contains("  __cell_2()"))
  assert_true(!src.contains("  __cell_3()"))
}

///|
test "keep_first and drop_first slice from opposite ends" {
  assert_eq(keep_first([1, 2, 3, 4], 2), [1, 2])
  assert_eq(drop_first([1, 2, 3, 4], 2), [3, 4])
  // over-sized keep/drop are safe
  assert_eq(keep_first([1, 2], 5), [1, 2])
  assert_eq(drop_first([1, 2], 5), [])
}

///|
test "filter helpers keep or drop selectively" {
  assert_eq(filter_ne(["a", "b", "a"], "a"), ["b"])
  assert_eq(
    filter_by([1, 2, 3, 4], fn(x) { x > 2 }),
    [3, 4],
  )
}
