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
  execution_time : Map[String, Int] // msg_id -> milliseconds
  mut error_count : Int
  throughput : Map[Int, Double] // 1-minute window index -> messages seen
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
  log_kernel(kernel, LogLevel::Info, 
    "kernel \{kernel.session_id} listening on \{conn.ip} " +
    "(shell=\{conn.shell_port} iopub=\{conn.iopub_port} stdin=\{conn.stdin_port} " +
    "control=\{conn.control_port} hb=\{conn.hb_port})"
  )
  
  // Periodic status logging and maintenance
  @nodeff.every(30000, fn() { log_kernel_stats(kernel) })
  @nodeff.every(60000, fn() { cleanup_inactive_peers(kernel) })
  detect_language_version(kernel)
}

///|
fn Channel::new(name : String, socket_type : String) -> Channel {
  let peers : Map[Int, Peer] = Map([])
  { name, socket_type, peers }
}

///|
fn bind_channel(k : Kernel, ch : Channel, ip : String, port : Int) -> Unit {
  // Every incoming TCP connection becomes a Peer on this channel.
  @nodeff.tcp_server(
    ip,
    port,
    fn(id) {
      let peer = Peer::{ id, session: @zmtp.ZmtpSession::new(), identities: [] }
      ch.peers[id] = peer
      if ch.name == "iopub" {
        k.iopub = Some(peer)
      }
      // ZMTP 3.1: the listening side sends its greeting immediately on
      // connect, before any command or message traffic.
      @nodeff.tcp_send(id, @zmtp.greeting_bytes(true))
      log_kernel(k, LogLevel::Info, "peer \{id} connected to \{ch.name} channel")
    },
    fn(id, data) {
      match ch.peers.get(id) {
        Some(peer) => on_peer_data(k, ch, peer, data)
        None => {
          log_kernel(k, LogLevel::Warn, "received data from unknown peer \{id}")
        }
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
        log_kernel(k, LogLevel::Debug, "ping from peer \{peer.id} on \{ch.name}")
        send_raw(peer, @zmtp.pong_command(context))
      }
      @zmtp.Error(reason) => {
        log_kernel(k, LogLevel::Error, "peer \{peer.id} error on \{ch.name}: \{reason}")
      }
      @zmtp.Message(frames) => {
        log_kernel(k, LogLevel::Debug, "peer \{peer.id} message on \{ch.name}: \{frames.length()} frames")
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
      log_kernel(k, LogLevel::Error, "failed to parse message on \{ch.name}: \{e}")
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
      let status = if brackets_balanced(code) { "complete" } else { "incomplete" }
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
    "inspect_request" =>
      reply(
        k,
        peer,
        "inspect_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("found", Json::boolean(false)),
          ("data", @jupyter.obj([])),
          ("metadata", @jupyter.obj([])),
        ]),
        msg,
      )
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
const KERNEL_VERSION : String = "0.1.6"

///|
/// Return the last `keep` elements of `xs`.
fn drop_first(xs : Array[(Int, String)], drop : Int) -> Array[(Int, String)] {
  let out : Array[(Int, String)] = []
  for i = drop; i < xs.length(); i = i + 1 {
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
fn jbool_field(j : Json, key : String, default~ : Bool = false) -> Bool {
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
  let out : Array[Json] = []
  for l in lines {
    out.push(Json::string(l))
  }
  Json::array(out)
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
  paren == 0 && brack == 0 && brace == 0
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

  let code = @jupyter.jstr_field(msg.content, "code")
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
  
  run_cell(k, code, fn(ok, ename, evalue, traceback, duration_ms) {
    let meta = @jupyter.obj([
      ("moonjupyter", @jupyter.obj([("duration_ms", Json::number(duration_ms.to_double()))])),
    ])
    if ok {
      log_kernel(k, LogLevel::Info, "cell \{count} executed successfully in \{duration_ms}ms")
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
      log_kernel(k, LogLevel::Error, "cell \{count} failed: \{ename} - \{evalue}")
      reply(
        k,
        peer,
        "execute_reply",
        @jupyter.obj([
          ("status", Json::string("error")),
          ("execution_count", Json::number(count.to_double())),
          ("ename", Json::string(ename)),
          ("evalue", Json::string(evalue)),
          ("traceback", lines_json(traceback)),
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
  "fn", "let", "mut", "if", "else", "match", "while", "for", "in", "break",
  "continue", "return", "struct", "enum", "trait", "impl", "pub", "priv",
  "true", "false", "print", "println", "to_string", "to_int", "to_double",
  "length", "abs", "min", "max", "sqrt",
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
fn complete_token(
  code : String,
  cursor_pos : Int,
) -> (Int, Int, Array[String]) {
  let end = if cursor_pos > code.length() { code.length() } else { cursor_pos }
  let mut start = end
  while start > 0 && is_ident_char(code[start - 1]) {
    start = start - 1
  }
  let token = @bx.sslice(code, start, end)
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
/// A cell that produces no output for this long is considered hung and is
/// killed (covers infinite loops; compile time is well under this).
const CELL_TIMEOUT_MS : Int = 60000

///|
/// Execute a cell in a temporary MoonBit project via `moon run`. `on_done`
/// receives (success, ename, evalue, traceback, wall-time in ms).
fn run_cell(
  k : Kernel,
  code : String,
  on_done : (Bool, String, String, String, Int) -> Unit,
) -> Unit {
  let dir = @nodeff.mkdtemp()
  @nodeff.write_text(
    dir + "/moon.mod.json",
    "{\"name\": \"cell/exec\", \"version\": \"0.1.0\"}",
  )
  @nodeff.write_text(dir + "/src/main/moon.pkg.json", "{\"is-main\": true}")
  let src = if contains_main_fn(code) {
    code
  } else {
    "fn main {\n  " + code + "\n}\n"
  }
  @nodeff.write_text(dir + "/src/main/main.mbt", src)
  let started = @nodeff.elapsed_ms()
  k.run_seq = k.run_seq + 1
  let my_seq = k.run_seq
  k.cell_pid = -1
  k.cell_interrupted = false
  let mut out_buf = ""
  let mut err_buf = ""
  @nodeff.spawn_process(
    dir,
    "moon run src/main",
    fn(chunk) { out_buf = stream_lines(k, out_buf + chunk, "stdout") },
    fn(chunk) { err_buf = stream_lines(k, err_buf + chunk, "stderr") },
    fn(exit_code) {
      if k.cell_pid < 0 {
        return // aborted; timeout/interrupt path already reported
      }
      k.cell_pid = -1
      let duration_ms = @nodeff.elapsed_ms() - started
      k.metrics.execution_time["exec-\{k.exec_count}"] = duration_ms
      flush_pending(k, out_buf, "stdout")
      flush_pending(k, err_buf, "stderr")
      if k.cell_interrupted {
        k.metrics.error_count = k.metrics.error_count + 1
        log_kernel(k, LogLevel::Warn, "cell \{k.exec_count} interrupted")
        on_done(false, "KeyboardInterrupt", "execution interrupted", "", duration_ms)
      } else if exit_code == 0 {
        on_done(true, "", "", "", duration_ms)
      } else {
        k.metrics.error_count = k.metrics.error_count + 1
        on_done(
          false,
          "MoonBitError",
          "cell exited with code \{exit_code}",
          last_lines(err_buf, 8),
          duration_ms,
        )
      }
      @nodeff.rmrf(dir)
    },
    fn(pid) { k.cell_pid = pid },
  )
  @nodeff.later(CELL_TIMEOUT_MS, fn() {
    if k.run_seq == my_seq && k.cell_pid > 0 {
      log_kernel(
        k,
        LogLevel::Warn,
        "cell \{k.exec_count} exceeded \{CELL_TIMEOUT_MS / 1000}s, killing pid \{k.cell_pid}",
      )
      @nodeff.kill_tree(k.cell_pid)
      k.cell_pid = -1
      let duration_ms = @nodeff.elapsed_ms() - started
      k.metrics.execution_time["exec-\{k.exec_count}"] = duration_ms
      k.metrics.error_count = k.metrics.error_count + 1
      @nodeff.rmrf(dir)
      on_done(
        false,
        "CellTimeout",
        "cell exceeded \{CELL_TIMEOUT_MS / 1000}s and was terminated",
        "",
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
/// Forward complete lines of an output stream to iopub as `stream` messages.
/// Returns the trailing partial line (kept until more output arrives).
fn stream_lines(k : Kernel, buf : String, name : String) -> String {
  let lines = buf.split("\n")
    |> Iter::to_array
    |> Array::map(fn(v) { v.to_owned() })
  let n = lines.length()
  for i = 0; i < n - 1; i = i + 1 {
    if lines[i] != "" {
      publish(
        k,
        "stream",
        @jupyter.obj([
          ("name", Json::string(name)),
          ("text", Json::string(lines[i] + "\n")),
        ]),
      )
    }
  }
  lines[n - 1]
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
fn last_lines(buf : String, count : Int) -> String {
  let lines = buf.split("\n")
    |> Iter::to_array
    |> Array::map(fn(v) { v.to_owned() })
  let start = if lines.length() > count { lines.length() - count } else { 0 }
  let sel : Array[String] = []
  for i = start; i < lines.length(); i = i + 1 {
    sel.push(lines[i])
  }
  sel.join("\n")
}

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
  log_kernel(k, LogLevel::Debug, "cleanup: checked \{k.peer_stats.length()} peers")
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
    (k.metrics.error_count.to_double() / k.metrics.message_count.to_double()) * 100.0
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
