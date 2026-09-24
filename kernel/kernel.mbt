/// Kernel session orchestration: binds the five Jupyter channels over the
/// ZMTP transport and dispatches requests.
///
/// Channel wiring (kernel side):
///   shell   ROUTER  (client: REQ/DEALER)   - code execution requests
///   control ROUTER  (client: DEALER/REQ)   - control-plane requests
///   stdin   ROUTER  (client: DEALER)       - user I/O (stub)
///   iopub   PUB                            - output broadcast
///   hb      REP     (client: REQ)          - heartbeat echo

/// A connected peer on a single ZMTP socket (keyed by TCP connection id).
struct Peer {
  id : Int
  session : @zmtp.ZmtpSession
  mut identities : Array[Bytes]
}

struct Channel {
  name : String
  socket_type : String
  peers : Map[Int, Peer]
}

struct Kernel {
  key : String
  session_id : String
  username : String
  mut exec_count : Int  /// incremented on each execute_reply (Jupyter spec §4.3)
  mut iopub : Peer?
  mut parent : Json?
}

pub fn run_kernel(conn : @jupyter.ConnectionInfo) -> Unit {
  // Per-kernel session: one shell ROUTER, one iopub PUB, one REP hb, ...
  let kernel : Kernel = {
    key: conn.key,
    session_id: @jupyter.uuid_v4_from(@nodeff.random_bytes(16)),
    username: @nodeff.username(),
    exec_count: 0,
    iopub: None,
    parent: None,
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
  @nodeff.eprint(
    "[moonjupyter] kernel \{kernel.session_id} listening on \{conn.ip} " +
    "(shell=\{conn.shell_port} iopub=\{conn.iopub_port} stdin=\{conn.stdin_port} " +
    "control=\{conn.control_port} hb=\{conn.hb_port})",
  )
}

fn Channel::new(name : String, socket_type : String) -> Channel {
  let peers : Map[Int, Peer] = Map([])
  { name, socket_type, peers }
}

fn bind_channel(k : Kernel, ch : Channel, ip : String, port : Int) -> Unit {
  @nodeff.tcp_server(
    ip,
    port,
    fn(id) {
      let peer : Peer = { id, session: @zmtp.ZmtpSession::new(), identities: [] }
      ch.peers[id] = peer
      if ch.name == "iopub" {
        k.iopub = Some(peer)
      }
    },
    fn(id, data) {
      match ch.peers.get(id) {
        Some(peer) => on_peer_data(k, ch, peer, data)
        None => ()
      }
    },
    fn(id) {
      let _ = ch.peers.remove(id)
      if ch.name == "iopub" {
        k.iopub = None
      }
    },
  )
}

fn send_raw(peer : Peer, data : Bytes) -> Unit {
  @nodeff.tcp_send(peer.id, data)
}

fn on_peer_data(k : Kernel, ch : Channel, peer : Peer, data : Bytes) -> Unit {
  let events = peer.session.feed(data)
  for ev in events {
    match ev {
      @zmtp.PeerGreeting => send_raw(peer, @zmtp.ready_command(ch.socket_type))
      @zmtp.PeerReady(_) => ()
      @zmtp.PingReceived(context) => send_raw(peer, @zmtp.pong_command(context))
      @zmtp.Error(reason) => @nodeff.eprint("[moonjupyter] \{ch.name}: \{reason}")
      @zmtp.Message(frames) => handle_message(k, ch, peer, frames)
    }
  }
}

fn handle_message(k : Kernel, ch : Channel, peer : Peer, frames : Array[Bytes]) -> Unit {
  if ch.name == "iopub" {
    // SUB subscription frames; nothing to do.
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
    Err(e) => @nodeff.eprint("[moonjupyter] \{ch.name}: \{e}")
    Ok(msg) => {
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

fn dispatch(k : Kernel, peer : Peer, msg : @jupyter.JMsg) -> Unit {
  match msg.msg_type {
    "kernel_info_request" => reply(k, peer, "kernel_info_reply", kernel_info(), msg)
    "execute_request" => handle_execute(k, peer, msg)
    "is_complete_request" =>
      reply(
        k,
        peer,
        "is_complete_reply",
        @jupyter.obj([("status", Json::string("complete"))]),
        msg,
      )
    "complete_request" =>
      reply(
        k,
        peer,
        "complete_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("matches", Json::array([])),
          (
            "cursor_start",
            Json::number(@jupyter.jint_field(msg.content, "cursor_pos").to_double()),
          ),
          (
            "cursor_end",
            Json::number(@jupyter.jint_field(msg.content, "cursor_pos").to_double()),
          ),
          ("metadata", @jupyter.obj([])),
        ]),
        msg,
      )
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
        @jupyter.obj([("status", Json::string("ok")), ("history", Json::array([]))]),
        msg,
      )
    "shutdown_request" => {
      reply(
        k,
        peer,
        "shutdown_reply",
        @jupyter.obj([
          ("status", Json::string("ok")),
          ("restart", Json::boolean(false)),
        ]),
        msg,
      )
      publish(k, "status", @jupyter.obj([("execution_state", Json::string("idle"))]))
      @nodeff.later(300, fn() {
        @nodeff.exit(0)
      })
    }
    "comm_open" | "comm_msg" | "comm_close" | "interrupt_request" => ()
    other => @nodeff.eprint("[moonjupyter] unhandled msg_type: \{other}")
  }
}

fn kernel_info() -> Json {
  @jupyter.obj([
    ("status", Json::string("ok")),
    ("protocol_version", Json::string("5.4")),
    ("implementation", Json::string("moonjupyter")),
    ("implementation_version", Json::string("0.1.0")),
    (
      "language_info",
      @jupyter.obj([
        ("name", Json::string("moonbit")),
        ("version", Json::string("dev")),
        ("mimetype", Json::string("text/x-moonbit")),
        ("file_extension", Json::string(".mbt")),
      ]),
    ),
    (
      "banner",
      Json::string(
        "MoonBit kernel (moonjupyter) 0.1.0 — each cell runs via `moon run` in a scratch project",
      ),
    ),
    ("help_links", Json::array([])),
  ])
}

fn handle_execute(k : Kernel, peer : Peer, msg : @jupyter.JMsg) -> Unit {
  let count = k.exec_count + 1
  k.exec_count = count
  k.parent = Some(msg.header)
  let code = @jupyter.jstr_field(msg.content, "code")
  publish(k, "status", @jupyter.obj([("execution_state", Json::string("busy"))]))
  publish(
    k,
    "execute_input",
    @jupyter.obj([
      ("code", Json::string(code)),
      ("execution_count", Json::number(count.to_double())),
    ]),
  )
  run_cell(k, code, fn(ok, ename, evalue, traceback) {
    if ok {
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
    } else {
      reply(
        k,
        peer,
        "execute_reply",
        @jupyter.obj([
          ("status", Json::string("error")),
          ("execution_count", Json::number(count.to_double())),
          ("ename", Json::string(ename)),
          ("evalue", Json::string(evalue)),
          ("traceback", Json::array([Json::string(traceback)])),
        ]),
        msg,
      )
    }
    publish(k, "status", @jupyter.obj([("execution_state", Json::string("idle"))]))
  })
}

/// Execute a cell in a temporary MoonBit project via `moon run`.
fn run_cell(k : Kernel, code : String, on_done : (Bool, String, String, String) -> Unit) -> Unit {
  let dir = @nodeff.mkdtemp()
  @nodeff.write_text(dir + "/moon.mod.json", "{\"name\": \"cell/exec\", \"version\": \"0.1.0\"}")
  @nodeff.write_text(dir + "/src/main/moon.pkg.json", "{\"is-main\": true}")
  let src = if contains_main_fn(code) {
    code
  } else {
    "fn main {\n  " + code + "\n}\n"
  }
  @nodeff.write_text(dir + "/src/main/main.mbt", src)
  let mut out_buf = ""
  let mut err_buf = ""
  @nodeff.spawn_process(
    dir,
    "moon run src/main",
    fn(chunk) {
      out_buf = stream_lines(k, out_buf + chunk, "stdout")
    },
    fn(chunk) {
      err_buf = stream_lines(k, err_buf + chunk, "stderr")
    },
    fn(exit_code) {
      flush_pending(k, out_buf, "stdout")
      flush_pending(k, err_buf, "stderr")
      if exit_code == 0 {
        on_done(true, "", "", "")
      } else {
        on_done(false, "MoonBitError", "cell exited with code \{exit_code}", last_lines(err_buf, 8))
      }
      @nodeff.rmrf(dir)
    },
  )
}

/// MVP heuristic: cells that declare their own `fn main` run unmodified;
/// everything else is wrapped in a generated entry point.
fn contains_main_fn(code : String) -> Bool {
  code.contains("fn main")
}

/// Forward complete lines of an output stream to iopub as `stream` messages.
/// Returns the trailing partial line (kept until more output arrives).
fn stream_lines(k : Kernel, buf : String, name : String) -> String {
  let lines = buf.split("\n") |> Iter::to_array |> Array::map(fn(v) { v.to_owned() })
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

fn last_lines(buf : String, count : Int) -> String {
  let lines = buf.split("\n") |> Iter::to_array |> Array::map(fn(v) { v.to_owned() })
  let start = if lines.length() > count {
    lines.length() - count
  } else {
    0
  }
  let sel : Array[String] = []
  for i = start; i < lines.length(); i = i + 1 {
    sel.push(lines[i])
  }
  sel.join("\n")
}

/// Send a reply on the same channel, routed back through the identities the
/// request arrived with.
fn reply(k : Kernel, peer : Peer, msg_type : String, content : Json, msg : @jupyter.JMsg) -> Unit {
  let header = @jupyter.new_header(
    k.session_id,
    k.username,
    @nodeff.iso_now(),
    msg_type,
    @jupyter.uuid_v4_from(@nodeff.random_bytes(16)),
  )
  let frames = @jupyter.build_frames(k.key, header, msg.header, @jupyter.obj([]), content)
  let all : Array[Bytes] = []
  for f in peer.identities {
    all.push(f)
  }
  for f in frames {
    all.push(f)
  }
  send_raw(peer, @zmtp.encode_message(all))
}

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
      let frames = @jupyter.build_frames(k.key, header, parent, @jupyter.obj([]), content)
      send_raw(peer, @zmtp.encode_message(frames))
    }
  }
}
