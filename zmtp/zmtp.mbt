/// ZMTP 3.1 protocol core (NULL security mechanism): greeting, handshake
/// commands, frame codec and a parse-side session state machine.
///
/// The session is transport-agnostic: the host feeds raw TCP bytes into
/// `ZmtpSession::feed` and receives events; outgoing wire bytes are produced
/// by the pure encoding functions below.

/// Frame flag bits (ZMTP 3.1 spec, section 3.1).
let FLAG_MORE : Int = 0x01   /// more frames follow before message boundary
let FLAG_LONG : Int = 0x02   /// long 64-bit size encoding instead of 8-bit
let FLAG_COMMAND : Int = 0x04 /// this frame is a COMMAND, not a MESSAGE

/// Events produced by feeding wire bytes into a session.
pub(all) enum ZmtpEvent {
  /// The peer greeting was received; the host should now send `ready_command`.
  PeerGreeting
  /// The peer completed the handshake with the given socket type.
  PeerReady(String)
  /// A complete multi-frame message was received.
  Message(Array[Bytes])
  /// The peer sent a PING; reply with `pong_command`.
  PingReceived(Bytes)
  /// A protocol error occurred; the session is closed.
  Error(String)
}

enum ZState {
  AwGreet
  AwReady
  Est
  Closed
} derive(Eq)

/// One connection's parse-side state. Construct with `ZmtpSession::new`.
pub struct ZmtpSession {
  mut state : ZState
  mut buf : Array[Byte]
  mut pos : Int
  mut pending : Array[Bytes]
}

pub fn ZmtpSession::new() -> ZmtpSession {
  { state: AwGreet, buf: [], pos: 0, pending: [] }
}

struct ZFrame {
  flags : Int
  body : Array[Byte]
}

/// Build the 64-byte ZMTP greeting.
/// Magic `0xFF` + 8 zero bytes + `0x7F` + version + mechanism + as-server flag.
pub fn greeting_bytes(as_server : Bool) -> Bytes {
  let out : Array[Byte] = []
  out.push(@bx.u8(0xFF))       /// 0xFF magic byte
  for _i = 0; _i < 8; _i = _i + 1 {
    out.push(@bx.u8(0))       /// 8-byte long size placeholder
  }
  out.push(@bx.u8(0x7F))       /// version-present flag
  out.push(@bx.u8(3))         /// version major
  out.push(@bx.u8(1))         /// version minor
  // mechanism "NULL" padded to 20 bytes
  push_str(out, "NULL")
  for _i = 0; _i < 16; _i = _i + 1 {
    out.push(@bx.u8(0))
  }
  out.push(@bx.u8(if as_server {
    1
  } else {
    0
  }))
  for _i = 0; _i < 31; _i = _i + 1 {
    out.push(@bx.u8(0))
  }
  Bytes::from_array(out)
}

/// Build the READY handshake command announcing our socket type.
/// READY command: FRAME(flags=COMMAND, body="READY\x00\x00\x00\x05Socket-Type<4-byte-len><type>")
pub fn ready_command(socket_type : String) -> Bytes {
  let body : Array[Byte] = []
  body.push(@bx.u8(5))
  push_str(body, "READY")
  // metadata: key "Socket-Type"
  body.push(@bx.u8(11))
  push_str(body, "Socket-Type")
  let tlen = socket_type.length()
  body.push(@bx.u8((tlen >> 24) & 0xFF))
  body.push(@bx.u8((tlen >> 16) & 0xFF))
  body.push(@bx.u8((tlen >> 8) & 0xFF))
  body.push(@bx.u8(tlen & 0xFF))
  push_str(body, socket_type)
  frame_bytes(Bytes::from_array(body), false, true)
}

/// Build a PONG command replying to a PING with the same context.
pub fn pong_command(context : Bytes) -> Bytes {
  let body : Array[Byte] = []
  body.push(@bx.u8(4))
  push_str(body, "PONG")
  for i = 0; i < context.length(); i = i + 1 {
    body.push(context[i])
  }
  frame_bytes(Bytes::from_array(body), false, true)
}

/// Encode a multi-frame message into wire bytes (MORE on all but last frame).
pub fn encode_message(parts : Array[Bytes]) -> Bytes {
  let frames : Array[Bytes] = []
  let n = parts.length()
  for i = 0; i < n; i = i + 1 {
    frames.push(frame_bytes(parts[i], i < n - 1, false))
  }
  @bx.concat(frames)
}

fn push_str(out : Array[Byte], s : String) -> Unit {
  for i = 0; i < s.length(); i = i + 1 {
    out.push(@bx.u8(s.code_unit_at(i).to_int()))
  }
}

fn frame_bytes(body : Bytes, more : Bool, command : Bool) -> Bytes {
  let out : Array[Byte] = []
  let mut flags = 0
  if more {
    flags = flags | FLAG_MORE
  }
  if command {
    flags = flags | FLAG_COMMAND
  }
  let len = body.length()
  if len > 255 {
    flags = flags | FLAG_LONG
    out.push(@bx.u8(flags))
    for shift = 56; shift >= 0; shift = shift - 8 {
      out.push(@bx.u8((len >> shift) & 0xFF))
    }
  } else {
    out.push(@bx.u8(flags))
    out.push(@bx.u8(len))
  }
  for i = 0; i < len; i = i + 1 {
    out.push(body[i])
  }
  Bytes::from_array(out)
}

pub fn ZmtpSession::feed(self : ZmtpSession, data : Bytes) -> Array[ZmtpEvent] {
  for i = 0; i < data.length(); i = i + 1 {
    self.buf.push(data[i])
  }
  let events : Array[ZmtpEvent] = []
  let mut progressed = true
  while progressed && self.state != Closed {
    progressed = false
    if self.state == AwGreet && self.buf.length() - self.pos >= 64 {
      match self.try_greeting() {
        Some(mech) => {
          progressed = true
          if mech == "NULL" {
            self.state = AwReady
            events.push(PeerGreeting)
          } else {
            self.state = Closed
            events.push(Error("unsupported security mechanism: \{mech}"))
          }
        }
        None => {
          self.state = Closed
          events.push(Error("invalid ZMTP greeting"))
        }
      }
    } else if self.state == AwReady {
      match self.try_frame() {
        Some(f) => {
          progressed = true
          if f.flags & FLAG_COMMAND != 0 {
            match command_name(f.body) {
              Some("READY") => {
                self.state = Est
                events.push(PeerReady(ready_socket_type(f.body)))
              }
              Some(_) => {
                self.state = Closed
                events.push(Error("unexpected command during handshake"))
              }
              None => {
                self.state = Closed
                events.push(Error("malformed handshake command"))
              }
            }
          } else {
            self.state = Closed
            events.push(Error("data frame before READY"))
          }
        }
        None => ()
      }
    } else if self.state == Est {
      match self.try_frame() {
        Some(f) => {
          progressed = true
          if f.flags & FLAG_COMMAND != 0 {
            match command_name(f.body) {
              Some("PING") => events.push(PingReceived(ping_context(f.body))),
              _ => () // PONG/HICCUP/ERROR etc: ignore
            }
          } else {
            self.pending.push(Bytes::from_array(f.body))
            if f.flags & FLAG_MORE == 0 {
              events.push(Message(self.pending))
              self.pending = []
            }
          }
        }
        None => ()
      }
    } else {
      ()
    }
  }
  // compact consumed bytes
  if self.pos > 0 {
    let rest : Array[Byte] = []
    for i = self.pos; i < self.buf.length(); i = i + 1 {
      rest.push(self.buf[i])
    }
    self.buf = rest
    self.pos = 0
  }
  events
}

fn ZmtpSession::try_greeting(self : ZmtpSession) -> String? {
  let base = self.pos
  if self.buf[base].to_int() != 0xFF || self.buf[base + 9].to_int() != 0x7F {
    return None
  }
  for i = 1; i < 9; i = i + 1 {
    if self.buf[base + i].to_int() != 0 {
      return None
    }
  }
  if self.buf[base + 10].to_int() < 3 {
    return None
  }
  // mechanism: bytes 12..32, zero-padded
  let mut mech = ""
  for i = 12; i < 32; i = i + 1 {
    let c = self.buf[base + i].to_int()
    if c != 0 {
      mech = mech + @bx.char_str(c)
    }
  }
  self.pos = base + 64
  Some(mech)
}

fn ZmtpSession::try_frame(self : ZmtpSession) -> ZFrame? {
  let base = self.pos
  let avail = self.buf.length() - base
  if avail < 1 {
    return None
  }
  let flags = self.buf[base].to_int()
  let mut hdr = 2
  let mut size = 0
  if flags & FLAG_LONG != 0 {
    if avail < 10 {
      return None
    }
    for i = 0; i < 8; i = i + 1 {
      size = (size << 8) | self.buf[base + 2 + i].to_int()
    }
    hdr = 10
  } else {
    size = self.buf[base + 1].to_int()
  }
  if avail < hdr + size {
    return None
  }
  let body : Array[Byte] = []
  for i = 0; i < size; i = i + 1 {
    body.push(self.buf[base + hdr + i])
  }
  self.pos = base + hdr + size
  Some({ flags, body })
}

/// The command name is a length-prefixed string at the start of the body.
fn command_name(body : Array[Byte]) -> String? {
  if body.length() < 1 {
    return None
  }
  let n = body[0].to_int()
  if body.length() < 1 + n {
    return None
  }
  let mut s = ""
  for i = 1; i < 1 + n; i = i + 1 {
    s = s + @bx.char_str(body[i].to_int())
  }
  Some(s)
}

/// Extract the Socket-Type property from a READY body.
fn ready_socket_type(body : Array[Byte]) -> String {
  let mut i = 1 + 5 // skip name-length + "READY"
  let n = body.length()
  while i < n {
    if i + 1 > n {
      break
    }
    let klen = body[i].to_int()
    if i + 1 + klen + 4 > n {
      break
    }
    let mut key = ""
    for j = 0; j < klen; j = j + 1 {
      key = key + @bx.char_str(body[i + 1 + j].to_int())
    }
    i = i + 1 + klen
    let vlen = (body[i].to_int() << 24)
      | (body[i + 1].to_int() << 16)
      | (body[i + 2].to_int() << 8)
      | body[i + 3].to_int()
    i = i + 4
    if i + vlen > n {
      break
    }
    let mut value = ""
    for j = 0; j < vlen; j = j + 1 {
      value = value + @bx.char_str(body[i + j].to_int())
    }
    i = i + vlen
    if key == "Socket-Type" {
      return value
    }
  }
  ""
}

/// PING body: name-length + "PING" + 2-byte TTL + context.
fn ping_context(body : Array[Byte]) -> Bytes {
  if body.length() > 7 {
    let out : Array[Byte] = []
    for i = 7; i < body.length(); i = i + 1 {
      out.push(body[i])
    }
    Bytes::from_array(out)
  } else {
    b""
  }
}

test "handshake" {
  let s = ZmtpSession::new()
  let events = s.feed(greeting_bytes(true))
  assert_eq!(events.length(), 1)
  match events[0] {
    PeerGreeting => ()
    _ => assert_true!(false)
  }
  let events2 = s.feed(ready_command("REQ"))
  match events2[0] {
    PeerReady(t) => assert_eq!(t, "REQ")
    _ => assert_true!(false)
  }
}

test "message roundtrip" {
  let s = ZmtpSession::new()
  let _ = s.feed(greeting_bytes(true))
  let _ = s.feed(ready_command("DEALER"))
  let wire = encode_message([b"one", b"two", b"three"])
  let events = s.feed(wire)
  match events[0] {
    Message(frames) => {
      assert_eq!(frames.length(), 3)
      assert_true!(@bx.bytes_equal(frames[1], b"two"))
    }
    _ => assert_true!(false)
  }
}

test "byte-by-byte feed and long frames" {
  let s = ZmtpSession::new()
  let g = greeting_bytes(true)
  let mut last : Array[ZmtpEvent] = []
  for i = 0; i < g.length(); i = i + 1 {
    last = s.feed(@bx.slice(g, i, i + 1))
  }
  match last[0] {
    PeerGreeting => ()
    _ => assert_true!(false)
  }
  let r = ready_command("PUB")
  let mut last2 : Array[ZmtpEvent] = []
  for i = 0; i < r.length(); i = i + 1 {
    last2 = s.feed(@bx.slice(r, i, i + 1))
  }
  match last2[0] {
    PeerReady(t) => assert_eq!(t, "PUB")
    _ => assert_true!(false)
  }
  // long frame (>255 bytes)
  let big : Array[Byte] = []
  for i = 0; i < 300; i = i + 1 {
    big.push(@bx.u8(i % 251))
  }
  let events = s.feed(encode_message([Bytes::from_array(big)]))
  match events[0] {
    Message(frames) => {
      assert_eq!(frames.length(), 1)
      assert_eq!(frames[0].length(), 300)
    }
    _ => assert_true!(false)
  }
}
