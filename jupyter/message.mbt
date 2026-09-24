/// Jupyter kernel wire protocol (v5.x): message envelopes, HMAC-SHA256
/// signing, and the connection-file format.
///
/// Signing is computed over the four uncompressed JSON parts, matching
/// jupyter_client. Incoming parts may be zlib-compressed (detected by the
/// 0x78 zlib header byte) and are decompressed through an injected `inflate`
/// function so this package stays transport/backend independent.

/// The envelope delimiter shared by every Jupyter message.
pub let DELIMITER : Bytes = b"<IDS|MSG>"

/// Jupyter wire protocol version (currently v5.4).
let PROTOCOL_VERSION : String = "5.4"

/// A parsed Jupyter message. `header`/`parent`/`metadata`/`content` are kept
/// as raw JSON so replies can echo the parent header verbatim.
pub(all) struct JMsg {
  identities : Array[Bytes]
  header : Json
  parent : Json
  metadata : Json
  content : Json
  msg_type : String
}

/// A parsed kernel connection file (kernel-<id>.json).
/// Matches Jupyter Client's `connection_info` dict exactly.
pub(all) struct ConnectionInfo {
  ip : String
  shell_port : Int
  control_port : Int
  stdin_port : Int
  iopub_port : Int
  hb_port : Int
  key : String
}

/// ---- JSON helpers ----

pub fn jget(j : Json, k : String) -> Json? {
  match j {
    Json::Object(m) => m.get(k)
    _ => None
  }
}

pub fn jstr(j : Json) -> String {
  match j {
    Json::String(s) => s
    _ => ""
  }
}

pub fn jstr_field(j : Json, k : String) -> String {
  match jget(j, k) {
    Some(v) => jstr(v)
    None => ""
  }
}

pub fn jint_field(j : Json, k : String) -> Int {
  match jget(j, k) {
    Some(Json::Number(d)) => d.to_int()
    _ => 0
  }
}

/// Build a JSON object from key/value pairs.
pub fn obj(pairs : Array[(String, Json)]) -> Json {
  let m : Map[String, Json] = Map([])
  for p in pairs {
    m[p.0] = p.1
  }
  Json::object(m)
}

/// ---- identity / time ----

/// Format 16 random bytes as a v4 UUID string (version/variant bits set).
pub fn uuid_v4_from(b : Bytes) -> String {
  let out : Array[Byte] = []
  for i = 0; i < b.length() && i < 16; i = i + 1 {
    out.push(b[i])
  }
  while out.length() < 16 {
    out.push(@bx.u8(0))
  }
  out[6] = @bx.u8((out[6].to_int() & 0x0F) | 0x40)
  out[8] = @bx.u8((out[8].to_int() & 0x3F) | 0x80)
  let hex = @bx.hex_encode(Bytes::from_array(out))
  // 8-4-4-4-12
  "\{@bx.slice(hex, 0, 8)}-\{@bx.slice(hex, 8, 12)}-\{@bx.slice(hex, 12, 16)}-\{@bx.slice(hex, 16, 20)}-\{@bx.slice(hex, 20, 32)}"
}

/// ---- signing ----

pub fn sign(key : String, header_b : Bytes, parent_b : Bytes, meta_b : Bytes, content_b : Bytes) -> String {
  if key == "" {
    return ""
  }
  let mac = @sha256.hmac_sha256(
    @bx.utf8_encode(key),
    @bx.concat([header_b, parent_b, meta_b, content_b]),
  )
  @bx.hex_encode(mac)
}

/// ---- outgoing messages ----

/// Build a message header (new msg id, protocol 5.4).
pub fn new_header(session_id : String, username : String, date : String, msg_type : String, msg_id : String) -> Json {
  obj([
    ("msg_id", Json::string(msg_id)),
    ("session", Json::string(session_id)),
    ("username", Json::string(username)),
    ("date", Json::string(date)),
    ("msg_type", Json::string(msg_type)),
    ("version", Json::string(PROTOCOL_VERSION)),
  ])
}

/// Serialize header/parent/metadata/content into the 6 wire frames that
/// follow the identity frames: [DELIMITER, sig, header, parent, meta, content].
/// Outgoing parts are sent uncompressed; the signature is over the exact
/// uncompressed bytes, matching jupyter_client's verification.
pub fn build_frames(
  key : String,
  header : Json,
  parent : Json,
  metadata : Json,
  content : Json,
) -> Array[Bytes] {
  let header_b = @bx.utf8_encode(@json.stringify(header))
  let parent_b = @bx.utf8_encode(@json.stringify(parent))
  let meta_b = @bx.utf8_encode(@json.stringify(metadata))
  let content_b = @bx.utf8_encode(@json.stringify(content))
  let sig = sign(key, header_b, parent_b, meta_b, content_b)
  [
    DELIMITER,
    @bx.utf8_encode(sig),
    header_b,
    parent_b,
    meta_b,
    content_b,
  ]
}

/// ---- incoming messages ----

fn maybe_inflate(b : Bytes, inflate : (Bytes) -> Bytes) -> Bytes {
  if b.length() > 0 && b[0].to_int() == 0x78 {
    inflate(b)
  } else {
    b
  }
}

/// Parse a full ROUTER-side message: [identities..., DELIMITER, sig,
/// header, parent, metadata, content]. `inflate` decompresses zlib parts.
pub fn parse_message(frames : Array[Bytes], key : String, inflate : (Bytes) -> Bytes) -> Result[JMsg, String] {
  let mut di = -1
  for i = 0; i < frames.length(); i = i + 1 {
    if @bx.bytes_equal(frames[i], DELIMITER) {
      di = i
      break
    }
  }
  if di < 0 {
    return Err("missing <IDS|MSG> delimiter")
  }
  if frames.length() < di + 6 {
    return Err("truncated message: expected 5 frames after delimiter")
  }
  let identities : Array[Bytes] = []
  for i = 0; i < di; i = i + 1 {
    identities.push(frames[i])
  }
  let header_b = maybe_inflate(frames[di + 2], inflate)
  let parent_b = maybe_inflate(frames[di + 3], inflate)
  let meta_b = maybe_inflate(frames[di + 4], inflate)
  let content_b = maybe_inflate(frames[di + 5], inflate)
  let sig = @bx.utf8_decode(frames[di + 1])
  if key != "" {
    let expected = sign(key, header_b, parent_b, meta_b, content_b)
    if sig != expected {
      return Err("bad message signature")
    }
  }
  let header = @json.parse(@bx.utf8_decode(header_b))
  let parent = @json.parse(@bx.utf8_decode(parent_b))
  let metadata = @json.parse(@bx.utf8_decode(meta_b))
  let content = @json.parse(@bx.utf8_decode(content_b))
  Ok({
    identities,
    header,
    parent,
    metadata,
    content,
    msg_type: jstr_field(header, "msg_type"),
  })
}

/// ---- connection file ----

pub fn parse_connection(s : String) -> Result[ConnectionInfo, String] {
  let j = @json.parse(s)
  let ip = match jget(j, "ip") {
    Some(v) => jstr(v)
    None => "127.0.0.1"
  }
  let key = jstr_field(j, "key")
  let miss = fn(name : String) -> Result[ConnectionInfo, String] {
    Err("connection file missing field: \{name}")
  }
  let shell_port = match jget(j, "shell_port") {
    Some(Json::Number(d)) => d.to_int()
    Some(_) => return miss("shell_port")
    None => return miss("shell_port")
  }
  let control_port = match jget(j, "control_port") {
    Some(Json::Number(d)) => d.to_int()
    _ => return miss("control_port")
  }
  let stdin_port = match jget(j, "stdin_port") {
    Some(Json::Number(d)) => d.to_int()
    _ => return miss("stdin_port")
  }
  let iopub_port = match jget(j, "iopub_port") {
    Some(Json::Number(d)) => d.to_int()
    _ => return miss("iopub_port")
  }
  let hb_port = match jget(j, "hb_port") {
    Some(Json::Number(d)) => d.to_int()
    _ => return miss("hb_port")
  }
  Ok({ ip, shell_port, control_port, stdin_port, iopub_port, hb_port, key })
}

/// ---- tests ----

test "uuid format" {
  let b : Array[Byte] = []
  for i = 0; i < 16; i = i + 1 {
    b.push(@bx.u8(i))
  }
  let u = uuid_v4_from(Bytes::from_array(b))
  // 00000000-0004-0..-8...-............
  assert_eq!(u.length(), 36)
  assert_eq!(@bx.slice(u, 8, 9), "-")
  assert_eq!(@bx.slice(u, 13, 14), "-")
  assert_eq!(@bx.slice(u, 18, 19), "-")
  assert_eq!(@bx.slice(u, 23, 24), "-")
  assert_eq!(@bx.slice(u, 14, 15), "4")
}

test "build and parse roundtrip" {
  let header = new_header("sess", "user", "2026-01-01T00:00:00Z", "kernel_info_request", "id-1")
  let parent = obj([])
  let metadata = obj([])
  let content = obj([])
  let frames = build_frames("secret", header, parent, metadata, content)
  let full : Array[Bytes] = [b"identA", b""]
  for f in frames {
    full.push(f)
  }
  let noop = fn(b : Bytes) -> Bytes {
    b
  }
  match parse_message(full, "secret", noop) {
    Ok(msg) => {
      assert_eq!(msg.msg_type, "kernel_info_request")
      assert_eq!(msg.identities.length(), 2)
      assert_eq!(jstr_field(msg.header, "session"), "sess")
    }
    Err(e) => assert_true!(false)
  }
}

test "bad signature rejected" {
  let header = new_header("sess", "user", "2026-01-01T00:00:00Z", "execute_request", "id-2")
  let frames = build_frames("k1", header, obj([]), obj([]), obj([]))
  let full : Array[Bytes] = [b"i"]
  for f in frames {
    full.push(f)
  }
  let noop = fn(b : Bytes) -> Bytes {
    b
  }
  match parse_message(full, "other-key", noop) {
    Ok(_) => assert_true!(false)
    Err(_) => ()
  }
}

test "parse connection file" {
  let raw = "{\"shell_port\": 1, \"iopub_port\": 2, \"stdin_port\": 3, \"control_port\": 4, \"hb_port\": 5, \"ip\": \"127.0.0.1\", \"transport\": \"tcp\", \"signature_scheme\": \"hmac-sha256\", \"key\": \"a-key\"}"
  match parse_connection(raw) {
    Ok(c) => {
      assert_eq!(c.shell_port, 1)
      assert_eq!(c.hb_port, 5)
      assert_eq!(c.key, "a-key")
    }
    Err(_) => assert_true!(false)
  }
}
