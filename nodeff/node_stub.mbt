/// WASM/wasm-gc stubs for the Node.js FFI. nodeff is js-only by nature
/// (Node TCP/child_process/fs/crypto); these aborting stubs exist so the
/// module typechecks on the wasm backends (e.g. `moon publish` checks the
/// default target). The kernel itself is only ever run on the js backend
/// via Node.

///|
pub fn tcp_server(
  _host : String,
  _port : Int,
  _on_connect : (Int) -> Unit,
  _on_data : (Int, Bytes) -> Unit,
  _on_close : (Int) -> Unit,
) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn tcp_send(_id : Int, _data : Bytes) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn tcp_close(_id : Int) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn is_zlib(_b : Bytes) -> Bool {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn inflate(_b : Bytes) -> Bytes {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn random_bytes(_n : Int) -> Bytes {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn read_text(_path : String) -> String {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn write_text(_path : String, _content : String) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn mkdtemp() -> String {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn rmrf(_path : String) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn spawn_process(
  _cwd : String,
  _command : String,
  _on_out : (String) -> Unit,
  _on_err : (String) -> Unit,
  _on_exit : (Int) -> Unit,
  _on_pid : (Int) -> Unit,
) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn kill_tree(_pid : Int) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn iso_now() -> String {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn now_seconds() -> Int {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn username() -> String {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn last_arg() -> String {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn exit(_code : Int) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn eprint(_s : String) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn later(_ms : Int, _cb : () -> Unit) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn every(_ms : Int, _cb : () -> Unit) -> Unit {
  abort("nodeff requires the js backend (Node.js)")
}

///|
pub fn elapsed_ms() -> Int {
  abort("nodeff requires the js backend (Node.js)")
}
