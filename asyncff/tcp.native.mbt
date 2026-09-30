/// Native async transport for moonjupyter (roadmap item 2, milestone 1).
///
/// This package proves out the moonbitlang/async primitives the kernel
/// needs before the orchestration layer migrates off the Node.js FFI:
/// TCP server + client roundtrip over the native (IOCP/epoll/kqueue)
/// backends. Only compiled for native/llvm targets; the JS backend has
/// no TCP support in moonbitlang/async, see tcp.js.mbt for the stub.

///|
/// Serve TCP echo on an already-parsed address forever (test helper:
/// lets the caller read back the auto-assigned port via `server.addr`).
pub async fn echo_serve_forever_addr(server : @socket.TcpServer) -> Unit raise {
  server.run_forever((conn, _peer) => {
    match conn.read_some() {
      Some(data) => conn.write(data)
      None => ()
    }
  })
}

///|
/// One client roundtrip: connect, send `payload`, return the echo reply.
pub async fn roundtrip(addr : String, payload : Bytes) -> Bytes raise {
  roundtrip_addr(@socket.Addr::parse(addr), payload)
}

///|
/// Client roundtrip against an already-parsed address.
pub async fn roundtrip_addr(
  addr : @socket.Addr,
  payload : Bytes,
) -> Bytes raise {
  let conn = @socket.Tcp::connect(addr)
  conn.write(payload)
  match conn.read_some() {
    Some(data) => data
    None => b""
  }
}
