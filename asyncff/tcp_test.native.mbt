/// Native-only test: an in-process TCP echo server plus a client
/// roundtrip, exercising the same primitives the kernel transport will
/// use (listen/accept/read/write) on IOCP/epoll/kqueue.

///|
async test "echo roundtrip over native async socket" {
  @async.with_task_group(root => {
    let server = @socket.TcpServer(
      @socket.Addr::parse("127.0.0.1:0"),
      reuse_addr=true,
    )
    // port 0: the OS picks a free port; read it back from the server
    let addr = server.addr
    root.spawn_bg(() => echo_serve_forever_addr(server))
    let reply = roundtrip_addr(addr, b"ping-async")
    assert_eq(reply, b"ping-async")
  }) catch {
    err => raise err
  }
}
