/// moonjupyter kernel entry point.
///
/// Usage (via launcher.mjs): node launcher.mjs <connection-file>
/// The connection file path is the last program argument.

fn main {
  let path = @nodeff.last_arg()
  if path == "" || path.contains("main.js") {
    @nodeff.eprint("usage: moonjupyter -f <connection-file>")
    @nodeff.exit(1)
  }
  match @jupyter.parse_connection(@nodeff.read_text(path)) {
    Err(e) => {
      @nodeff.eprint("[moonjupyter] invalid connection file \{path}: \{e}")
      @nodeff.exit(1)
    }
    Ok(conn) => @kernel.run_kernel(conn)
  }
}
