"""Exercise the kernel through the real jupyter_client / pyzmq stack.

Run from the repository root with ``python tools/jupyter_client_smoke.py``.
Requires jupyter_client, pyzmq, Node.js, and the MoonBit CLI.
"""

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from jupyter_client import BlockingKernelClient
from jupyter_client.connect import write_connection_file


ROOT = Path(__file__).resolve().parents[1]


def main():
    with tempfile.TemporaryDirectory(prefix="moonjupyter-smoke-") as tmp:
        connection_file = str(Path(tmp) / "connection.json")
        write_connection_file(connection_file, ip="127.0.0.1", key=b"smoke-test-key")
        log_path = Path(tmp) / "kernel.log"
        client = BlockingKernelClient(connection_file=connection_file)
        client.load_connection_file()
        with log_path.open("w+", encoding="utf-8") as log:
            kernel = subprocess.Popen(
                ["node", "launcher.mjs", connection_file],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=os.name != "nt",
            )
            try:
                client.start_channels()
                client.wait_for_ready(timeout=45)
                print("PASS: jupyter_client kernel ready")

                msg_id = client.execute('println("hello from smoke test")')
                deadline = time.monotonic() + 60
                reply = None
                output = ""
                idle = False
                while time.monotonic() < deadline and (reply is None or not idle):
                    if reply is None:
                        try:
                            msg = client.get_shell_msg(timeout=0.5)
                            if msg.get("parent_header", {}).get("msg_id") == msg_id:
                                reply = msg
                        except Exception:
                            pass
                    try:
                        msg = client.get_iopub_msg(timeout=0.5)
                        if msg.get("parent_header", {}).get("msg_id") == msg_id:
                            if msg["msg_type"] == "stream":
                                output += msg["content"]["text"]
                            if msg["msg_type"] == "status" and msg["content"].get("execution_state") == "idle":
                                idle = True
                    except Exception:
                        pass
                assert reply is not None and reply["content"].get("status") == "ok", "execute_reply missing or failed"
                assert idle, "iopub idle missing"
                assert "hello from smoke test" in output, "iopub stream output missing"
                print("PASS: execute_reply and iopub output")

                deadline = time.monotonic() + 10
                while time.monotonic() < deadline and not client.hb_channel.is_beating():
                    time.sleep(0.2)
                assert client.hb_channel.is_beating(), "heartbeat channel not responding"
                print("PASS: heartbeat")

                client.shutdown(restart=False)
                response = client.get_control_msg(timeout=10)
                assert response["msg_type"] == "shutdown_reply", "control shutdown reply missing"
                print("PASS: control channel shutdown")
                assert kernel.wait(timeout=10) == 0, "launcher did not exit after shutdown"
                print("PASS: launcher exits cleanly")
                log.flush()
                kernel_log = log_path.read_text(encoding="utf-8", errors="replace")
                for channel in ("shell", "control", "stdin", "iopub", "hb"):
                    assert f"greeting on {channel}" in kernel_log, f"{channel} ZMTP handshake missing"
                assert "invalid ZMTP greeting" not in kernel_log
                print("PASS: all five ZMTP channel greetings")
            except Exception:
                log.flush()
                lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
                print("Kernel log (last 60 lines):\n" + "\n".join(lines[-60:]), file=sys.stderr)
                raise
            finally:
                client.stop_channels()
                if kernel.poll() is None:
                    if os.name == "nt":
                        subprocess.run(
                            ["taskkill", "/T", "/F", "/PID", str(kernel.pid)],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            check=False,
                        )
                    else:
                        os.killpg(kernel.pid, signal.SIGTERM)
                    try:
                        kernel.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        if os.name != "nt":
                            os.killpg(kernel.pid, signal.SIGKILL)
                        else:
                            kernel.kill()
                        kernel.wait()


if __name__ == "__main__":
    main()
