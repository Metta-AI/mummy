"""Real startup ordering, occupied-port failure, and owned thread shutdown."""
import http.client
import signal
import socket
import subprocess
import sys
from pathlib import Path

probe = Path(sys.argv[1]).resolve()
for mode in ("normal", "before-listen", "bind-failure", "allocation-4", "allocation-5", "allocation-6"):
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
        if mode == "bind-failure":
            reserved.listen()
        else:
            reserved.close()
        allocation = mode.startswith("allocation-")
        command = [str(probe), "allocation-failure" if allocation else mode, str(port)]
        if allocation:
            limit = mode.split("-")[1]
            command = ["prlimit", f"--nofile={limit}:{limit}"] + command
        process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            if mode == "before-listen":
                assert process.stdout.readline().strip() == "CONSTRUCTED"
                process.send_signal(signal.SIGTERM)
                process.stdin.write("serve\n")
                process.stdin.flush()
            elif mode == "normal":
                assert process.stdout.readline().strip() == "READY"
                client = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
                try:
                    client.request("GET", "/health")
                    response = client.getresponse()
                    assert response.status == 200 and response.read() == b"actual-ready"
                finally:
                    client.close()
                process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr
            expected = ("ALLOCATION_FAILED_WITHOUT_OWNER" if allocation else
                        "BIND_FAILED_WITHOUT_OWNER" if mode == "bind-failure" else "SEALED_AND_JOINED")
            assert expected in stdout, stdout
            print(mode, expected, flush=True)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
