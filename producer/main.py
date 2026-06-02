import os
import signal
import socket
import sys

CONFIG = {
    "dump1090_host": os.getenv("DUMP1090_HOST", "localhost"),
    "dump1090_port": int(os.getenv("DUMP1090_PORT", "30003")),
}


def main() -> None:
    running = True

    def _handle_signal(signum: int, frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    with socket.create_connection(
        (CONFIG["dump1090_host"], CONFIG["dump1090_port"])
    ) as sock:
        sock.settimeout(1.0)
        with sock.makefile("r", encoding="utf-8") as stream:
            while running:
                try:
                    line = stream.readline()
                    if not line:
                        break
                    print(line, end="")
                except socket.timeout:
                    continue


if __name__ == "__main__":
    main()
