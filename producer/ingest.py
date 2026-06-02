import os
import socket
import sys
import time

from db import open_db, store_line

CONFIG = {
    "dump1090_host": os.getenv("DUMP1090_HOST", "localhost"),
    "dump1090_port": int(os.getenv("DUMP1090_PORT", "30003")),
}


def run() -> None:
    try:
        with open_db() as conn:
            while True:
                try:
                    sock = socket.create_connection(
                        (CONFIG["dump1090_host"], CONFIG["dump1090_port"]),
                        timeout=5,
                    )
                except OSError:
                    sys.stderr.write("ingest: connection failed, retrying...\n")
                    time.sleep(5)
                    continue

                try:
                    with sock, sock.makefile("r", encoding="utf-8") as stream:
                        for raw in stream:
                            store_line(conn, raw)
                except OSError:
                    pass

                sys.stderr.write("ingest: disconnected, reconnecting...\n")
                time.sleep(5)
    except KeyboardInterrupt:
        sys.stderr.write("ingest: shutting down\n")


if __name__ == "__main__":
    run()
