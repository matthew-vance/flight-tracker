import os
import signal
import socket
import sqlite3
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

CONFIG = {
    "dump1090_host": os.getenv("DUMP1090_HOST", "localhost"),
    "dump1090_port": int(os.getenv("DUMP1090_PORT", "30003")),
    "buffer_db_path": os.getenv("BUFFER_DB_PATH", "buffer.db"),
}


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages ("
        "  id TEXT PRIMARY KEY,"
        "  raw TEXT NOT NULL,"
        "  timestamp TEXT NOT NULL"
        ")"
    )
    return conn


def store_line(conn: sqlite3.Connection, raw: str) -> None:
    conn.execute(
        "INSERT INTO messages (id, raw, timestamp) VALUES (?, ?, ?)",
        (
            str(uuid.uuid7()),
            raw,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


@contextmanager
def managed_connection(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = init_db(db_path)
    try:
        yield conn
    finally:
        conn.close()


def main() -> None:
    running = True
    sock: socket.socket | None = None

    def _handle_signal(signum: int, frame: object) -> None:
        sys.stderr.write("\nproducer: shutting down\n")
        nonlocal running
        running = False
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RD)
            except OSError:
                pass

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    with managed_connection(CONFIG["buffer_db_path"]) as conn:
        while running:
            try:
                sock = socket.create_connection(
                    (CONFIG["dump1090_host"], CONFIG["dump1090_port"]),
                    timeout=5,
                )
            except OSError:
                if running:
                    sys.stderr.write("producer: connection failed, retrying...\n")
                    time.sleep(5)
                continue

            try:
                with sock, sock.makefile("r", encoding="utf-8") as stream:
                    for raw in stream:
                        print(raw, end="")
                        store_line(conn, raw)
            except OSError:
                pass  # signal-triggered shutdown

            if running:
                sys.stderr.write("producer: disconnected, reconnecting...\n")
                time.sleep(5)


if __name__ == "__main__":
    main()
