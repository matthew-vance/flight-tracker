from datetime import datetime
import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager


def get_db_path() -> str:
    return os.getenv("BUFFER_DB_PATH", "data/buffer.db")


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages ("
        "  id TEXT PRIMARY KEY,"
        "  raw TEXT NOT NULL,"
        "  timestamp TEXT NOT NULL"
        ")"
    )


def store_line(conn: sqlite3.Connection, raw: str) -> None:
    conn.execute(
        "INSERT INTO messages (id, raw, timestamp) VALUES (?, ?, ?)",
        (str(uuid.uuid7()), raw, datetime.now().isoformat()),
    )
    conn.commit()


def fetch_batch(
    conn: sqlite3.Connection, limit: int = 100
) -> list[tuple[str, str, str]]:
    cursor = conn.execute(
        "SELECT id, raw, timestamp FROM messages ORDER BY id LIMIT ?",
        (limit,),
    )
    return cursor.fetchall()


def delete_batch(conn: sqlite3.Connection, ids: list[str]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"DELETE FROM messages WHERE id IN ({placeholders})",
        ids,
    )
    conn.commit()


@contextmanager
def open_db() -> Iterator[sqlite3.Connection]:
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    init_db(conn)
    try:
        yield conn
    finally:
        conn.close()
