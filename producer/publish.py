import sys
import time

from db import delete_batch, fetch_batch, open_db

POLL_INTERVAL = 0.5
BATCH_SIZE = 100


def run() -> None:
    try:
        with open_db() as conn:
            while True:
                rows = fetch_batch(conn, limit=BATCH_SIZE)
                if rows:
                    ids = [row[0] for row in rows]
                    for _, raw, _ in rows:
                        print(raw, end="", flush=True)
                    delete_batch(conn, ids)
                else:
                    time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        sys.stderr.write("publish: shutting down\n")


if __name__ == "__main__":
    run()
