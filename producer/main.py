import multiprocessing
import signal
import sys

from db import open_db
from ingest import run as run_ingest
from publish import run as run_publish


def main() -> None:
    with open_db():
        pass  # initialize schema / WAL before children race for it

    ingest_proc = multiprocessing.Process(target=run_ingest, name="ingest")
    publish_proc = multiprocessing.Process(target=run_publish, name="publish")

    def _handle_signal(signum: int, frame: object) -> None:
        ingest_proc.terminate()
        publish_proc.terminate()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    ingest_proc.start()
    publish_proc.start()

    ingest_proc.join()
    publish_proc.join()

    sys.stderr.write("producer: shutting down\n")


if __name__ == "__main__":
    main()
