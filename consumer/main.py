import signal
import sys

from consume import run


def main() -> None:
    def _handle_signal(signum: int, frame: object) -> None:
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)

    run()


if __name__ == "__main__":
    main()
