
import logging, sys, os

def setup_logging(level: str | None = None, log_file: str | None = None):
    lvl = getattr(logging, (level or os.getenv("LOG_LEVEL", "INFO")).upper(), logging.INFO)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(lvl)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    root.addHandler(stream)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        root.addHandler(fh)
