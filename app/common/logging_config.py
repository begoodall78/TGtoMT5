import logging, logging.config, os, sys, json
from datetime import datetime

# Allowed extras we won't include from LogRecord
_EXCLUDE = {
    "args","asctime","created","exc_info","exc_text","filename","funcName","levelno","lineno",
    "module","msecs","msg","pathname","process","processName","relativeCreated","stack_info",
    "thread","threadName","name"
}

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # merge extras
        for k, v in record.__dict__.items():
            if k not in _EXCLUDE:
                base[k] = v
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, separators=(",", ":"))

def setup_logging(level: str | None = None):
    lvl = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    logging.config.dictConfig({
        "version": 1,
        "formatters": {"json": {"()": JsonFormatter}},
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "json",
                "level": lvl,
            }
        },
        "root": {"level": lvl, "handlers": ["stdout"]},
        "disable_existing_loggers": False,
    })
