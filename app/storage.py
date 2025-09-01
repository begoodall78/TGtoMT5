from __future__ import annotations
from pathlib import Path
import logging, os
from dotenv import load_dotenv

log = logging.getLogger("storage")
# --- DB path resolution (absolute, repo-root-safe) --------------------------
_env_db = os.environ.get("APP_DB_PATH")
if _env_db:
    DB_PATH = str(Path(_env_db).expanduser().resolve())
else:
    _repo_root = Path(__file__).resolve().parents[1]  # .../app/
    DB_PATH = str((_repo_root / "runtime" / "data" / "app.db").resolve())
try:
    log.info("DB_PATH_RESOLVED", extra={"event": "DB_PATH", "path": DB_PATH})
except Exception:
    pass
load_dotenv(override=True)
import sqlite3, os, time
from typing import List, Optional
from pydantic import TypeAdapter
from app.models import Action, RouterResult

#DB_PATH = os.environ.get("APP_DB_PATH", os.path.join(os.getcwd(), "data", "app.db"))

def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)  # autocommit
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS queue(
              action_id TEXT PRIMARY KEY,
              payload   BLOB NOT NULL,
              status    TEXT NOT NULL DEFAULT 'PENDING',
              ts        REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS executions(
              action_id     TEXT PRIMARY KEY,
              status        TEXT NOT NULL,
              router_result BLOB,
              ts            REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS queue_status_ts ON queue(status, ts);
            """
        )
    finally:
        conn.close()

def enqueue(action: Action) -> bool:
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO queue(action_id, payload, status, ts) VALUES(?,?,?,?)",
            (action.action_id, action.model_dump_json().encode("utf-8"), "PENDING", time.time())
        )
        return True
    except sqlite3.IntegrityError:
        # already exists -> treat as dedup OK
        return False
    finally:
        conn.close()

def fetch_batch(limit: int = 32) -> List[Action]:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT action_id, payload FROM queue WHERE status='PENDING' ORDER BY ts LIMIT ?",
            (limit,)
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    ta = TypeAdapter(Action)
    actions: List[Action] = [ta.validate_json(row[1]) for row in rows]
    return actions

def mark_in_progress(action_id: str) -> None:
    conn = _connect()
    try:
        conn.execute("UPDATE queue SET status='IN_PROGRESS' WHERE action_id=?", (action_id,))
    finally:
        conn.close()

def mark_done(action_id: str, result: RouterResult) -> None:
    conn = _connect()
    try:
        conn.execute("UPDATE queue SET status='DONE' WHERE action_id=?", (action_id,))
        conn.execute(
            "INSERT OR REPLACE INTO executions(action_id, status, router_result, ts) VALUES(?,?,?,?)",
            (action_id, result.status, result.model_dump_json().encode("utf-8"), time.time())
        )
    finally:
        conn.close()

def already_executed(action_id: str) -> Optional[RouterResult]:
    conn = _connect()
    try:
        cur = conn.execute("SELECT router_result FROM executions WHERE action_id=?", (action_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        return None
    ta = TypeAdapter(RouterResult)
    return ta.validate_json(row[0])


def get_db_path() -> str:
    return DB_PATH


def reset_in_progress_to_pending() -> int:
    conn = _connect()
    try:
        cur = conn.execute("UPDATE queue SET status='PENDING' WHERE status='IN_PROGRESS'")
        return cur.rowcount or 0
    finally:
        conn.close()


def queue_counts() -> dict:
    conn = _connect()
    try:
        rows = conn.execute("SELECT status, COUNT(*) FROM queue GROUP BY status").fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        conn.close()
