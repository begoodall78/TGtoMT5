import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional
import hashlib
import csv
import re

DB_PATH = Path(os.getenv("APP_DB_PATH", "runtime/data/app.db"))
ACKS_DIR = Path(os.getenv("ACKS_DIR", "runtime/actions/acks"))

_COMMENT_RE = re.compile(r'^(?P<msg>\d+)_(?P<idx>\d+):(?P<sym>[A-Z0-9+]+)$')

def get_connection():
    _ensure_db()
    return sqlite3.connect(DB_PATH)

def _to_result_dict(exec_result: Any) -> dict:
    # Pydantic v2: model_dump; v1: dict()
    if hasattr(exec_result, "model_dump"):
        return exec_result.model_dump()
    if hasattr(exec_result, "dict"):
        return exec_result.dict()
    return dict(exec_result or {}) if isinstance(exec_result, dict) else {}


def apply_open_result(conn, action, exec_result):
    """
    Persist order/position tickets returned by the router into legs_index.
    Matches rows by (group_key = OPEN_<source_msg_id>) and leg index parsed from request.comment.
    """
    data = _to_result_dict(exec_result)
    results = (data.get("details") or {}).get("results") or []
    if not results:
        return

    gk = f"OPEN_{action.source_msg_id}"
    cur = conn.cursor()

    for item in results:
        d = (item or {}).get("result", {}).get("details", {})
        req = d.get("request", {}) or {}
        comment = req.get("comment") or d.get("request_comment") or ""
        m = _COMMENT_RE.match(comment)
        if not m:
            continue
        idx = int(m.group("idx"))
        order_ticket = d.get("order")
        # Optional: if you have a way to compute live position ticket now, put it here.
        position_ticket = d.get("position")  # provided by router for market deals; else None

        # Update the row for this leg
        # We match by group_key and leg_tag suffix '#<idx>' to avoid symbol formatting issues
        cur.execute("""
            UPDATE legs_index
               SET order_ticket = COALESCE(?, order_ticket),
                   position_ticket = COALESCE(?, position_ticket)
             WHERE group_key = ?
               AND leg_tag LIKE ?
        """, (order_ticket, position_ticket, gk, f"%#{idx}"))
    conn.commit()


def ensure_ticket_columns(conn):
    cur = conn.cursor()
    # add columns if missing (SQLite is tolerant to ALTER on existing columns in try/except)
    try:
        cur.execute("ALTER TABLE legs_index ADD COLUMN order_ticket INTEGER")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE legs_index ADD COLUMN position_ticket INTEGER")
    except Exception:
        pass
    conn.commit()


def _ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals(
                source_msg_id TEXT PRIMARY KEY,
                chat_id       INTEGER,
                msg_ts        TEXT,
                group_key     TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS legs_index(
                group_key   TEXT,
                leg_tag     TEXT,
                symbol      TEXT,
                volume      REAL,
                entry       REAL,
                ticket      TEXT,
                status      TEXT,
                UNIQUE(group_key, leg_tag)
            )
        """)
        # Attempt to add sl/tp if missing (idempotent)
        try: cur.execute("ALTER TABLE legs_index ADD COLUMN sl REAL")
        except Exception: pass
        try: cur.execute("ALTER TABLE legs_index ADD COLUMN tp REAL")
        except Exception: pass
        con.commit()

def _gk_for_open(action: Dict[str, Any]) -> str:
    return f"OPEN_{action.get('source_msg_id','')}"

def record_open(action: Dict[str, Any]) -> str:
    """Insert OPEN action legs as PENDING into index. Returns group_key."""
    _ensure_db()
    gk = _gk_for_open(action)
    src = str(action.get("source_msg_id",""))
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO signals(source_msg_id, chat_id, msg_ts, group_key) VALUES (?,?,?,?)",
                    (src, None, None, gk))
        for leg in action.get("legs", []):
            cur.execute("""                    INSERT OR IGNORE INTO legs_index(group_key, leg_tag, symbol, volume, entry, sl, tp, ticket, status)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (gk, leg.get('tag') or leg.get('leg_id'), leg.get('symbol'), float(leg.get('volume') or 0.0), float(leg.get('entry') or 0.0), None if leg.get('sl') is None else float(leg.get('sl')), None if leg.get('tp') is None else float(leg.get('tp')), None, 'PENDING'))
        con.commit()
    return gk

def _fake_ticket(seed: str) -> str:
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10].upper()
    return f"FAKE{h}"

def generate_mock_ack_for_open(action: Dict[str, Any]) -> str:
    """Create an ACK CSV for this OPEN (deterministic fake tickets) and update index to OPEN."""
    _ensure_db()
    ACKS_DIR.mkdir(parents=True, exist_ok=True)
    action_id = action.get("action_id","")
    src = str(action.get("source_msg_id",""))
    gk = _gk_for_open(action)
    # Update index and build rows
    rows = [("client_id","status","order_ticket","error_code","error_text")]
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        for leg in action.get("legs", []):
            tag = leg.get("tag") or leg.get("leg_id")
            ticket = _fake_ticket(f"{src}:{tag}")
            # apply to index
            cur.execute("""                    UPDATE legs_index
                   SET ticket=?, status='OPEN'
                 WHERE group_key=? AND leg_tag=?
            """, (ticket, gk, tag))
            rows.append((action_id, "OK", ticket, "0", ""))
        con.commit()
    # Write ack csv
    out_path = ACKS_DIR / f"ack_{action_id}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)
    return str(out_path)

def list_open_legs(group_key: str) -> List[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("""
            SELECT leg_tag,
                   symbol,
                   volume,
                   entry,
                   sl,
                   tp,
                   ticket,                 -- legacy field
                   order_ticket,           -- NEW
                   position_ticket,        -- NEW
                   status
              FROM legs_index
             WHERE group_key=?
             ORDER BY leg_tag
        """, (group_key,))
        return [dict(r) for r in cur.fetchall()]


def update_leg_targets(group_key: str, leg_tag: str, *, sl: float | None = None, tp: float | None = None) -> None:
    """Persist desired SL/TP targets into legs_index for a specific leg.
    This acts as 'desired' values even if a pending cannot be modified immediately.
    """
    _ensure_db()
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        if sl is not None:
            cur.execute("""UPDATE legs_index SET sl=? WHERE group_key=? AND leg_tag=?""", (float(sl), group_key, leg_tag))
        if tp is not None:
            cur.execute("""UPDATE legs_index SET tp=? WHERE group_key=? AND leg_tag=?""", (float(tp), group_key, leg_tag))
        con.commit()

def resolve_group_key_from_reply(source_msg_id: str) -> str:
    # For now group key is derived from original OPEN source_msg_id
    return f"OPEN_{source_msg_id}"


def resolve_group_key(text: str = "", reply_to_msg_id: str | None = None) -> str | None:
    """Resolve a stable group key (GK) for management commands.
    Priority:
      1) If reply_to_msg_id is provided, use it (maps to OPEN_{msg_id}).
      2) Fallback: try to detect an explicit GK marker in text like [GK:OPEN_...].
      3) Otherwise return None.
    """
    if reply_to_msg_id:
        return resolve_group_key_from_reply(str(reply_to_msg_id))
    # optional lightweight fallback via marker
    import re
    m = re.search(r"\[GK:(?P<gk>OPEN_[^\]]+)\]", text or "")
    if m:
        return m.group("gk")
    return None
