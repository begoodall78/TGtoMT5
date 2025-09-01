# unparsed_reporter.py
# MVP: log + forward Telegram messages we can't safely parse into trades.
# Python 3.10+ / Telethon v2.x

import os
import json
import asyncio
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Literal

from telethon import TelegramClient
from telethon.tl.custom.message import Message

Reason = Literal["NO_MATCH", "CONFLICT", "UNSAFE_RANGE"]

DEFAULT_LOG_DIR = r"D:\0 Trading\TGtoMT5\logs"
DEFAULT_DEDUP_WINDOW_SEC = 300
DEFAULT_KEEP_DAYS = 30


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _date_tag(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _norm_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8"), usedforsecurity=False).hexdigest()


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _tg_deeplink_from_ids(chat_id: int, message_id: int) -> str:
    """
    Build a deep link to the message when possible.
    - For private/supergroup channels: https://t.me/c/<internal>/<mid>
      where internal = abs(chat_id) - 1000000000000
    - If the math looks odd (very small/positive chat_id), we still try the c/ form.
    """
    internal = abs(chat_id) - 1000000000000
    if internal <= 0:
        # Fallback: best-effort modulo trim (covers some migrated chats)
        internal = abs(chat_id)
        if internal > 1000000000000:
            internal = internal - 1000000000000
    return f"https://t.me/c/{internal}/{message_id}"


class UnparsedReporter:
    """
    Usage:
        reporter = UnparsedReporter(client)
        await reporter.report_unparsed(msg, reason="NO_MATCH", symbol_guess="XAUUSD")
    """

    def __init__(
        self,
        client: TelegramClient,
        *,
        log_dir: Optional[str] = None,
        review_chat_id: Optional[int] = None,
        forwarding_enabled: Optional[bool] = None,
        dedup_window_sec: Optional[int] = None,
        keep_days: Optional[int] = None,
        parser_version: str = "v0.1",
        ops_ack_chat_id: Optional[int] = None,
    ) -> None:
        self.client = client

        # Env-configurable knobs
        self.log_dir = Path(
            log_dir
            or os.getenv("UNPARSED_LOG_DIR", DEFAULT_LOG_DIR)
        )
        self.review_chat_id = review_chat_id or self._int_env("UNPARSED_REVIEW_CHAT_ID")
        self.forwarding_enabled = (
            forwarding_enabled
            if forwarding_enabled is not None
            else os.getenv("UNPARSED_FORWARD_ENABLED", "true").lower() == "true"
        )
        self.dedup_window_sec = dedup_window_sec or int(
            os.getenv("UNPARSED_DEDUP_WINDOW_SECONDS", DEFAULT_DEDUP_WINDOW_SEC)
        )
        self.keep_days = keep_days or int(os.getenv("UNPARSED_KEEP_DAYS", DEFAULT_KEEP_DAYS))
        self.parser_version = parser_version
        self.ops_ack_chat_id = ops_ack_chat_id or self._int_env("UNPARSED_OPS_ACK_CHAT_ID", required=False)

        _ensure_dir(self.log_dir)

        # In-memory dedup cache: sha1 -> last_seen_utc
        self._dedup_cache: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _int_env(name: str, required: bool = True) -> Optional[int]:
        v = os.getenv(name)
        if v is None:
            if required:
                raise ValueError(f"Missing environment variable: {name}")
            return None
        try:
            return int(v)
        except ValueError as e:
            raise ValueError(f"Environment variable {name} must be an integer") from e

    def _log_path_for(self, dt: datetime) -> Path:
        return self.log_dir / f"unparsed_{_date_tag(dt)}.ndjson"

    async def report_unparsed(
        self,
        msg: Message,
        *,
        reason: Reason,
        symbol_guess: Optional[str] = None,
        side_guess: Optional[str] = None,
    ) -> None:
        """
        Log + (optionally) forward an unparsed message.
        Dedups forwards within a rolling window.
        """
        # Build record
        dt = _now_utc()
        norm = _norm_text(msg.message or "")
        content_sha1 = _sha1(norm)

        record_id = f"unp_{dt.strftime('%Y%m%d_%H%M%S')}_{content_sha1[:4]}"
        rec = {
            "id": record_id,
            "ts_utc": dt.isoformat(),
            "source_chat_id": msg.chat_id,
            "source_msg_id": msg.id,
            "sender_id": msg.sender_id if hasattr(msg, "sender_id") else None,
            "sender_username": getattr(getattr(msg, "sender", None), "username", None),
            "raw_text": msg.message or "",
            "reason": reason,
            "symbol_guess": symbol_guess,
            "side_guess": side_guess,
            "parser_version": self.parser_version,
            "content_sha1": content_sha1,
            "forward_state": "PENDING",
        }

        async with self._lock:
            # 1) Append to today's NDJSON
            self._append_ndjson(self._log_path_for(dt), rec)

            # 2) Dedup window check (in-memory). Always record; only skip forwarding.
            now = _now_utc()
            last = self._dedup_cache.get(content_sha1)
            should_forward = True
            if last and (now - last) < timedelta(seconds=self.dedup_window_sec):
                should_forward = False
            # Refresh cache timestamp
            self._dedup_cache[content_sha1] = now

            # 3) Forward if enabled & not dedup’d
            if self.forwarding_enabled and should_forward and self.review_chat_id:
                await self._forward_to_review(rec)
                # 4) Ack line to ops (optional)
                if self.ops_ack_chat_id:
                    await self._post_ops_ack(rec, dedup=False)
                # 5) Mark forwarded in log (append a tiny status record for audit)
                rec2 = rec.copy()
                rec2["forward_state"] = "FORWARDED"
                self._append_ndjson(self._log_path_for(dt), rec2)
            else:
                # Dedup or disabled — optional ops ack (silent dedup info)
                if self.ops_ack_chat_id and self.forwarding_enabled:
                    await self._post_ops_ack(rec, dedup=True)

        # 6) Fire-and-forget cleanup (don’t block the call)
        asyncio.create_task(self._cleanup_old_logs())

    async def _forward_to_review(self, rec: dict) -> None:
        chat_id = rec["source_chat_id"]
        msg_id = rec["source_msg_id"]
        symbol_hint = f"{rec['symbol_guess']}?" if rec.get("symbol_guess") else ""
        link = _tg_deeplink_from_ids(chat_id, msg_id)

        # London time display (user is in Europe/London per project context)
        # We'll render a UTC line too for clarity.
        london_offset = timedelta(hours=1)  # Summer; acceptable MVP
        ts_utc = datetime.fromisoformat(rec["ts_utc"])
        ts_london = (ts_utc + london_offset).strftime("%d %b %Y %H:%M")
        ts_utc_str = ts_utc.strftime("%d %b %Y %H:%M UTC")

        raw = rec["raw_text"]
        if raw and len(raw) > 400:
            raw = raw[:400] + " […]"

        header = f"UNPARSED [{rec['reason']}]"
        if symbol_hint:
            header += f" | {symbol_hint}"

        body = (
            f"{header}\n"
            f"From: @{rec.get('sender_username') or rec.get('sender_id')}\n"
            f"Time: {ts_london} London / {ts_utc_str}\n"
            f"Msg Link: {link}\n\n"
            f"Text:\n"
            f"\"{raw}\"\n\n"
            f"Context:\n"
            f"id={rec['id']} | parser={rec['parser_version']}"
        )

        await self.client.send_message(self.review_chat_id, body)

    async def _post_ops_ack(self, rec: dict, *, dedup: bool) -> None:
        # One-liner in ops channel
        status = "DEDUP-SKIP" if dedup else "FORWARDED"
        line = (
            f"[UNPARSED→REVIEW] {rec['reason']} | msg {rec['source_msg_id']} | id {rec['id']} | {status}"
        )
        try:
            await self.client.send_message(self.ops_ack_chat_id, line)
        except Exception:
            # Non-fatal
            pass

    def _append_ndjson(self, path: Path, obj: dict) -> None:
        _ensure_dir(path.parent)
        s = json.dumps(obj, ensure_ascii=False)
        with open(path, "a", encoding="utf-8", newline="\n") as f:
            f.write(s)
            f.write("\n")

    async def _cleanup_old_logs(self) -> None:
        """Delete daily NDJSON files older than keep_days."""
        if self.keep_days <= 0:
            return
        cutoff = _now_utc().date() - timedelta(days=self.keep_days)
        try:
            for p in self.log_dir.glob("unparsed_*.ndjson"):
                # Expect filename pattern unparsed_YYYYMMDD.ndjson
                try:
                    tag = p.stem.split("_")[1]
                    dt = datetime.strptime(tag, "%Y%m%d").date()
                    if dt < cutoff:
                        p.unlink(missing_ok=True)
                except Exception:
                    # Ignore unexpected filenames
                    continue
        except Exception:
            # Non-fatal cleaner
            pass
