# app/infra/telegram_queue.py
from __future__ import annotations

import asyncio
import logging
import os
import re
import threading

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError

from app.engine.semantic import load_semantic_dictionary
from app.storage import init_db, enqueue
from app.processing import build_actions_from_message
from app.common.logging_setup import setup_logging
from app.infra.unparsed_reporter import UnparsedReporter

# --- Load env & logger early (so helpers can use them) ------------------------
load_dotenv(override=True)
log = logging.getLogger("telegram_queue")

# Provide a single source of truth for YAML path
SEMANTIC_DICT_PATH = os.getenv("SEMANTIC_DICT_PATH", "runtime/data/parser_semantic.yaml")

# --- ENV (with fallbacks to older keys) ---------------------------------------
API_ID   = int(os.environ.get("TG_API_ID",  os.environ.get("API_ID", "0")))
API_HASH =     os.environ.get("TG_API_HASH", os.environ.get("API_HASH", ""))

# Session storage
SESSION_PATH = os.environ.get("TG_SESSION_PATH")
SESSION_DIR  = os.environ.get("TG_SESSION_DIR")
SESSION_NAME = os.environ.get("TG_SESSION_NAME", os.environ.get("TG_SESSION", "tg"))

if SESSION_PATH:
    session_base = SESSION_PATH.rstrip(".session")
else:
    if not SESSION_DIR:
        SESSION_DIR = os.path.join(os.getcwd(), "runtime/sessions")
    os.makedirs(SESSION_DIR, exist_ok=True)
    session_base = os.path.join(SESSION_DIR, SESSION_NAME)

# Sources (multi takes priority)
SOURCE_CHATS_RAW = (os.environ.get("TG_SOURCE_CHATS") or "").strip()
SOURCE_CHAT_SINGLE = (os.environ.get("TG_SOURCE_CHAT", os.environ.get("TG_SOURCE", ""))).strip()

# Optional first-run auth helpers
PHONE      = os.environ.get("TG_PHONE") or os.environ.get("TELEGRAM_PHONE")
PASSWORD   = os.environ.get("TG_PASSWORD")
LOGIN_CODE = os.environ.get("TG_LOGIN_CODE")

# Defaults
DEFAULT_LEG_VOLUME = float(os.environ.get("DEFAULT_LEG_VOLUME", "0.01"))
DEFAULT_NUM_LEGS   = int(os.environ.get("DEFAULT_NUM_LEGS", "8"))

# Global router instance (created lazily)
_router_instance = None
_router_lock = threading.Lock()

# --- Helpers ------------------------------------------------------------------
def _split_sources(srcs: str) -> list[str]:
    if not srcs:
        return []
    parts = re.split(r"[,\s;]+", srcs.strip())
    return [p for p in parts if p]

def _collect_source_tokens() -> list[str]:
    tokens = _split_sources(SOURCE_CHATS_RAW)
    if not tokens and SOURCE_CHAT_SINGLE:
        tokens = _split_sources(SOURCE_CHAT_SINGLE)
    return tokens

async def ensure_authorized(client: TelegramClient):
    await client.connect()
    if await client.is_user_authorized():
        log.info("AUTHORIZED", extra={"event": "AUTHORIZED", "session": session_base})
        return

    if not PHONE:
        log.error(
            "First run needs TG_PHONE in .env (or supply an existing session file).",
            extra={"event": "LOGIN_MISSING_PHONE"},
        )
        raise SystemExit(1)

    log.info("REQUEST_CODE", extra={"event": "REQUEST_CODE", "phone": PHONE[-4:]})
    await client.send_code_request(PHONE)

    code = LOGIN_CODE or input("Enter Telegram login code: ").strip()
    try:
        await client.sign_in(phone=PHONE, code=code)
    except SessionPasswordNeededError:
        if not PASSWORD:
            log.error(
                "2FA enabled — set TG_PASSWORD in .env for first run.",
                extra={"event": "LOGIN_2FA_REQUIRED"},
            )
            raise
        await client.sign_in(password=PASSWORD)

    log.info("SESSION_SAVED", extra={"event": "SESSION_SAVED", "session": session_base})

async def resolve_one(client: TelegramClient, token: str):
    token = token.strip()
    try:
        if token.startswith("@") or any(c.isalpha() for c in token):
            ent = await client.get_entity(token.lstrip("@"))
            return ent

        if token.lstrip("+-").isdigit():
            ent = None
            try:
                ent = await client.get_entity(int(token))
            except Exception:
                ent = None
            if ent is None:
                phone = token if token.startswith("+") else "+" + token
                try:
                    ent = await client.get_entity(phone)
                except Exception:
                    ent = None
            if ent is None:
                target = int(token)
                async for d in client.iter_dialogs():
                    if getattr(d.entity, "id", None) == target:
                        ent = d.entity
                        break
            if ent is None:
                raise ValueError(f"cannot resolve numeric '{token}'")
            return ent

        return await client.get_entity(token)
    except Exception as e:
        log.error(
            "SOURCE_CHAT_RESOLVE_FAILED",
            extra={
                "event": "SOURCE_CHAT_RESOLVE_FAILED",
                "input": token,
                "error": str(e),
            },
        )
        return None

async def resolve_chats(client: TelegramClient, tokens: list[str]):
    if not tokens or any(t.upper() in ("ALL", "*") for t in tokens):
        return None  # listen to all
    resolved = []
    for tok in tokens:
        ent = await resolve_one(client, tok)
        if ent:
            resolved.append(ent)
            log.info(
                "SOURCE_CHAT_RESOLVED",
                extra={
                    "event": "SOURCE_CHAT_RESOLVED",
                    "input": tok,
                    "resolved": getattr(ent, "title", None) or getattr(ent, "username", None),
                    "id": getattr(ent, "id", None),
                    "type": ent.__class__.__name__,
                },
            )
    if not resolved:
        log.info("FALLBACK_ALL_CHATS", extra={"event": "FALLBACK_ALL_CHATS"})
        return None
    return resolved

def _echo_runtime_config():
    """Print human banner and semantic/ignore rules status."""
    cfg = {
        "ROUTER_BACKEND": os.environ.get("ROUTER_BACKEND"),
        "MT5_ACTIONS_DIR": os.environ.get("MT5_ACTIONS_DIR"),
        "DEFAULT_NUM_LEGS": str(DEFAULT_NUM_LEGS),
        "DEFAULT_LEG_VOLUME": str(DEFAULT_LEG_VOLUME),
        "SIGNAL_REQUIRE_SYMBOL": os.environ.get("SIGNAL_REQUIRE_SYMBOL"),
        "SIGNAL_REQUIRE_PRICE": os.environ.get("SIGNAL_REQUIRE_PRICE"),
        "TG_SOURCE_CHATS": os.environ.get("TG_SOURCE_CHATS"),
        "TG_SESSION_DIR": os.environ.get("TG_SESSION_DIR"),
        "TG_SESSION_NAME": os.environ.get("TG_SESSION_NAME"),
        "UNPARSED_FORWARD_ENABLED": os.environ.get("UNPARSED_FORWARD_ENABLED"),
        "UNPARSED_REVIEW_CHAT_ID": os.environ.get("UNPARSED_REVIEW_CHAT_ID"),
        "ROUTER_MODE": os.environ.get("ROUTER_MODE"),
        "ALLOW_OPEN_ON_MODIFY": os.environ.get("ALLOW_OPEN_ON_MODIFY"),
        "MT5_FIRST_LEG_WORSE_PIPS": os.environ.get("MT5_FIRST_LEG_WORSE_PIPS"),
        "MT5_FIRST_LEG_WORSE_PRICE": os.environ.get("MT5_FIRST_LEG_WORSE_PRICE"),
        "MT5_DEVIATION": os.environ.get("MT5_DEVIATION"),
        "MT5_FILLING": os.environ.get("MT5_FILLING"),
        "POSITION_POLL_ENABLED": os.environ.get("POSITION_POLL_ENABLED", "false"),
        "RISK_FREE_PIPS": os.environ.get("RISK_FREE_PIPS", "10.0"),
    }

    # Semantic dictionary metadata
    dict_ver = "NA"
    dict_path = SEMANTIC_DICT_PATH
    try:
        sem_dict = load_semantic_dictionary(dict_path)
        dict_ver = getattr(sem_dict, "version", None) or getattr(sem_dict, "dictionary_version", None) or "-"
        log.info(
            "SEMANTIC_DICT_LOADED",
            extra={
                "event": "SEMANTIC_DICT_LOADED",
                "dictionary_version": dict_ver,
                "path": dict_path,
            },
        )
    except Exception as e:
        log.warning(
            f"Failed to load semantic dictionary: {e}",
            extra={"event": "SEMANTIC_DICT_LOAD_FAILED", "path": dict_path},
        )

    banner = (
        "\n"
        "==================== TGtoMT5 CONFIG ====================\n"
        f"Router          : {cfg['ROUTER_BACKEND']}\n"
        f"Actions Dir     : {cfg['MT5_ACTIONS_DIR']}\n"
        f"Legs / Volume   : {cfg['DEFAULT_NUM_LEGS']} / {cfg['DEFAULT_LEG_VOLUME']}\n"
        f"Require Symbol  : {cfg['SIGNAL_REQUIRE_SYMBOL']}\n"
        f"Require Price   : {cfg['SIGNAL_REQUIRE_PRICE']}\n"
        f"Dictionary Ver  : {dict_ver}\n"
        f"Source Chats    : {cfg['TG_SOURCE_CHATS']}\n"
        f"Session Dir/Name: {cfg['TG_SESSION_DIR']} / {cfg['TG_SESSION_NAME']}\n"
        f"Unparsed Fwd    : {cfg['UNPARSED_FORWARD_ENABLED']}  "
        f"(Review Chat: {cfg['UNPARSED_REVIEW_CHAT_ID']})\n"
        f"Risk-Free       : Enabled={cfg['POSITION_POLL_ENABLED']} Pips={cfg['RISK_FREE_PIPS']}\n"
        "========================================================\n"
    )
    print(banner)

    human_summary = (
        f"Router={cfg['ROUTER_BACKEND']}  "
        f"ActionsDir={cfg['MT5_ACTIONS_DIR']}  "
        f"Legs={cfg['DEFAULT_NUM_LEGS']}  "
        f"Vol={cfg['DEFAULT_LEG_VOLUME']}  "
        f"ReqSymbol={cfg['SIGNAL_REQUIRE_SYMBOL']}  "
        f"ReqPrice={cfg['SIGNAL_REQUIRE_PRICE']}  "
        f"SrcChats={cfg['TG_SOURCE_CHATS']}  "
        f"UnparsedFwd={cfg['UNPARSED_FORWARD_ENABLED']}  "
        f"DictVer={dict_ver}"
    )
    log.info(human_summary, extra={"event": "CONFIG", **cfg})

    # Ignore-rules audit (safe, no crash if absent)
    try:
        from app.processing import _SEM_DICT, get_flag
        ir = (_SEM_DICT.data or {}).get("ignore_rules") or {}
        iv = str(ir.get("version") or "NA")
        ic = len(ir.get("contains") or [])
        log.info(
            "Ignore Rules   : %s | version=%s | count=%d",
            "enabled" if get_flag("ENGINE_ENABLE_IGNORE_GATE", False) else "disabled",
            iv,
            ic,
        )
    except Exception:
        log.info("Ignore Rules   : disabled | version=NA | count=0")

# Get router instance for risk-free processing (LAZY - FIXED)
def get_router_for_processing():
    """Get router instance lazily - only create when actually needed for RISK FREE messages."""
    global _router_instance
    
    # Check if risk-free is even enabled
    if os.getenv("POSITION_POLL_ENABLED", "false").lower() not in ("true", "1", "yes"):
        return None
    
    # Use double-checked locking pattern
    if _router_instance is not None:
        return _router_instance
    
    with _router_lock:
        # Check again inside the lock
        if _router_instance is not None:
            return _router_instance
        
        try:
            # Only import and create when actually needed
            from app.infra.mt5_router import Mt5NativeRouter
            from app.common.config import Config
            
            # Run MT5 initialization in a separate thread to avoid blocking
            def create_router():
                return Mt5NativeRouter(Config())
            
            # Create in thread to avoid blocking event loop
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(create_router)
                _router_instance = future.result(timeout=5.0)
            
            log.info("Router created for risk-free processing")
            return _router_instance
            
        except Exception as e:
            log.warning(f"Could not create router for risk-free: {e}")
            return None

# --- Main ingest loop ---------------------------------------------------------
async def run_telegram_ingest():
    if API_ID == 0 or not API_HASH:
        log.error("TG_API_ID / TG_API_HASH missing in .env", extra={"event": "ENV_MISSING_API"})
        raise SystemExit(1)

    client = TelegramClient(session_base, API_ID, API_HASH)
    reporter = UnparsedReporter(client)
    await ensure_authorized(client)

    tokens = _collect_source_tokens()  # may be empty → all
    chat_filter = await resolve_chats(client, tokens)
    log.info(
        "INGEST_STARTED",
        extra={
            "event": "INGEST_STARTED",
            "source_tokens": tokens or ["ALL"],
            "resolved_count": (len(chat_filter) if isinstance(chat_filter, list) else ("ALL" if chat_filter is None else 1)),
        },
    )
    
    # Don't create router at startup - will be created lazily if needed
    log.info("Risk-free support: %s", 
             "enabled (router will be created on demand)" 
             if os.getenv("POSITION_POLL_ENABLED", "false").lower() in ("true", "1", "yes")
             else "disabled")

    @client.on(events.NewMessage(chats=chat_filter))
    async def on_new(event):
        try:
            msg_id = str(event.id)
            text = event.raw_text or ""
            chat = await event.get_chat()
            
            # Debug logging for reply detection
            reply_to = getattr(event.message, "reply_to_msg_id", None)
            if "risk free" in text.lower():
                log.info(f"RISK FREE detected - reply_to_msg_id: {reply_to}, message: {event.message}")
                if event.message.reply_to:
                    log.info(f"Reply object exists: {event.message.reply_to}")
        
            # Only get router if this looks like a RISK FREE message
            router = None
            if re.search(r'\b(?:GOING\s+)?RISK\s*FREE\b', text, re.IGNORECASE):
                router = get_router_for_processing()

            actions = build_actions_from_message(
                source_msg_id=msg_id,
                text=text,
                is_edit=False,
                legs_count=DEFAULT_NUM_LEGS,
                leg_volume=DEFAULT_LEG_VOLUME,
                unparsed_reporter=reporter,
                unparsed_raw_msg=event.message,
                reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                router=router  # Pass router only when needed
            )

            if actions:
                for a in actions:
                    dedup = not enqueue(a)
                    log.info(
                        "INGESTED_NEW",
                        extra={
                            "event": "INGESTED_DONE",
                            "action_id": a.action_id,
                            "dedup": dedup,
                            "chat_id": getattr(chat, "id", None),
                            "chat_title": getattr(chat, "title", None) or getattr(chat, "username", None),
                        },
                    )
            else:
                # If ignored explicitly, do not forward to unparsed
                if getattr(event.message, "_ignored_by_gate", False):
                    log.debug("Message ignored by gate", extra={"msg_id": msg_id})
                else:
                    await reporter.report_unparsed(event.message, reason="NO_MATCH")
                log.info(
                    "UNPARSED_NEW_FORWARDED",
                    extra={
                        "event": "UNPARSED_NEW_FORWARDED",
                        "chat_id": getattr(chat, "id", None),
                        "message_id": msg_id,
                    },
                )
        except Exception as e:
            log.error(f"Error processing new message: {e}", exc_info=True)

    @client.on(events.MessageEdited(chats=chat_filter))
    async def on_edit(event):
        try:
            msg_id = str(event.id)
            text = event.raw_text or ""
            chat = await event.get_chat()
            
            # Only get router if this looks like a RISK FREE message
            router = None
            if re.search(r'\b(?:GOING\s+)?RISK\s*FREE\b', text, re.IGNORECASE):
                router = get_router_for_processing()

            actions = build_actions_from_message(
                source_msg_id=msg_id,
                text=text,
                is_edit=True,
                legs_count=DEFAULT_NUM_LEGS,
                leg_volume=DEFAULT_LEG_VOLUME,
                unparsed_reporter=reporter,
                unparsed_raw_msg=event.message,
                router=router  # Pass router only when needed
            )

            if actions:
                for a in actions:
                    dedup = not enqueue(a)
                    log.info(
                        "INGESTED_EDIT",
                        extra={
                            "event": "INGESTED_EDIT",
                            "action_id": a.action_id,
                            "dedup": dedup,
                            "chat_id": getattr(chat, "id", None),
                            "chat_title": getattr(chat, "title", None) or getattr(chat, "username", None),
                        },
                    )
            else:
                # Forward/report non-parsed edit
                await reporter.report_unparsed(event.message, reason="NO_MATCH")
                log.info(
                    "UNPARSED_EDIT_FORWARDED",
                    extra={
                        "event": "UNPARSED_EDIT_FORWARDED",
                        "chat_id": getattr(chat, "id", None),
                        "message_id": msg_id,
                    },
                )
        except Exception as e:
            log.error(f"Error processing edited message: {e}", exc_info=True)

    await client.run_until_disconnected()

def main():
    setup_logging()
    _echo_runtime_config()
    init_db()
    asyncio.run(run_telegram_ingest())

if __name__ == "__main__":
    main()