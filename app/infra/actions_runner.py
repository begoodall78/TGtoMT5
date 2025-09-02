from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import traceback


import logging
import os
import time
from typing import Any, Dict, List, Optional

from app import storage
from app.infra.mt5_router import get_router
from app.models import Action, RouterResult

# Import position polling for risk-free management
try:
    from app.monitors.position_poller import start_position_polling
except ImportError:
    start_position_polling = None

# Optional ticket persistence hooks
try:
    from app import refindex  # type: ignore
except Exception:  # pragma: no cover
    refindex = None  # type: ignore


log = logging.getLogger("actions_runner")
EXEC_TIMEOUT_SECS = int(os.environ.get("RUNNER_EXEC_TIMEOUT", "30"))
HEARTBEAT_SECS = int(os.environ.get("RUNNER_HEARTBEAT_SECS", "5"))



# -------- JSONL debug handler -------------------------------------------------
import json

class JsonlFileHandler(logging.FileHandler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = getattr(record, "json_payload", None)
            if msg is None:
                return
            line = json.dumps(msg, ensure_ascii=False, separators=(",", ":"))
            self.stream.write(line + "\n")
            self.flush()
        except Exception:
            self.handleError(record)


def attach_jsonl_debug_logger(logger_name: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    logger = logging.getLogger(logger_name)
    h = JsonlFileHandler(path, encoding="utf-8")
    h.setLevel(logging.DEBUG)
    logger.addHandler(h)


# -------- Human formatting ----------------------------------------------------
def _render_leg_row(leg_item: Dict[str, Any]) -> str:
    leg = leg_item.get("leg", "?")
    r = leg_item.get("result", {}) or {}
    ok = r.get("ok", False)
    d = (r.get("details") or {})
    rc = d.get("retcode_label") or d.get("retcode")
    deal = d.get("deal")
    order = d.get("order") or d.get("position")
    req = d.get("request") or {}
    action = req.get("action")
    typ = req.get("type")
    action_map = {1: "TRADE", 5: "PENDING"}
    type_map = {
        0: "BUY", 1: "SELL", 2: "BUY_LIMIT", 3: "SELL_LIMIT",
        4: "BUY_STOP", 5: "SELL_STOP", 6: "BUY_STOP_LIMIT", 7: "SELL_STOP_LIMIT",
    }
    human_action = action_map.get(action, str(action) if action is not None else "")
    human_type = type_map.get(typ, str(typ) if typ is not None else "")
    sym = req.get("symbol", "")
    px = req.get("price")
    sl = req.get("sl")
    tp = req.get("tp")
    vol = req.get("volume")
    comment = d.get("request_comment") or req.get("comment", "")
    status = "OK" if ok else "FAIL"
    parts = [f"{leg}", f"{status}", f"{human_action}", f"{human_type}", f"{sym}"]
    if px is not None: parts.append(f"px={px}")
    if vol is not None: parts.append(f"vol={vol}")
    if sl is not None: parts.append(f"SL={sl}")
    if tp is not None: parts.append(f"TP={tp}")
    if rc is not None: parts.append(f"retcode={rc}")
    if deal: parts.append(f"deal={deal}")
    if order: parts.append(f"order={order}")
    if comment: parts.append(f"comment={comment}")
    return "- " + " | ".join([p for p in parts if p])


def render_exec_human(details: Dict[str, Any]) -> str:
    backend = details.get("backend")
    mode = details.get("mode")
    results = details.get("results") or []
    lines = [f"Backend={backend}  Mode={mode}", "Legs:"]
    for item in results:
        lines.append(_render_leg_row(item))
    return "\n".join(lines)


# -------- Spinner UI (console) -----------------------------------------------
class SpinnerUI:
    def __init__(self, stdout, env, *, idle_heartbeat_sec: int = 60):
        import sys as _sys, os as _os, platform, itertools as _it
        self._sys = _sys
        self._os = _os
        self.idle_heartbeat_sec = idle_heartbeat_sec
        self.enabled = (env.get("RUNNER_SPINNER", "1").lower() in ("1","true","yes","on")) and getattr(stdout, "isatty", lambda: False)()
        self.emit_idle_json = (env.get("RUNNER_IDLE_JSON", "0").lower() in ("1","true","yes","on")) and (not self.enabled)

        # Colors & frames
        if platform.system() == "Windows" and env.get("RUNNER_SET_CODEPAGE_UTF8", "0").lower() in ("1","true","yes","on"):
            try: _os.system("chcp 65001 > nul")
            except Exception: pass

        if platform.system() == "Windows":
            self.ansi_ok = self._enable_ansi_windows()
        else:
            term = (env.get("TERM") or "").lower()
            self.ansi_ok = bool(getattr(stdout, "isatty", lambda: False)()) and term not in ("", "dumb")

        STYLE = (env.get("RUNNER_SPINNER_STYLE") or "").lower().strip()
        SPIN_FRAMES = {
            "ascii": ["/","/","-","|"],
            "dots": ["",".","..","..."],
            "bar": ["[    ]","[=   ]","[==  ]","[=== ]","[====]","[ ===]","[  ==]","[   =]"],
        }
        if not STYLE:
            STYLE = "bar" if self.ansi_ok else "ascii"
        frames = SPIN_FRAMES.get(STYLE, SPIN_FRAMES["ascii"])
        if not self.ansi_ok and STYLE not in ("ascii","dots","bar"):
            frames = SPIN_FRAMES["ascii"]
        self._frames = frames
        self._it = _it
        self._cycle = _it.cycle(frames)
        self._frame_w = max(len(f) for f in frames)

        COLOR = (env.get("RUNNER_SPINNER_COLOR") or "bright_green").lower().strip()
        ANSI = {"reset":"\x1b[0m","green":"\x1b[32m","bright_green":"\x1b[92m","cyan":"\x1b[36m",
                "yellow":"\x1b[33m","magenta":"\x1b[35m","white":"\x1b[37m","none":""}
        self._color_on  = ANSI.get(COLOR, ANSI["green"]) if self.ansi_ok and COLOR != "none" else ""
        self._color_off = ANSI["reset"] if self._color_on else ""

        self.header = env.get("RUNNER_SPINNER_TEXT", "Waiting for orders...")
        self._header_printed = False
        self._cursor_hidden = False
        self._logger_filter = None
        self._stdout = stdout

    def _enable_ansi_windows(self) -> bool:
        try:
            import ctypes
            k32 = ctypes.windll.kernel32
            h = k32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            mode = ctypes.c_uint32()
            if k32.GetConsoleMode(h, ctypes.byref(mode)) == 0: return False
            new_mode = mode.value | 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
            return k32.SetConsoleMode(h, new_mode) != 0
        except Exception:
            return False

    def install_log_guard(self, root_logger):
        if not self.enabled:
            return
        import logging as _logging
        ui = self
        class _SpinnerLogGuard(_logging.Filter):
            def filter(self, record):
                ui._clear_block()
                return True
        self._logger_filter = _SpinnerLogGuard()
        root_logger.addFilter(self._logger_filter)

    def remove_log_guard(self, root_logger):
        if self._logger_filter:
            try: root_logger.removeFilter(self._logger_filter)
            except Exception: pass
            self._logger_filter = None

    def step(self):
        if not self.enabled:
            return
        self._start_if_needed()
        raw = next(self._cycle).ljust(self._frame_w)
        vis = f"{self._color_on}{raw}{self._color_off}" if self._color_on else raw
        try:
            self._stdout.write("\r" + vis)
            self._stdout.flush()
        except Exception:
            self.disable()

    def disable(self):
        self._clear_block()

    def cleanup(self, root_logger):
        self.remove_log_guard(root_logger)
        self._show_cursor()

    def _start_if_needed(self):
        if not self._header_printed:
            self._stdout.write(self.header.rstrip() + "\n")
            self._stdout.write(" " * self._frame_w)
            self._stdout.flush()
            self._header_printed = True
            self._hide_cursor()

    def _hide_cursor(self):
        if not (self.ansi_ok and not self._cursor_hidden):
            return
        try:
            self._stdout.write("\x1b[?25l"); self._stdout.flush()
            self._cursor_hidden = True
        except Exception:
            pass

    def _show_cursor(self):
        if not self._cursor_hidden:
            return
        try:
            self._stdout.write("\x1b[?25h"); self._stdout.flush()
            self._cursor_hidden = False
        except Exception:
            pass

    def _clear_block(self):
        if not (self.enabled and self._header_printed):
            return
        if self.ansi_ok:
            self._stdout.write("\r\x1b[2K\x1b[1A\r\x1b[2K\n")
        else:
            self._stdout.write("\r" + (" " * self._frame_w) + "\n")
        self._stdout.flush()
        self._header_printed = False
        self._show_cursor()


# -------- Refindex helpers ----------------------------------------------------
def _ensure_ticket_columns_once():
    if not refindex:
        return
    func = getattr(refindex, "ensure_ticket_columns", None)
    if not callable(func):
        return
    try:
        func()
        log.info("REFINDEX: ensured ticket columns (no-arg)")
    except TypeError:
        get_conn = getattr(refindex, "get_connection", None)
        if callable(get_conn):
            try:
                conn = get_conn()
                func(conn)  # type: ignore[misc]
                log.info("REFINDEX: ensured ticket columns (with-conn)")
            except Exception as e:
                log.warning("REFINDEX: ensure_ticket_columns failed with-conn: %s", e, extra={"event": "REFINDEX_INIT_WARN"})
        else:
            log.warning("REFINDEX: ensure_ticket_columns requires a connection but get_connection() not found", extra={"event": "REFINDEX_INIT_WARN"})
    except Exception as e:
        log.warning("REFINDEX: ensure_ticket_columns error: %s", e, extra={"event": "REFINDEX_INIT_WARN"})


def _apply_open_result_safe(action: Action, result: RouterResult):
    if not refindex:
        return
    func = getattr(refindex, "apply_open_result", None)
    if not callable(func):
        alt = getattr(refindex, "on_open_executed", None)
        if callable(alt):
            try:
                alt(action, result)  # type: ignore[misc]
                log.info("REFINDEX: applied open result via on_open_executed()")
            except Exception as e:
                log.error("REFINDEX: on_open_executed failed: %s", e, extra={"event": "OPEN_TICKETS_SAVE_ERROR", "action_id": action.action_id})
        return
    try:
        func(action, result)  # type: ignore[misc]
        log.info("REFINDEX: applied open result (no-conn)", extra={"event": "OPEN_TICKETS_SAVED", "action_id": action.action_id})
        return
    except TypeError:
        get_conn = getattr(refindex, "get_connection", None)
        if callable(get_conn):
            try:
                conn = get_conn()
                func(conn, action, result)  # type: ignore[misc]
                log.info("REFINDEX: applied open result (with-conn)", extra={"event": "OPEN_TICKETS_SAVED", "action_id": action.action_id})
                return
            except Exception as e:
                log.error("REFINDEX: apply_open_result failed with-conn: %s", e, extra={"event": "OPEN_TICKETS_SAVE_ERROR", "action_id": action.action_id})
        else:
            log.warning("REFINDEX: apply_open_result requires a connection but get_connection() not found", extra={"event": "OPEN_TICKETS_SAVE_WARN", "action_id": action.action_id})
    except Exception as e:
        log.error("REFINDEX: apply_open_result failed: %s", e, extra={"event": "OPEN_TICKETS_SAVE_ERROR", "action_id": action.action_id})


# -------- Poll interval -------------------------------------------------------
def _resolve_poll_seconds(poll_seconds_arg: Optional[float]) -> float:
    import os as _os
    if poll_seconds_arg is not None:
        val = float(poll_seconds_arg)
    else:
        env = _os.environ
        val: Optional[float] = None
        if "RUNNER_POLL_MS" in env:
            try:
                val = float(env["RUNNER_POLL_MS"]) / 1000.0
            except Exception:
                val = None
        if val is None and "RUNNER_POLL_SECONDS" in env:
            try:
                val = float(env["RUNNER_POLL_SECONDS"])
            except Exception:
                val = None
        if val is None:
            val = 0.05
    return max(0.01, val)


# -------- Core processing -----------------------------------------------------
from collections import Counter

def process_once(router, batch: int = 32) -> int:
    actions: List[Action] = storage.fetch_batch(limit=batch)
    processed = 0
    for action in actions:
        prior = storage.already_executed(action.action_id)
        if prior:
            storage.mark_done(action.action_id, prior)
            log.info("DEDUP", extra={"event": "DEDUP", "action_id": action.action_id, "status": prior.status})
            continue

        storage.mark_in_progress(action.action_id)
        result: RouterResult = router.execute(action)
        storage.mark_done(action.action_id, result)

        try:
            if getattr(action, "type", None) == "OPEN" and getattr(result, "status", None) == "OK":
                _apply_open_result_safe(action, result)
        except Exception as e:  # pragma: no cover
            log.error("OPEN_TICKETS_POSTPROC_FAIL %s", e, extra={"event": "OPEN_TICKETS_SAVE_ERROR", "action_id": action.action_id})

        # Print human block directly to console (multiline)
        exec_payload = (result.details or {})
        try:
            import sys as _sys
            _sys.stdout.write("\nEXECUTED\n" + render_exec_human(exec_payload) + "\n")
            _sys.stdout.flush()
        except Exception:
            pass
        # Machine JSON for downstream
        try:
            payload_for_jsonl = {"action_id": action.action_id}
            payload_for_jsonl.update(exec_payload)
            log.debug("EXECUTED_JSON", extra={"json_payload": payload_for_jsonl})
        except Exception:
            pass

        # Keep the concise summary (INFO) — fine if your formatter is JSON
        try:
            details = result.details or {}
            mode = str(details.get("mode", "NA"))
            results_list = details.get("results", []) or []
            n_legs = len(results_list)
            oks = sum(1 for r in results_list if r.get("result", {}).get("ok") is True)
            fails = n_legs - oks
            symbols = []
            retcodes = []
            for r in results_list:
                det = (r.get("result") or {}).get("details", {}) or {}
                req = det.get("request", {}) or {}
                sym = req.get("symbol")
                if sym: symbols.append(sym)
                rc = det.get("retcode_label")
                if rc: retcodes.append(rc)
            sym_set = ",".join(sorted(set(symbols))) if symbols else "NA"
            rc_counts = Counter(retcodes)
            rc_compact = ",".join(f"{k}:{v}" for k, v in rc_counts.items()) if rc_counts else "NA"
            summary_line = (
                f"EXEC_SUMMARY action={action.action_id} mode={mode} "
                f"legs={n_legs} ok={oks} fail={fails} symbols={sym_set} retcodes={rc_compact}"
            )
            # Bright orange to console
            import sys
            sys.stdout.write(f"\033[38;5;208m{summary_line}\033[0m\n")

            # Still send it to logs for JSON collectors if you want
            log.debug("EXEC_SUMMARY_JSON", extra={
                "json_payload": {
                    "action_id": action.action_id,
                    "mode": mode,
                    "legs": n_legs,
                    "ok": oks,
                    "fail": fails,
                    "symbols": sym_set,
                    "retcodes": rc_compact,
                }
            })
        except Exception:
            pass

        processed += 1

    return processed


# -------- Runner loop ---------------------------------------------------------

def _execute_with_timeout(router, action, timeout_s: int):
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="exec") as ex:
        fut = ex.submit(router.execute, action)
        return fut.result(timeout=timeout_s)

def run_forever(poll_seconds: float | None = None, batch: int = 32, idle_heartbeat_every: int = 60):
    # DB path & stale rescue
    db_path = storage.get_db_path()
    try:
        log.info("RUNNER_DB_PATH", extra={"event": "DB_PATH", "path": db_path})
    except Exception:
        pass
    try:
        rescued = storage.reset_in_progress_to_pending()
        if rescued:
            log.info("RUNNER_RESCUED", extra={"event": "RESCUE_IN_PROGRESS", "count": rescued})
    except Exception:
        pass
    last_heartbeat = 0

    os.makedirs("runtime/logs", exist_ok=True)
    log.info("RUNNER_STARTED", extra={"event": "RUNNER_STARTED"})
    _ensure_ticket_columns_once()

    router = get_router()
    
    # Start position polling if enabled (NEW FOR RISK-FREE)
    if start_position_polling:
        poller = start_position_polling(router)
        if poller:
            log.info("Position polling started for risk-free management")
    else:
        log.info("Position polling not available - risk-free features disabled")

    import sys as _sys, logging as _logging
    spinner = SpinnerUI(stdout=_sys.stdout, env=os.environ, idle_heartbeat_sec=idle_heartbeat_every)
    root_logger = _logging.getLogger()
    spinner.install_log_guard(root_logger)
    attach_jsonl_debug_logger("actions_runner", "runtime/logs/actions_runner.jsonl")

    poll = _resolve_poll_seconds(poll_seconds)

    idle_acc = 0.0
    try:
        while True:
            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_SECS:
                try:
                    counts = storage.queue_counts()
                except Exception:
                    counts = {}
                try:
                    log.info("HEALTH", extra={"event": "HEALTH", "counts": counts, "exec_timeout": EXEC_TIMEOUT_SECS, "db_path": db_path})
                except Exception:
                    pass
                last_heartbeat = now

            n = process_once(router=router, batch=batch)
            if n == 0:
                idle_acc += poll
                if spinner.emit_idle_json and idle_acc >= idle_heartbeat_every:
                    log.info("IDLE", extra={"event": "IDLE", "pending": 0})
                    idle_acc = 0.0
                spinner.step()
                time.sleep(poll)
            else:
                idle_acc = 0.0
    finally:
        spinner.cleanup(root_logger)