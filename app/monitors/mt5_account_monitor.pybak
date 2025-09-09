# app/monitors/mt5_account_monitor.py
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

import typer

from app.common.config import Config
from app.common.logging_config import setup_logging
from app.infra.mt5_router import Mt5NativeRouter
from .actions import Action, execute_actions, Alert
from .user_monitors import MONITORS

app = typer.Typer(no_args_is_help=True)
log = logging.getLogger("mt5_account_monitor")

@dataclass
class MonitorConfig:
    heartbeat_symbol: str = "XAUUSD"
    interval_sec: float = 2
    write_json: bool = True
    write_csv: bool = True
    symbols_filter: Optional[List[str]] = None  # None=all; else limit to these uppercase names

def _ensure_dirs(cfg: Config) -> Tuple[str, str]:
    base = os.path.join(cfg.OUTPUT_BASE, "monitor")
    os.makedirs(base, exist_ok=True)
    return base, base  # both json/csv into same base

# === Comment parsing ===
# Current (router) shape from _comment_key(): "<msgId>_<legIdx>:<SYM>" or "<msgId>_<legIdx>"
# Historic shapes occasionally had '#': "<msgId>#<legIdx>:<SYM>"
# Be lenient: allow either '_' or '#', message id of any length, optional ':+SYM'
_comment_key_res = [
    re.compile(r'(?<!\d)(?P<msg>\d+)[_#](?P<leg>\d+)(?::(?P<sym>[A-Za-z0-9+._-]+))?'),
]

def _parse_msg_leg_from_comment(comment: Optional[str]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Extract (message_id, leg, sym_suffix) from MT5 comment produced by _comment_key():
      "<msgId>_<legIdx>:<SYM>" or "<msgId>_<legIdx>"  (and accept '#')
    Returns (message_id:str|None, leg:int|None, sym_suffix:str|None).
    """
    if not comment:
        return None, None, None
    s = str(comment)
    for rx in _comment_key_res:
        m = rx.search(s)
        if m:
            msg = m.group("msg")
            try:
                leg = int(m.group("leg"))
            except Exception:
                leg = None
            sym_suffix = m.group("sym") if m.group("sym") else None
            return msg, leg, sym_suffix
    return None, None, None

def _summarise_positions(mt5, symbols_filter: Optional[List[str]]) -> List[dict]:
    try:
        raw = mt5.positions_get() or []
    except Exception:
        raw = []
    out: List[dict] = []
    for p in raw:
        try:
            sym = getattr(p, "symbol", "")
            if symbols_filter and sym.upper() not in symbols_filter:
                continue
            side = "BUY" if getattr(p, "type", 0) == mt5.POSITION_TYPE_BUY else "SELL"
            comment = getattr(p, "comment", "") or ""
            msg_id, leg, sym_suffix = _parse_msg_leg_from_comment(comment)
            out.append({
                "kind": "position",
                "ticket": int(getattr(p, "ticket", 0)),
                "symbol": sym,
                "side": side,
                "volume": float(getattr(p, "volume", 0.0)),
                "price_open": float(getattr(p, "price_open", 0.0)),
                "sl": float(getattr(p, "sl", 0.0)) if getattr(p, "sl", 0.0) else None,
                "tp": float(getattr(p, "tp", 0.0)) if getattr(p, "tp", 0.0) else None,
                "price_current": float(getattr(p, "price_current", 0.0)),
                "profit": float(getattr(p, "profit", 0.0)),
                "comment": comment,
                "message_id": msg_id,
                "leg": leg,
                "comment_sym_suffix": sym_suffix,
                "time": int(getattr(p, "time", 0)),
                "magic": int(getattr(p, "magic", 0)),
            })
        except Exception:
            continue
    return out

def _summarise_orders(mt5, symbols_filter: Optional[List[str]]) -> List[dict]:
    try:
        raw = mt5.orders_get() or []
    except Exception:
        raw = []
    out: List[dict] = []
    for o in raw:
        try:
            sym = getattr(o, "symbol", "")
            if symbols_filter and sym.upper() not in symbols_filter:
                continue
            t = getattr(o, "type", None)
            type_label = {
                mt5.ORDER_TYPE_BUY_LIMIT: "BUY_LIMIT",
                mt5.ORDER_TYPE_SELL_LIMIT: "SELL_LIMIT",
                mt5.ORDER_TYPE_BUY_STOP: "BUY_STOP",
                mt5.ORDER_TYPE_SELL_STOP: "SELL_STOP",
                mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY_STOP_LIMIT",
                mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL_STOP_LIMIT",
            }.get(t, str(t))
            comment = getattr(o, "comment", "") or ""
            msg_id, leg, sym_suffix = _parse_msg_leg_from_comment(comment)
            out.append({
                "kind": "order",
                "ticket": int(getattr(o, "ticket", 0)),
                "symbol": sym,
                "type": type_label,
                "side": "BUY" if "BUY" in type_label else "SELL" if "SELL" in type_label else None,
                "volume": float(getattr(o, "volume_current", getattr(o, "volume_initial", 0.0))),
                "price_open": float(getattr(o, "price_open", 0.0)),
                "sl": float(getattr(o, "sl", 0.0)) if getattr(o, "sl", 0.0) else None,
                "tp": float(getattr(o, "tp", 0.0)) if getattr(o, "tp", 0.0) else None,
                "comment": comment,
                "message_id": msg_id,
                "leg": leg,
                "comment_sym_suffix": sym_suffix,
                "time_setup": int(getattr(o, "time_setup", 0)),
                "expiration": int(getattr(o, "time_expiration", 0)),
                "magic": int(getattr(o, "magic", 0)),
            })
        except Exception:
            continue
    return out

def _aggregate_summary(rows: List[dict]) -> Dict[str, dict]:
    per_symbol: Dict[str, dict] = {}
    for r in rows:
        sym = r.get("symbol") or "NA"
        ps = per_symbol.setdefault(sym, {
            "symbol": sym,
            "positions": {"count": 0, "buy": 0, "sell": 0, "vol": 0.0, "profit": 0.0},
            "orders": {"count": 0, "buy": 0, "sell": 0, "vol": 0.0},
        })
        if r["kind"] == "position":
            ps["positions"]["count"] += 1
            ps["positions"]["vol"] += float(r.get("volume") or 0.0)
            ps["positions"]["profit"] += float(r.get("profit") or 0.0)
            if r.get("side") == "BUY":
                ps["positions"]["buy"] += 1
            elif r.get("side") == "SELL":
                ps["positions"]["sell"] += 1
        else:
            ps["orders"]["count"] += 1
            ps["orders"]["vol"] += float(r.get("volume") or 0.0)
            side = r.get("side")
            if side == "BUY":
                ps["orders"]["buy"] += 1
            elif r.get("side") == "SELL":
                ps["orders"]["sell"] += 1
    return per_symbol

def _group_by_msg_leg(positions: List[Dict[str, Any]], orders: List[Dict[str, Any]]) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """
    Build an index keyed by (message_id, leg) with rollups:
      counts, volumes, PnL, symbols.
    Keys use 'UNKNOWN' and -1 when not parsable.
    """
    idx: Dict[Tuple[str, int], Dict[str, Any]] = defaultdict(lambda: {
        "positions_count": 0,
        "orders_count": 0,
        "pos_volume": 0.0,
        "ord_volume": 0.0,
        "pos_profit": 0.0,
        "symbols": set(),
    })

    for p in positions:
        mid = p.get("message_id") or "UNKNOWN"
        leg = p.get("leg")
        key = (mid, leg if isinstance(leg, int) else -1)
        row = idx[key]
        row["positions_count"] += 1
        row["pos_volume"] += float(p.get("volume") or 0.0)
        row["pos_profit"] += float(p.get("profit") or 0.0)
        if p.get("symbol"):
            row["symbols"].add(p["symbol"])

    for o in orders:
        mid = o.get("message_id") or "UNKNOWN"
        leg = o.get("leg")
        key = (mid, leg if isinstance(leg, int) else -1)
        row = idx[key]
        row["orders_count"] += 1
        row["ord_volume"] += float(o.get("volume") or 0.0)
        if o.get("symbol"):
            row["symbols"].add(o["symbol"])

    for row in idx.values():
        row["symbols"] = ",".join(sorted(row["symbols"])) if row["symbols"] else ""

    return idx

def _render_msg_leg_table(idx: Dict[Tuple[str, int], Dict[str, Any]]) -> str:
    """
    Render a pretty fixed-width table for the console and .txt snapshot.
    """
    headers = ["message_id", "leg", "pos_ct", "ord_ct", "pos_vol", "ord_vol", "pos_pnl", "symbols"]
    colw = [14, 5, 6, 6, 9, 9, 10, 22]

    def fmt_row(row_vals):
        return " ".join(str(v).ljust(w) for v, w in zip(row_vals, colw))

    lines = []
    lines.append(fmt_row(headers))
    lines.append("-" * (sum(colw) + len(colw) - 1))

    def sort_key(k):
        mid, leg = k
        return (1, "", 10**9) if mid == "UNKNOWN" else (0, mid, leg if isinstance(leg, int) else 10**9)

    for (mid, leg), row in sorted(idx.items(), key=sort_key):
        symbols = row["symbols"]
        if len(symbols) > colw[-1]:
            symbols = symbols[:colw[-1]-1] + "…"
        lines.append(fmt_row([
            mid, leg,
            row["positions_count"], row["orders_count"],
            f'{row["pos_volume"]:.2f}', f'{row["ord_volume"]:.2f}',
            f'{row["pos_profit"]:.2f}',
            symbols
        ]))

    return "\n".join(lines)

# === Human-friendly grouped console renderer (ASCII-only, line-by-line) ===

def _format_legs(legs: List[int]) -> str:
    """Turn [1,2,3,5,7,8] into '1-3,5,7-8' (or '-' if empty)."""
    if not legs:
        return "-"
    legs = sorted(set(int(x) for x in legs if isinstance(x, int) and x >= 0))
    if not legs:
        return "-"
    ranges = []
    start = prev = legs[0]
    for x in legs[1:]:
        if x == prev + 1:
            prev = x
            continue
        ranges.append((start, prev))
        start = prev = x
    ranges.append((start, prev))
    parts = [f"{a}-{b}" if a != b else f"{a}" for a, b in ranges]
    return ",".join(parts)

def _render_msg_grouped_console_lines(idx: Dict[Tuple[str, int], Dict[str, Any]], max_messages: int = 8) -> List[str]:
    """
    Console-friendly, grouped-by-message view with per-message totals + leg lists.
    Returns a list of ASCII-only lines; caller should log each line separately.
    """
    # Build per-message aggregates
    groups: Dict[str, Dict[str, Any]] = {}
    for (mid, leg), row in idx.items():
        mid_key = mid or "UNKNOWN"
        g = groups.setdefault(mid_key, {
            "pos_ct": 0, "ord_ct": 0,
            "pos_vol": 0.0, "ord_vol": 0.0,
            "pos_pnl": 0.0,
            "pos_legs": set(), "ord_legs": set(),
            "symbols": set(),
        })
        if row.get("positions_count", 0) > 0:
            g["pos_ct"] += row["positions_count"]
            g["pos_vol"] += row.get("pos_volume", 0.0) or 0.0
            g["pos_pnl"] += row.get("pos_profit", 0.0) or 0.0
            if isinstance(leg, int) and leg >= 0:
                g["pos_legs"].add(leg)
        if row.get("orders_count", 0) > 0:
            g["ord_ct"] += row["orders_count"]
            g["ord_vol"] += row.get("ord_volume", 0.0) or 0.0
            if isinstance(leg, int) and leg >= 0:
                g["ord_legs"].add(leg)
        # symbols is a comma list on each row; merge them
        syms = row.get("symbols") or ""
        if syms:
            for s in syms.split(","):
                s = s.strip()
                if s:
                    g["symbols"].add(s)

    # Sort: numeric msg ids first (ascending), then non-numeric, then UNKNOWN
    def sort_key(mid: str):
        if mid == "UNKNOWN":
            return (2, "", 0)
        if mid.isdigit():
            return (0, "", int(mid))
        return (1, mid, 0)

    mids_sorted = sorted(groups.keys(), key=sort_key)
    total_groups = len(mids_sorted)

    # Limit in-console output
    mids_display = mids_sorted[:max_messages]
    truncated = total_groups - len(mids_display)

    lines: List[str] = []
    lines.append("BY MESSAGE (grouped view) - pos=positions  ord=pending orders")
    lines.append("-----------------------------------------------------------------")
    for mid in mids_display:
        g = groups[mid]
        sym_str = ",".join(sorted(g["symbols"])) if g["symbols"] else ""
        header = (
            f"MSG {mid:<8} | "
            f"POS ct={g['pos_ct']:<2} vol={g['pos_vol']:.2f} pnl={g['pos_pnl']:.2f}  | "
            f"ORD ct={g['ord_ct']:<2} vol={g['ord_vol']:.2f}  | "
            f"SYM {sym_str}"
        )
        lines.append(header)
        lines.append(f"  pos_legs: {_format_legs(list(g['pos_legs']))}")
        lines.append(f"  ord_legs: {_format_legs(list(g['ord_legs']))}")
    if truncated > 0:
        lines.append(f"... {truncated} more group(s) (see current_by_msg_leg.txt for full list)")
    return lines

def _emit_console_lines(lines: List[str], plain: bool = True) -> None:
    """Write each line either via print([HH:MM:SS] ...) or logger, to avoid JSON escaping."""
    if not lines:
        return
    if plain:
        ts = time.strftime("%H:%M:%S", time.localtime())
        for line in lines:
            try:
                print(f"[{ts}] {line}")
            except Exception:
                log.info(line)
    else:
        for line in lines:
            log.info(line)

def _emit_console_line(line: str, plain: bool = True) -> None:
    _emit_console_lines([line], plain)

def _write_msg_leg_outputs(base_dir: str, idx: Dict[Tuple[str,int],Dict[str,Any]]) -> None:
    """
    Write CSV + TXT snapshots for the (message_id, leg) breakdown.
    """
    import csv
    csv_path = os.path.join(base_dir, "current_by_msg_leg.csv")
    txt_path = os.path.join(base_dir, "current_by_msg_leg.txt")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["message_id", "leg", "positions_count", "orders_count", "pos_volume", "ord_volume", "pos_profit", "symbols"])
        for (mid, leg), row in sorted(idx.items(), key=lambda k: (k[0] == "UNKNOWN", k[0], k[1] if isinstance(k[1], int) else 10**9)):
            w.writerow([
                mid, leg,
                row["positions_count"], row["orders_count"],
                f'{row["pos_volume"]:.2f}', f'{row["ord_volume"]:.2f}',
                f'{row["pos_profit"]:.2f}', row["symbols"]
            ])

    table = _render_msg_leg_table(idx)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(table)

def _write_outputs(base_dir: str, positions: List[dict], orders: List[dict], aggregate: Dict[str,dict], write_json=True, write_csv=True):
    ts = int(time.time())
    if write_json:
        with open(os.path.join(base_dir, "current_positions.json"), "w", encoding="utf-8") as f:
            json.dump(positions, f, ensure_ascii=False, indent=2)
        with open(os.path.join(base_dir, "current_orders.json"), "w", encoding="utf-8") as f:
            json.dump(orders, f, ensure_ascii=False, indent=2)
        with open(os.path.join(base_dir, "current_summary.json"), "w", encoding="utf-8") as f:
            json.dump({"ts": ts, "symbols": list(aggregate.values())}, f, ensure_ascii=False, indent=2)
    if write_csv:
        import csv
        with open(os.path.join(base_dir, "current_positions.csv"), "w", newline="", encoding="utf-8") as f:
            cols = list(positions[0].keys()) if positions else [
                "kind","ticket","symbol","side","volume","price_open","sl","tp","price_current","profit",
                "comment","message_id","leg","comment_sym_suffix","time","magic"
            ]
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in positions:
                w.writerow(r)
        with open(os.path.join(base_dir, "current_orders.csv"), "w", newline="", encoding="utf-8") as f:
            cols = list(orders[0].keys()) if orders else [
                "kind","ticket","symbol","type","side","volume","price_open","sl","tp",
                "comment","message_id","leg","comment_sym_suffix","time_setup","expiration","magic"
            ]
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in orders:
                w.writerow(r)

def _next_tick(mt5, heartbeat_symbol: str, prev_time: Optional[int]) -> Optional[int]:
    """Poll the heartbeat symbol's last tick time; return new time if advanced."""
    try:
        tick = mt5.symbol_info_tick(heartbeat_symbol)
        t = int(getattr(tick, "time_msc", 0) or getattr(tick, "time", 0) or 0)
    except Exception:
        t = 0
    if not prev_time or t > prev_time:
        return t
    return None

def _symbols_filter_from_env() -> Optional[List[str]]:
    raw = os.environ.get("MONITOR_SYMBOLS", "").strip()
    if not raw:
        return None
    out = []
    for tok in re.split(r"[\s,;]+", raw):
        tok = tok.strip().upper()
        if tok:
            out.append(tok)
    return out or None

def _debug_dump_comments(base_dir: str, positions: List[dict], orders: List[dict], idx: Dict[Tuple[str,int],Dict[str,Any]]) -> None:
    """
    When any UNKNOWN/-1 exists, write a small sample of comments to disk for inspection.
    """
    has_unknown = any(k[0] == "UNKNOWN" or k[1] == -1 for k in idx.keys())
    if not has_unknown:
        return
    path = os.path.join(base_dir, "current_comments_sample.txt")
    lines = []
    lines.append("== POSITIONS ==")
    for p in positions[:50]:
        lines.append(f'ticket={p.get("ticket")} sym={p.get("symbol")} comment="{p.get("comment")}" '
                     f'-> mid={p.get("message_id")} leg={p.get("leg")} sym_sfx={p.get("comment_sym_suffix")}')
    lines.append("\n== ORDERS ==")
    for o in orders[:50]:
        lines.append(f'ticket={o.get("ticket")} sym={o.get("symbol")} comment="{o.get("comment")}" '
                     f'-> mid={o.get("message_id")} leg={o.get("leg")} sym_sfx={o.get("comment_sym_suffix")}')
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
    except Exception:
        pass

def _format_actions_console(actions, exec_results, applied: bool, max_lines: int = 8) -> List[str]:
    lines: List[str] = []
    if not actions:
        return lines
    applied_str = "APPLIED" if applied else "DRY-RUN"
    lines.append(f"ACTIONS [{applied_str}] planned={len(actions)}")
    shown = 0
    for a in actions[:max_lines]:
        t = a.__class__.__name__
        if t == "Alert":
            lines.append(f"  [ALERT] {getattr(a, 'message', '')}")
        elif t == "DeleteOrder":
            lines.append(f"  DeleteOrder ticket={getattr(a, 'ticket', '?')} reason={getattr(a, 'reason', '')}")
        elif t == "ModifySLTP":
            sl = getattr(a, 'sl', None); tp = getattr(a, 'tp', None)
            lines.append(f"  ModifySLTP ticket={getattr(a, 'ticket', '?')} sl={sl} tp={tp} reason={getattr(a, 'reason', '')}")
        elif t == "ClosePosition":
            vol = getattr(a, 'volume', None)
            lines.append(f"  ClosePosition ticket={getattr(a, 'ticket', '?')} volume={vol} reason={getattr(a, 'reason', '')}")
        else:
            lines.append(f"  {t} {a.to_dict()}")
        shown += 1
    if len(actions) > shown:
        lines.append(f"  … {len(actions)-shown} more")
    if exec_results:
        ok = sum(1 for r in exec_results if r.ok)
        fail = len(exec_results) - ok
        lines.append(f"ACTIONS RESULT ok={ok} fail={fail}")
    return lines

@app.command()
def run(log_level: str = typer.Option("INFO", "--log-level", "-l"),
        heartbeat_symbol: str = typer.Option(None, "--heartbeat", "-h"),
        interval_sec: float = typer.Option(None, "--interval"),
        plain_console: bool = typer.Option(True, "--plain-console/--no-plain-console", help="Bypass JSON logger and print with time only"),
        apply_actions: bool = typer.Option(False, "--apply-actions/--dry-run", help="Actually execute planned actions"),
        show_grouped: bool = typer.Option(True, "--grouped/--no-grouped", help="Show grouped-by-message breakdown in console")):
    """Run the MT5 account monitor (summarises positions & pending orders on each tick)."""
    setup_logging(log_level)
    cfg = Config()
    base_dir, _ = _ensure_dirs(cfg)

    # Respect env overrides
    hb = heartbeat_symbol or os.environ.get("MONITOR_HEARTBEAT_SYMBOL") or "XAUUSD"
    iv = interval_sec or float(os.environ.get("MONITOR_INTERVAL_SEC", "0.25"))
    sym_filter = _symbols_filter_from_env()
    # env overrides for console behavior
    if os.environ.get("MONITOR_PLAIN_CONSOLE") is not None:
        plain_console = os.environ.get("MONITOR_PLAIN_CONSOLE", "1").lower() in ("1","true","yes","on")
    if os.environ.get("MONITOR_SHOW_GROUPED") is not None:
        show_grouped = os.environ.get("MONITOR_SHOW_GROUPED", "1").lower() in ("1","true","yes","on")

    # Force native backend; connect via router (reuses env/Config/mt5 setup)
    os.environ.setdefault("ROUTER_BACKEND", "native")
    router = Mt5NativeRouter(cfg)
    mt5 = router.mt5

    # Ensure heartbeat symbol is visible
    try:
        router._ensure_symbol(hb)
    except Exception:
        pass

    log.info("MONITOR_STARTED | heartbeat=%s interval=%.3fs filter=%s out=%s", hb, iv, sym_filter, base_dir)

    last_tick_time: Optional[int] = None
    last_log_time: float = 0.0  # for throttling log prints

    while True:
        # Wait for next tick (or fallback to interval sleep)
        new_t = _next_tick(mt5, hb, last_tick_time)
        if new_t is None:
            time.sleep(iv)
            continue
        last_tick_time = new_t

        positions = _summarise_positions(mt5, sym_filter)
        orders = _summarise_orders(mt5, sym_filter)
        aggregate = _aggregate_summary(positions + orders)

        # message_id/leg breakdown
        msg_leg_idx = _group_by_msg_leg(positions, orders)
        _write_msg_leg_outputs(base_dir, msg_leg_idx)
        _debug_dump_comments(base_dir, positions, orders, msg_leg_idx)

        _write_outputs(base_dir, positions, orders, aggregate, write_json=True, write_csv=True)

        # === Evaluate user monitors -> build an actions plan ===
        env_map = {k: v for k, v in os.environ.items() if k.startswith("MON_") or k.startswith("MONITOR_")}
        ctx = {
            "now_ts": int(time.time()),
            "env": env_map,
            "router": router,
            "mt5": mt5,
        }
        planned_actions: List[Action] = []
        for mon in MONITORS:
            try:
                acts = mon.evaluate(positions, orders, ctx) or []
                planned_actions.extend(acts)
            except Exception as e:
                # don't crash the loop on monitor error
                planned_actions.append(Alert(f"[monitor_error] {getattr(mon, 'name', mon.__class__.__name__)}: {e}"))

        # Write actions plan snapshots
        actions_dir = base_dir
        with open(os.path.join(actions_dir, "current_actions_plan.json"), "w", encoding="utf-8") as f:
            json.dump([a.to_dict() for a in planned_actions], f, ensure_ascii=False, indent=2)
        with open(os.path.join(actions_dir, "current_actions_plan.txt"), "w", encoding="utf-8") as f:
            for a in planned_actions:
                f.write(str(a.to_dict()) + "\n")

        # Optionally execute
        exec_results = execute_actions(mt5, router, planned_actions, apply=bool(apply_actions))
        with open(os.path.join(actions_dir, "current_actions_exec.json"), "w", encoding="utf-8") as f:
            json.dump([{"ok": r.ok, "action": r.action.to_dict(), "details": r.details} for r in exec_results], f, ensure_ascii=False, indent=2)

        # Show actions summary immediately after execution
        _emit_console_lines(_format_actions_console(planned_actions, exec_results, bool(apply_actions)), plain_console)

        # Human one-liner for logs + grouped console view (every 5 seconds)
        now = time.time()
        if now - last_log_time >= 5:
            total_pos = sum(v["positions"]["count"] for v in aggregate.values())
            total_ord = sum(v["orders"]["count"] for v in aggregate.values())
            pnl = sum(v["positions"]["profit"] for v in aggregate.values())
            _emit_console_line(f"SNAPSHOT | pos={total_pos} ord={total_ord} pnl={pnl:.2f} symbols={len(aggregate)}", plain_console)

            if show_grouped:
                lines = _render_msg_grouped_console_lines(msg_leg_idx, max_messages=int(os.environ.get("MONITOR_GROUPED_MAX", "8")))
                _emit_console_lines(lines, plain_console)

            last_log_time = now


if __name__ == "__main__":
    app()
