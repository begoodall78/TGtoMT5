# app/monitors/actions.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import inspect

@dataclass
class Action:
    """Base action. All concrete actions must implement .to_dict()."""
    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.__class__.__name__}

# --- Concrete actions ---

@dataclass
class ClosePosition(Action):
    ticket: int
    volume: Optional[float] = None  # None = full
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "ClosePosition", "ticket": self.ticket, "volume": self.volume, "reason": self.reason}

@dataclass
class DeleteOrder(Action):
    ticket: int
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "DeleteOrder", "ticket": self.ticket, "reason": self.reason}

@dataclass
class ModifySLTP(Action):
    ticket: int
    sl: Optional[float] = None
    tp: Optional[float] = None
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "ModifySLTP", "ticket": self.ticket, "sl": self.sl, "tp": self.tp, "reason": self.reason}

@dataclass
class Alert(Action):
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {"type": "Alert", "message": self.message}

# --- Executor ---

@dataclass
class ExecResult:
    ok: bool
    action: Action
    details: Dict[str, Any]

def _retcode_name(mt5, code: int) -> str:
    try:
        return str(code)
    except Exception:
        return str(code)

def _router_call(router, names: List[str]):
    for n in names:
        fn = getattr(router, n, None)
        if callable(fn):
            return fn
    return None

def _get_order_by_ticket(mt5, ticket: int):
    try:
        arr = mt5.orders_get(ticket=ticket)
        if arr and len(arr) > 0:
            return arr[0]
    except Exception:
        pass
    return None

def _get_position_by_ticket(mt5, ticket: int):
    try:
        arr = mt5.positions_get(ticket=ticket)
        if arr and len(arr) > 0:
            return arr[0]
    except Exception:
        pass
    return None

def _delete_order(mt5, router, ticket: int) -> Dict[str, Any]:
    # 1) Router helper
    fn = _router_call(router, ["delete_order", "cancel_order", "order_delete"])
    if fn:
        try:
            ok = bool(fn(ticket))
            return {"ok": ok, "via": fn.__name__}
        except Exception as e:
            return {"ok": False, "via": fn.__name__, "error": str(e)}

    # 2) Direct mt5.order_delete
    try:
        res = mt5.order_delete(ticket)
        if isinstance(res, bool):
            return {"ok": res, "via": "mt5.order_delete"}
        if hasattr(res, "retcode"):
            ret = int(getattr(res, "retcode", 0))
            return {"ok": ret == getattr(mt5, "TRADE_RETCODE_DONE", 10009),
                    "via": "mt5.order_delete", "retcode": ret, "comment": getattr(res, "comment", "")}
    except Exception:
        pass

    # 3) Fallback: order_send(TRADE_ACTION_REMOVE)
    ord_obj = _get_order_by_ticket(mt5, ticket)
    symbol = getattr(ord_obj, "symbol", None) if ord_obj else None
    req = {
        "action": getattr(mt5, "TRADE_ACTION_REMOVE", 3),
        "order": int(ticket),
    }
    if symbol:
        req["symbol"] = symbol
    try:
        res = mt5.order_send(req)
        ret = int(getattr(res, "retcode", 0))
        return {"ok": ret == getattr(mt5, "TRADE_RETCODE_DONE", 10009),
                "via": "mt5.order_send(REMOVE)", "retcode": ret, "retname": _retcode_name(mt5, ret),
                "comment": getattr(res, "comment", ""), "request": req}
    except Exception as e:
        return {"ok": False, "via": "mt5.order_send(REMOVE)", "error": str(e), "request": req}

def _find_param_name(candidates: List[str], params: Dict[str, inspect.Parameter]) -> Optional[str]:
    """
    Return the first parameter name in 'params' that matches any candidate (case-insensitive).
    """
    pset = {k.lower(): k for k in params.keys()}
    for c in candidates:
        k = pset.get(c.lower())
        if k:
            return k
    return None

def _modify_sltp(mt5, router, ticket: int, sl: Optional[float], tp: Optional[float]) -> Dict[str, Any]:
    """
    Robust SL/TP modify:
      - Read current SL/TP.
      - Fill missing side (SL or TP) with the current on-broker value.
      - Send BOTH values to avoid brokers/routers clearing the omitted field.
    """
    pos = _get_position_by_ticket(mt5, ticket)
    if not pos:
        return {"ok": False, "via": "mt5.order_send(SLTP)", "error": "position not found by ticket"}

    symbol = getattr(pos, "symbol", None)
    cur_sl = float(getattr(pos, "sl", 0.0) or 0.0)
    cur_tp = float(getattr(pos, "tp", 0.0) or 0.0)

    # Preserve existing values when a field is not explicitly changed
    final_sl = float(sl) if sl is not None else cur_sl
    final_tp = float(tp) if tp is not None else cur_tp

    # 1) Try router helper — but only if we can confidently map both SL & TP param names.
    fn = _router_call(router, ["modify_sltp", "position_modify_sltp", "set_sltp"])
    if fn and symbol:
        try:
            sig = inspect.signature(fn)
            params = sig.parameters

            # Common name variants for SL/TP in router helpers
            name_sl = _find_param_name(["sl", "stop_loss", "stoploss"], params)
            name_tp = _find_param_name(["tp", "take_profit", "takeprofit"], params)
            name_ticket = _find_param_name(["ticket", "position", "pos", "id"], params)
            name_symbol = _find_param_name(["symbol", "sym"], params)

            kwargs = {}
            if name_ticket:
                kwargs[name_ticket] = ticket
            if name_symbol:
                kwargs[name_symbol] = symbol
            # Only proceed if we can pass at least TP; include BOTH to avoid clearing
            if name_tp:
                kwargs[name_tp] = float(final_tp)
            if name_sl:
                kwargs[name_sl] = float(final_sl)

            if kwargs and (name_tp or name_sl):
                ok = bool(fn(**kwargs))
                return {"ok": ok, "via": fn.__name__, "kwargs": kwargs}
        except Exception:
            # fall through to mt5 fallback
            pass

    # 2) Fallback: order_send(TRADE_ACTION_SLTP) — ALWAYS include both SL and TP
    req = {
        "action": getattr(mt5, "TRADE_ACTION_SLTP", 6),
        "position": int(ticket),
        "symbol": symbol,
        "sl": float(final_sl),
        "tp": float(final_tp),
    }

    try:
        res = mt5.order_send(req)
        ret = int(getattr(res, "retcode", 0))
        return {"ok": ret == getattr(mt5, "TRADE_RETCODE_DONE", 10009),
                "via": "mt5.order_send(SLTP)", "retcode": ret, "retname": _retcode_name(mt5, ret),
                "comment": getattr(res, "comment", ""), "request": req}
    except Exception as e:
        return {"ok": False, "via": "mt5.order_send(SLTP)", "error": str(e), "request": req}

def execute_actions(mt5, router, actions: List[Action], apply: bool = False) -> List[ExecResult]:
    """Execute (or dry-run) planned actions."""
    results: List[ExecResult] = []

    for act in actions:
        if isinstance(act, Alert):
            results.append(ExecResult(True, act, {"note": act.message}))
            continue

        if not apply:
            results.append(ExecResult(True, act, {"dry_run": True}))
            continue

        try:
            if isinstance(act, DeleteOrder):
                d = _delete_order(mt5, router, act.ticket)
                results.append(ExecResult(bool(d.get("ok")), act, d))

            elif isinstance(act, ModifySLTP):
                d = _modify_sltp(mt5, router, act.ticket, act.sl, act.tp)
                results.append(ExecResult(bool(d.get("ok")), act, d))

            elif isinstance(act, ClosePosition):
                results.append(ExecResult(False, act, {"error": "ClosePosition not wired"}))

            else:
                results.append(ExecResult(False, act, {"error": "Unknown action type"}))

        except Exception as e:
            results.append(ExecResult(False, act, {"exception": str(e)}))

    return results
