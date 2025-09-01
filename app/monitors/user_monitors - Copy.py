# app/monitors/user_monitors.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
import re

from .actions import Action, DeleteOrder, ModifySLTP, Alert

# ---- Utilities to filter/compute ----

def _is_int_leg(leg: Any) -> bool:
    return isinstance(leg, int) and leg >= 0

def _is_tp2_like_leg(leg: Any) -> bool:
    """Legs 2,6,10,14,... i.e., leg ≡ 2 (mod 4) and leg >= 2."""
    return _is_int_leg(leg) and leg >= 2 and ((leg - 2) % 4 == 0)

def _pip_env_key(symbol: str) -> str:
    # Uppercase and replace all non-alnum with underscores (handles XAUUSD+ etc.)
    return f"PIP_MULT_{re.sub(r'[^A-Z0-9]+', '_', symbol.upper())}"

def _one_pip(mt5, symbol: str, env: Dict[str, str]) -> Optional[float]:
    """
    Compute 1 pip in price units.
    Heuristic:
      - If env override PIP_MULT_<SYMBOL> exists -> pip = point * override
      - Else: if digits in {1,4} -> pip = point
              else               -> pip = point * 10   (covers 2,3,5-digits like XAUUSD+, JPY, 5-digit FX)
    """
    try:
        if not symbol:
            return None
        info = mt5.symbol_info(symbol)
        if not info:
            return None
        point = float(getattr(info, "point", 0) or 0.0)
        digits = int(getattr(info, "digits", 0) or 0)
        key = _pip_env_key(symbol)
        if key in env:
            try:
                mult = float(env[key])
            except Exception:
                mult = None
            if mult and mult > 0:
                return point * mult
        mult = 1.0 if digits in (1, 4) else 10.0
        pip = point * mult
        return pip if pip > 0 else None
    except Exception:
        return None

# ---- Base monitor interface ----

class BaseMonitor:
    name: str = "base"
    def evaluate(self, positions: List[Dict[str, Any]], orders: List[Dict[str, Any]], ctx: Dict[str, Any]) -> List[Action]:
        """Return a list of Action instances. Never raise."""
        return []

# ---- Manage TP2 Hit ----

class ManageTP2HitMonitor(BaseMonitor):
    """
    For each message group:
      1) Scan open positions (then pending orders) for the first TP > 0 on a TP2-like leg: 2,6,10,14,...
      2) If current price has exceeded that TP (Bid >= TP for BUY; Ask <= TP for SELL):
         a) Delete all pending orders in the same message group (default).
            To keep certain legs (e.g., 1 and 2), set env MON_TP2_KEEP_LEGS="1,2".
         b) For every open position in the group, set SL to entry +/- 1 pip (BUY: entry+pip, SELL: entry-pip),
            but only if this improves protection (no downgrade).
    Env:
      PIP_MULT_<SYMBOL>=float  (e.g., PIP_MULT_XAUUSD+=10)
      MON_DEBUG_TP2=1          (debug alerts)
      MON_TP2_KEEP_LEGS="1,2"  (optional; default empty = keep none)
    """
    name = "manage_tp2_hit"

    def _first_tp2_candidate(self, positions: List[Dict[str, Any]], orders: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        pos_tp2 = [p for p in positions if _is_tp2_like_leg(p.get("leg"))]
        pos_tp2.sort(key=lambda r: r.get("leg", 10**9))
        for p in pos_tp2:
            tp = p.get("tp")
            if tp and float(tp) > 0:
                return p
        ord_tp2 = [o for o in orders if _is_tp2_like_leg(o.get("leg"))]
        ord_tp2.sort(key=lambda r: r.get("leg", 10**9))
        for o in ord_tp2:
            tp = o.get("tp")
            if tp and float(tp) > 0:
                return o
        return None

    def _price_exceeded(self, mt5, row: Dict[str, Any]) -> Tuple[bool, float, float]:
        sym = row.get("symbol")
        side = row.get("side")  # "BUY" or "SELL"
        tp = float(row.get("tp") or 0.0)
        if not sym or not side or tp <= 0:
            return False, 0.0, 0.0
        try:
            tick = mt5.symbol_info_tick(sym)
            bid = float(getattr(tick, "bid", 0.0) or getattr(tick, "last", 0.0) or 0.0)
            ask = float(getattr(tick, "ask", 0.0) or getattr(tick, "last", 0.0) or 0.0)
        except Exception:
            bid = ask = 0.0
        if side == "BUY":
            return (bid >= tp and bid > 0), bid, ask
        elif side == "SELL":
            return (ask <= tp and ask > 0), bid, ask
        return False, bid, ask

    def _improved_sl(self, side: str, current_sl: Optional[float], target_sl: float) -> bool:
        if target_sl is None or not isinstance(target_sl, (int, float)):
            return False
        if current_sl in (None, 0):
            return True
        if side == "BUY":
            return float(current_sl) < float(target_sl)
        if side == "SELL":
            return float(current_sl) > float(target_sl)
        return False

    @staticmethod
    def _parse_keep_legs(env: Dict[str, str]) -> set:
        raw = str(env.get("MON_TP2_KEEP_LEGS", "") or "").strip()
        keep = set()
        if raw:
            for tok in re.split(r"[,\s;]+", raw):
                tok = tok.strip()
                if tok.isdigit():
                    keep.add(int(tok))
        return keep

    def evaluate(self, positions, orders, ctx):
        mt5 = ctx.get("mt5")
        env = ctx.get("env", {})
        debug = str(env.get("MON_DEBUG_TP2", "0")).lower() in ("1","true","yes","on")
        keep_legs = self._parse_keep_legs(env)  # empty by default -> delete all

        # Group rows by message_id
        groups: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for p in positions:
            mid = p.get("message_id")
            if mid:
                groups.setdefault(mid, {"positions": [], "orders": []})["positions"].append(p)
        for o in orders:
            mid = o.get("message_id")
            if mid:
                groups.setdefault(mid, {"positions": [], "orders": []})["orders"].append(o)

        actions: List[Action] = []

        for mid, grp in groups.items():
            candidate = self._first_tp2_candidate(grp["positions"], grp["orders"])
            if not candidate:
                if debug:
                    actions.append(Alert(f"[{self.name}] msg={mid} no TP2-like candidate with TP>0"))
                continue

            sym = candidate.get("symbol"); side = candidate.get("side"); leg = candidate.get("leg")
            tp = float(candidate.get("tp") or 0.0)
            exceeded, bid, ask = self._price_exceeded(mt5, candidate)

            if debug:
                actions.append(Alert(f"[{self.name}] msg={mid} leg={leg} side={side} tp={tp} bid={bid} ask={ask} exceeded={exceeded}"))

            if not exceeded:
                continue

            # (a) Delete all pending orders in the group, except any explicitly kept via MON_TP2_KEEP_LEGS.
            for o in grp["orders"]:
                l = o.get("leg")
                if isinstance(l, int) and l in keep_legs:
                    continue
                actions.append(DeleteOrder(ticket=int(o["ticket"]), reason=f"{self.name}: TP2 exceeded msg={mid}, delete leg={l}"))

            # (b) For every open position, set SL to entry +/- 1 pip (BUY +, SELL -), only if improves.
            for p in grp["positions"]:
                s = p.get("symbol")
                pip = _one_pip(mt5, s, env) if s else None
                if not pip or pip <= 0:
                    if debug:
                        actions.append(Alert(f"[{self.name}] msg={mid} ticket={p.get('ticket')} no pip size for {s}"))
                    continue
                entry = float(p.get("price_open") or 0.0)
                sside = p.get("side")
                target_sl = entry + pip if sside == "BUY" else entry - pip if sside == "SELL" else None
                cur_sl = p.get("sl")
                if target_sl is None:
                    continue
                if self._improved_sl(sside, cur_sl, target_sl):
                    actions.append(ModifySLTP(ticket=int(p["ticket"]), sl=float(target_sl), reason=f"{self.name}: lock +1 pip msg={mid}"))
                elif debug:
                    actions.append(Alert(f"[{self.name}] msg={mid} ticket={p.get('ticket')} SL unchanged (cur={cur_sl} target={target_sl})"))

            if any(isinstance(a, (DeleteOrder, ModifySLTP)) for a in actions):
                kept_str = f" (kept legs: {sorted(keep_legs)})" if keep_legs else ""
                actions.append(Alert(f"[{self.name}] msg={mid} -> pending deleted{kept_str}, SL set to BE+/-1pip on open legs"))

        return actions


# ---- Registry ----
MONITORS: List[BaseMonitor] = [
    ManageTP2HitMonitor(),
]
