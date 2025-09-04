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

def _round_to_digits(mt5, symbol: str, price: float) -> float:
    try:
        info = mt5.symbol_info(symbol)
        digits = int(getattr(info, "digits", 0) or 0)
        return round(float(price), digits)
    except Exception:
        return float(price)

def _leg_to_layer(leg: Optional[int]) -> Optional[int]:
    """Map leg -> layer where each layer has 4 legs: 1-4->1, 5-8->2, 9-12->3, ..."""
    if isinstance(leg, int) and leg >= 1:
        return ((leg - 1) // 4) + 1
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
      1) Scan ALL open positions and pending orders for TP2-like legs (2,6,10,14,...)
      2) Find the MAXIMUM TP value among all TP2 candidates with TP > 0
      3) If current price has exceeded that maximum TP (Bid >= TP for BUY; Ask <= TP for SELL):
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

    def _max_tp2_candidate(self, positions: List[Dict[str, Any]], orders: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find the TP2 candidate with the MAXIMUM TP value (not the first one).
        This prevents triggering on positions that have already been modified to BE+1pip.
        """
        # Collect all TP2-like candidates from positions and orders
        all_tp2_candidates = []
        
        # Add TP2 positions with TP > 0
        for p in positions:
            if _is_tp2_like_leg(p.get("leg")):
                tp = p.get("tp")
                if tp and float(tp) > 0:
                    all_tp2_candidates.append(p)
        
        # Add TP2 orders with TP > 0
        for o in orders:
            if _is_tp2_like_leg(o.get("leg")):
                tp = o.get("tp")
                if tp and float(tp) > 0:
                    all_tp2_candidates.append(o)
        
        if not all_tp2_candidates:
            return None
        
        # Find the candidate with the maximum TP value
        # For BUY orders, we want the highest TP
        # For SELL orders, we want the lowest TP (but still use max since we're looking for furthest from current price)
        max_candidate = None
        max_tp_distance = 0
        
        for candidate in all_tp2_candidates:
            tp = float(candidate.get("tp", 0))
            side = candidate.get("side")
            
            # For comparison, we need to consider the side
            # BUY: higher TP is further from current price
            # SELL: lower TP is further from current price
            if side == "BUY":
                tp_distance = tp  # Higher is further for BUY
            elif side == "SELL":
                tp_distance = -tp  # Lower (more negative) is further for SELL
            else:
                continue
                
            if max_candidate is None or tp_distance > max_tp_distance:
                max_candidate = candidate
                max_tp_distance = tp_distance
        
        return max_candidate

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
            # CHANGED: Use _max_tp2_candidate instead of _first_tp2_candidate
            candidate = self._max_tp2_candidate(grp["positions"], grp["orders"])
            if not candidate:
                if debug:
                    actions.append(Alert(f"[{self.name}] msg={mid} no TP2-like candidate with TP>0"))
                continue

            sym = candidate.get("symbol"); side = candidate.get("side"); leg = candidate.get("leg")
            tp = float(candidate.get("tp") or 0.0)
            exceeded, bid, ask = self._price_exceeded(mt5, candidate)

            if debug:
                # Enhanced debug to show it's using the maximum TP
                actions.append(Alert(f"[{self.name}] msg={mid} MAX TP2 leg={leg} side={side} tp={tp} bid={bid} ask={ask} exceeded={exceeded}"))

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


# ---- Manage Price Layers (NEW) ----

class ManagePriceLayersMonitor(BaseMonitor):
    """
    When a higher price layer (groups of 4 legs) becomes active for a given message_id & side,
    tighten TP on all *lower* layers' open positions to that position's own entry +/- 1 pip:
      BUY  -> TP = price_open + pip
      SELL -> TP = price_open - pip

    Env:
      PIP_MULT_<SYMBOL>=float       # pip override (e.g. PIP_MULT_XAUUSD+=10)
      MON_DEBUG_LAYERS=1            # debug alerts
      MON_LAYERS_TP_EPS_PIPS=0.05   # tolerance in pips before updating TP
      MON_LAYERS_MIN_POS_IN_LAYER=1 # how many open positions constitute an "active" layer
      MON_LAYERS_APPLY_TO_N_LOWER   # "all" (default) or integer N to only adjust the last N lower layers
    """
    name = "manage_price_layers"

    @staticmethod
    def _get_env_int(env: Dict[str, str], key: str, default: int) -> int:
        try:
            return int(str(env.get(key, default)))
        except Exception:
            return default

    @staticmethod
    def _get_env_float(env: Dict[str, str], key: str, default: float) -> float:
        try:
            return float(str(env.get(key, default)))
        except Exception:
            return default

    def evaluate(self, positions, orders, ctx):
        mt5 = ctx.get("mt5")
        env = ctx.get("env", {})
        debug = str(env.get("MON_DEBUG_LAYERS", "0")).lower() in ("1","true","yes","on")
        min_pos_layer = self._get_env_int(env, "MON_LAYERS_MIN_POS_IN_LAYER", 1)
        apply_to_n_lower_raw = str(env.get("MON_LAYERS_APPLY_TO_N_LOWER", "all")).strip().lower()
        eps_pips = self._get_env_float(env, "MON_LAYERS_TP_EPS_PIPS", 0.05)

        # Group positions by message_id
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for p in positions:
            mid = p.get("message_id")
            if mid:
                groups.setdefault(mid, []).append(p)

        actions: List[Action] = []

        for mid, pos_list in groups.items():
            # Split by side to avoid mixing BUY/SELL logic
            sides = ("BUY", "SELL")
            for side in sides:
                side_pos = [p for p in pos_list if p.get("side") == side and _is_int_leg(p.get("leg"))]
                if not side_pos:
                    continue

                # Build layer -> list of positions mapping
                layers: Dict[int, List[Dict[str, Any]]] = {}
                for p in side_pos:
                    layer = _leg_to_layer(p.get("leg"))
                    if layer is None:
                        continue
                    layers.setdefault(layer, []).append(p)

                if not layers:
                    continue

                # Determine active layers (meeting min_pos_layer)
                active_layers = sorted([L for L, plist in layers.items() if len(plist) >= min_pos_layer])
                if not active_layers:
                    continue
                highest_layer = max(active_layers)

                # Decide which lower layers to affect
                if apply_to_n_lower_raw == "all":
                    target_layers = [L for L in active_layers if L < highest_layer]
                else:
                    try:
                        n = int(apply_to_n_lower_raw)
                        min_layer = max(1, highest_layer - n)
                        target_layers = [L for L in active_layers if min_layer <= L < highest_layer]
                    except Exception:
                        target_layers = [L for L in active_layers if L < highest_layer]

                if not target_layers:
                    if debug:
                        actions.append(Alert(f"[{self.name}] msg={mid} side={side} highest_layer={highest_layer} no lower layers to adjust"))
                    continue

                # For each position in target lower layers: set TP to entry +/- 1 pip (per position), if meaningfully different
                for layer in sorted(target_layers):
                    for p in layers.get(layer, []):
                        sym = p.get("symbol")
                        pip = _one_pip(mt5, sym, env) if sym else None
                        if not pip or pip <= 0:
                            if debug:
                                actions.append(Alert(f"[{self.name}] msg={mid} ticket={p.get('ticket')} no pip size for {sym}"))
                            continue
                        entry = float(p.get("price_open") or 0.0)
                        target_tp = entry + pip if side == "BUY" else entry - pip
                        target_tp = _round_to_digits(mt5, sym, target_tp)

                        cur_tp = p.get("tp")
                        # Only update if current TP is None or differs by more than eps*pip
                        needs_update = (cur_tp is None) or (abs(float(cur_tp) - target_tp) > (eps_pips * pip))
                        if needs_update:
                            actions.append(ModifySLTP(ticket=int(p["ticket"]), tp=float(target_tp),
                                                      reason=f"{self.name}: tighten TP to BE±1pip msg={mid} side={side} layer={layer}->{highest_layer}"))
                        elif debug:
                            actions.append(Alert(f"[{self.name}] msg={mid} ticket={p.get('ticket')} TP unchanged (cur={cur_tp} target={target_tp})"))

                if debug:
                    actions.append(Alert(f"[{self.name}] msg={mid} side={side} highest_layer={highest_layer} adjusted_layers={target_layers}"))

        return actions


# ---- Registry ----
MONITORS: List[BaseMonitor] = [
    ManageTP2HitMonitor(),
    ManagePriceLayersMonitor(),  # NEW
]
