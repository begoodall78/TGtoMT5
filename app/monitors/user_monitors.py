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

class TrailStopByTPLevelsMonitor(BaseMonitor):
    """
    Trail stop-loss based on TP levels from the database.
    For each message group with open positions:
      1. Query the database for TP1, TP2, TP3 from legs_index table
      2. Check current market price
      3. Adjust SL based on price position relative to TP levels:
         - Price > TP1: SL = BE + 1 pip <==== CURRENTLY TURNED OFF
         - Price > TP2: SL = TP1
         - Price > TP3: SL = TP2
    
    Env:
      MON_DEBUG_TRAIL=1         # debug alerts
      MON_TRAIL_ENABLED=1       # enable/disable monitor
    """
    name = "trail_stop_by_tp"

    def _get_tp_levels_from_db(self, message_id: str) -> Dict[int, float]:
        """
        Query the database for TP levels of the first 3 legs of a message.
        Returns dict: {1: tp1_value, 2: tp2_value, 3: tp3_value}
        """
        from app.common.database import get_db_manager
        
        try:
            db_manager = get_db_manager()
            
            # Build the group_key from message_id (format: OPEN_{message_id})
            group_key = f"OPEN_{message_id}"
            
            # Query for legs 1, 2, 3 - leg_tag format is like "XAUUSD#1", "XAUUSD#2", etc.
            rows = db_manager.fetchall("""
                SELECT leg_tag, tp 
                FROM legs_index 
                WHERE group_key = ? 
                AND (leg_tag LIKE '%#1' OR leg_tag LIKE '%#2' OR leg_tag LIKE '%#3')
                ORDER BY leg_tag
            """, (group_key,))
            
            tp_levels = {}
            for leg_tag, tp in rows:
                if tp and float(tp) > 0:
                    # Extract leg number from leg_tag (e.g., "XAUUSD#1" -> 1)
                    import re
                    match = re.search(r'#(\d+)$', leg_tag)
                    if match:
                        leg_num = int(match.group(1))
                        if leg_num in [1, 2, 3]:
                            tp_levels[leg_num] = float(tp)
            
            return tp_levels
            
        except Exception as e:
            if hasattr(self, 'debug') and self.debug:
                print(f"[{self.name}] Error fetching TP levels for msg={message_id}: {e}")
            return {}

    def _get_current_price(self, mt5, symbol: str, side: str) -> float:
        """Get current market price (Bid for BUY, Ask for SELL)."""
        try:
            tick = mt5.symbol_info_tick(symbol)
            if side == "BUY":
                return float(getattr(tick, "bid", 0.0) or 0.0)
            elif side == "SELL":
                return float(getattr(tick, "ask", 0.0) or 0.0)
        except Exception:
            pass
        return 0.0

    def _determine_new_sl(self, position: Dict[str, Any], tp_levels: Dict[int, float], 
                         current_price: float, pip: float, mt5) -> Optional[float]:
        """
        Determine the new SL based on current price position relative to TP levels.
        Returns None if no change needed.
        """
        side = position.get("side")
        entry = float(position.get("price_open") or 0.0)
        current_sl = position.get("sl")
        symbol = position.get("symbol")
        
        if not side or not entry or not tp_levels:
            return None
        
        # Get TP values (ensure they exist)
        tp1 = tp_levels.get(1, 0)
        tp2 = tp_levels.get(2, 0)
        tp3 = tp_levels.get(3, 0)
        
        # Calculate breakeven + 1 pip
        be_plus_1 = entry + pip if side == "BUY" else entry - pip
        
        # Determine target SL based on price position
        target_sl = None
        
        if side == "BUY":
            # For BUY positions: price moves up through TPs
            if tp3 > 0 and current_price >= tp3:
                target_sl = tp2  # Price > TP3: SL = TP2
            elif tp2 > 0 and current_price >= tp2:
                target_sl = tp1  # Price > TP2: SL = TP1
            # elif tp1 > 0 and current_price >= tp1:
            #    target_sl = be_plus_1  # Price > TP1: SL = BE + 1 pip
                
        elif side == "SELL":
            # For SELL positions: price moves down through TPs
            if tp3 > 0 and current_price <= tp3:
                target_sl = tp2  # Price < TP3: SL = TP2
            elif tp2 > 0 and current_price <= tp2:
                target_sl = tp1  # Price < TP2: SL = TP1
            #elif tp1 > 0 and current_price <= tp1:
            #    target_sl = be_plus_1  # Price < TP1: SL = BE - 1 pip
        
        if target_sl is None:
            return None
            
        # Round to symbol's digits
        target_sl = _round_to_digits(mt5, symbol, target_sl)
        
        # Only update if it improves protection (don't move SL against position)
        if self._improved_sl(side, current_sl, target_sl):
            return target_sl
            
        return None

    def _improved_sl(self, side: str, current_sl: Optional[float], target_sl: float) -> bool:
        """Check if target SL is an improvement over current SL."""
        if target_sl is None:
            return False
        if current_sl in (None, 0):
            return True
        if side == "BUY":
            return float(target_sl) > float(current_sl)
        elif side == "SELL":
            return float(target_sl) < float(current_sl)
        return False

    def evaluate(self, positions, orders, ctx):
        mt5 = ctx.get("mt5")
        env = ctx.get("env", {})
        
        # Check if monitor is enabled
        enabled = str(env.get("MON_TRAIL_ENABLED", "1")).lower() in ("1", "true", "yes", "on")
        if not enabled:
            return []
            
        self.debug = str(env.get("MON_DEBUG_TRAIL", "0")).lower() in ("1", "true", "yes", "on")
        
        # Group positions by message_id
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for p in positions:
            mid = p.get("message_id")
            if mid:
                groups.setdefault(mid, []).append(p)
        
        actions: List[Action] = []
        
        for mid, pos_list in groups.items():
            # Get TP levels from database
            tp_levels = self._get_tp_levels_from_db(mid)
            
            if not tp_levels:
                if self.debug:
                    actions.append(Alert(f"[{self.name}] msg={mid} no TP levels in database"))
                continue
                
            if self.debug:
                tp_str = ", ".join([f"TP{k}={v:.5f}" for k, v in sorted(tp_levels.items())])
                actions.append(Alert(f"[{self.name}] msg={mid} found TPs: {tp_str}"))
            
            # Process each position in the group
            for p in pos_list:
                symbol = p.get("symbol")
                side = p.get("side")
                
                if not symbol or not side:
                    continue
                
                # Get pip value
                pip = _one_pip(mt5, symbol, env)
                if not pip or pip <= 0:
                    if self.debug:
                        actions.append(Alert(f"[{self.name}] msg={mid} ticket={p.get('ticket')} no pip size for {symbol}"))
                    continue
                
                # Get current market price
                current_price = self._get_current_price(mt5, symbol, side)
                if current_price <= 0:
                    continue
                
                # Determine if SL needs adjustment
                new_sl = self._determine_new_sl(p, tp_levels, current_price, pip, mt5)
                
                if new_sl is not None:
                    ticket = int(p["ticket"])
                    current_sl = p.get("sl")
                    
                    # Format current_sl safely - handle None values
                    current_sl_str = f"{current_sl:.5f}" if current_sl is not None else "None"
                    
                    # Determine which TP level we're trailing to
                    level_desc = ""
                    entry = float(p.get("price_open") or 0.0)
                    be_plus_1 = entry + pip if side == "BUY" else entry - pip
                    
                    if abs(new_sl - be_plus_1) < pip * 0.1:
                        level_desc = "BE+1pip"
                    elif abs(new_sl - tp_levels.get(1, 0)) < pip * 0.1:
                        level_desc = "TP1"
                    elif abs(new_sl - tp_levels.get(2, 0)) < pip * 0.1:
                        level_desc = "TP2"
                    
                    actions.append(ModifySLTP(
                        ticket=ticket, 
                        sl=float(new_sl),
                        reason=f"{self.name}: trail to {level_desc} msg={mid} (was {current_sl_str})"
                    ))
                    
                    if self.debug:
                        actions.append(Alert(
                            f"[{self.name}] msg={mid} ticket={ticket} {side} "
                            f"price={current_price:.5f} SL: {current_sl_str} -> {new_sl:.5f} ({level_desc})"
                        ))
                elif self.debug:
                    current_sl_str = f"{p.get('sl')}" if p.get('sl') is not None else "None"
                    actions.append(Alert(
                        f"[{self.name}] msg={mid} ticket={p.get('ticket')} SL unchanged (current={current_sl_str})"
                    ))
        
        return actions
    
# ---- Registry ----
MONITORS: List[BaseMonitor] = [
    ManageTP2HitMonitor(),
    ManagePriceLayersMonitor(),
    TrailStopByTPLevelsMonitor(),  # NEW - Add this line
]
