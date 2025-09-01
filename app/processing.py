from __future__ import annotations
import asyncio
import threading
import inspect
import hashlib
import logging
import os
import csv
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from app.engine.semantic import load_semantic_dictionary, evaluate as sem_evaluate
from app.refindex import record_open, generate_mock_ack_for_open, resolve_group_key, list_open_legs, update_leg_targets
from .models import Action, Leg, Side
try:
    from .unparsed_reporter import UnparsedReporter
except Exception:
    UnparsedReporter = None
try:
    from app.engine.flags import get_flag
except Exception:

    def get_flag(name: str, default: bool=False) -> bool:
        v = os.getenv(name, '')
        return v.strip().lower() in ('1', 'true', 'on', 'yes') if v else default
log = logging.getLogger('processing')

# --- Refactor: central config with runtime apply/reload (keeps single-file) ---
from dataclasses import dataclass

@dataclass
class Config:
    semantic_dict_path: str = os.getenv('SEMANTIC_DICT_PATH', 'runtime/data/parser_semantic.yaml')
    failsafe_on_unparsed: bool = os.getenv('ENGINE_FAILSAFE_ON_UNPARSED', 'true').lower() in ('1','true','yes')
    max_legs: int = int(os.getenv('MAX_LEGS', '10'))
    default_leg_volume: float = float(os.getenv('DEFAULT_LEG_VOLUME', '0.01'))
    require_symbol: bool = os.getenv('SIGNAL_REQUIRE_SYMBOL', 'false').lower() in ('1','true','yes')
    require_price: bool = os.getenv('SIGNAL_REQUIRE_PRICE', 'true').lower() in ('1','true','yes')
    enable_ignore_gate: bool = get_flag('ENGINE_ENABLE_IGNORE_GATE', False)
    default_symbol: str = os.getenv('DEFAULT_SYMBOL', 'XAUUSD').upper()
    signal_min_text_len: int = int(os.getenv('SIGNAL_MIN_TEXT_LEN', '8'))


def _log_emit(event: str, *, action=None, gk=None, source_msg_id=None, reason: str | None = None) -> None:
    """Standardized structured log for emissions and unparsed cases."""
    try:
        extra = {
            'event': event,
            'gk': gk,
            'source_msg_id': str(source_msg_id) if source_msg_id is not None else None,
        }
        if action is not None:
            extra['action_id'] = getattr(action, 'action_id', None)
            extra['type'] = getattr(action, 'type', None)
            try:
                extra['legs_count'] = len(getattr(action, 'legs', []) or [])
            except Exception:
                extra['legs_count'] = None
        if reason:
            extra['reason'] = reason
        logging.getLogger('processing').info(event, extra=extra)
    except Exception:
        pass
_CFG_INITIALIZED = False

def load_config_from_env() -> Config:
    return Config()

def apply_config(cfg: Config) -> None:
    """Apply config values to module-level constants for backward compatibility."""
    global SEMANTIC_DICT_PATH, ENGINE_FAILSAFE_ON_UNPARSED, MAX_LEGS, DEFAULT_LEG_VOLUME
    global SIGNAL_REQUIRE_SYMBOL, SIGNAL_REQUIRE_PRICE, ENGINE_ENABLE_IGNORE_GATE
    global DEFAULT_SYMBOL, SIGNAL_MIN_TEXT_LEN
    SEMANTIC_DICT_PATH = cfg.semantic_dict_path
    ENGINE_FAILSAFE_ON_UNPARSED = cfg.failsafe_on_unparsed
    MAX_LEGS = cfg.max_legs
    DEFAULT_LEG_VOLUME = cfg.default_leg_volume
    SIGNAL_REQUIRE_SYMBOL = cfg.require_symbol
    SIGNAL_REQUIRE_PRICE = cfg.require_price
    ENGINE_ENABLE_IGNORE_GATE = cfg.enable_ignore_gate
    DEFAULT_SYMBOL = cfg.default_symbol
    SIGNAL_MIN_TEXT_LEN = cfg.signal_min_text_len

def init_processing(cfg: Config | None = None) -> None:
    """Initialize processing module once: config + semantic dict banner."""
    global _CFG_INITIALIZED, _SEM_DICT
    if _CFG_INITIALIZED:
        return
    cfg = cfg or load_config_from_env()
    apply_config(cfg)
    try:
        # reload semantic dict based on possibly new path
        _SEM_DICT = load_semantic_dictionary(SEMANTIC_DICT_PATH)
    except Exception as _e:
        logging.getLogger('processing').warning('SEMANTIC_DICT_LOAD_WARN %s', _e, extra={'event':'SEMANTIC_DICT_LOAD_WARN'})
    _log_rules_state_once()
    _CFG_INITIALIZED = True
# --- end config block ---
SEMANTIC_DICT_PATH = os.getenv('SEMANTIC_DICT_PATH', 'runtime/data/parser_semantic.yaml')
ENGINE_FAILSAFE_ON_UNPARSED = os.getenv('ENGINE_FAILSAFE_ON_UNPARSED', 'true').lower() in ('1', 'true', 'yes')
MAX_LEGS = int(os.getenv('MAX_LEGS', '10'))
DEFAULT_LEG_VOLUME = float(os.getenv('DEFAULT_LEG_VOLUME', '0.01'))
SIGNAL_REQUIRE_SYMBOL = os.getenv('SIGNAL_REQUIRE_SYMBOL', 'false').lower() in ('1', 'true', 'yes')
SIGNAL_REQUIRE_PRICE = os.getenv('SIGNAL_REQUIRE_PRICE', 'true').lower() in ('1', 'true', 'yes')
ENGINE_ENABLE_IGNORE_GATE = get_flag('ENGINE_ENABLE_IGNORE_GATE', False)

def _truncate_for_log(s, max_chars=600, max_lines=10):
    if s is None:
        return ''
    s = str(s)
    lines = s.splitlines()
    if len(lines) > max_lines:
        cut = '\n'.join(lines[:max_lines])
        rem = len(s) - len(cut)
        return cut + '\n... [truncated ' + str(rem) + ' chars]'
    if len(s) > max_chars:
        return s[:max_chars] + '... [truncated ' + str(len(s) - max_chars) + ' chars]'
    return s

def _maybe_ack_ignore(source_msg_id, text, unparsed_raw_msg=None):
    if not ENGINE_ENABLE_IGNORE_GATE:
        return False
    try:
        ir = (_SEM_DICT.data or {}).get('ignore_rules') or {}
        contains_list = [c for c in ir.get('contains') or [] if isinstance(c, str) and c.strip()]
        tlow = (text or '').lower()
        matched_phrase = None
        for phrase in contains_list:
            if phrase.lower() in tlow:
                matched_phrase = phrase
                break
        if matched_phrase:
            try:
                msg = _truncate_for_log(text or '')
                log.info('IGNORED | reason=ignore rule | rule=ign_contains | msg=%s\nText:\n"%s"', source_msg_id, msg)
            except Exception:
                pass
            try:
                if unparsed_raw_msg is not None:
                    setattr(unparsed_raw_msg, '_ignored_by_gate', True)
            except Exception:
                pass
            return True
    except Exception:
        return False
    return False
_SEM_DICT = load_semantic_dictionary(SEMANTIC_DICT_PATH)
DEFAULT_SYMBOL = os.getenv('DEFAULT_SYMBOL', 'XAUUSD').upper()
SIGNAL_MIN_TEXT_LEN = int(os.getenv('SIGNAL_MIN_TEXT_LEN', '8'))
CHAT_ID_WHITELIST: Set[int] = set((int(x) for x in os.getenv('SIGNAL_CHAT_ID_WHITELIST', '').split(',') if x.strip().lstrip('+-').isdigit()))
SENDER_WHITELIST: Set[str] = set((x.strip().lower() for x in os.getenv('SIGNAL_SENDER_WHITELIST', '').split(',') if x.strip()))
_SIDE_RE = re.compile('\\b(BUY|SELL)\\b', re.I)
ENTRY_RE = re.compile('^\\s*(?:BUY|SELL)\\s*@\\s*(\\d+(?:\\.\\d+)?)(?:\\s*/\\s*(\\d+(?:\\.\\d+)?))?', re.I | re.M)
TP_LINE_RE = re.compile('\\b(TP)\\s+(OPEN|\\d+(?:\\.\\d+)?)\\b', re.I)
SL_RE = re.compile('\\bSL\\s+(\\d+(?:\\.\\d+)?)\\b', re.I)
_SYMBOL_RE = re.compile('^\\s*([A-Z]{3,10}\\d{0,2})\\s*$', re.M)
SLIP_RE = re.compile('\\b(?:slip|slippage|max\\s*slip)\\s*[:=]?\\s*(\\d+(?:\\.\\d+)?)\\s*(?:pip|pips|pt|points)?', re.I)
WORSE_RE = re.compile('\\b(?:worse(?:\\s*pips?)?)\\s*[:=]?\\s*(\\d+(?:\\.\\d+)?)', re.I)
_rules_state_logged = False

def _log_rules_state_once() -> None:
    """One-time startup banner for semantic engine."""
    global _rules_state_logged
    if _rules_state_logged:
        return
    try:
        log.info('semantic_dict_version=%s', getattr(_SEM_DICT, 'version', 'NA'))
    except Exception:
        pass
    _rules_state_logged = True

def _client_id_for_message(symbol: str, source_msg_id: str) -> str:
    base = f'{symbol}_{source_msg_id}'.strip()
    clean = re.sub('[^A-Za-z0-9_]+', '_', base)
    return clean

class ParseSignal:

    def __init__(self, *, side: Optional[Side]=None, symbol: Optional[str]=None, entries: Optional[list[float]]=None, tps: Optional[list[Optional[float]]]=None, sl: Optional[float]=None, max_slip_pips: Optional[float]=None, raw: str='') -> None:
        self.side = side
        self.symbol = symbol
        self.entries = entries or []
        self.tps = tps or []
        self.sl = sl
        self.max_slip_pips = max_slip_pips
        self.raw = raw


def _preclean_text(s: str) -> str:
    """Strip zero-width chars and emojis, but **preserve** newlines and whitespace."""
    if not s:
        return ''
    # Remove zero-width chars
    s = re.sub(r'[\u200B-\u200D\uFEFF]', '', s)
    # Remove emoji range only; keep whitespace (including \n, \r, \t)
    def _keep(ch: str) -> bool:
        code = ord(ch)
        if 0x1F300 <= code <= 0x1FAFF:
            return False  # emoji
        return True  # keep everything else
    return ''.join(ch for ch in s if _keep(ch))

def parse_signal_text(text: str) -> ParseSignal:
    raw = text or ''
    s = _preclean_text(raw).strip()
    m = _SIDE_RE.search(s)
    side: Optional[Side] = m.group(1).upper() if m else None
    symbol: Optional[str] = None
    cand_syms = [g for g in _SYMBOL_RE.findall(s)]
    if cand_syms:
        reserved = {'TP','SL','BUY','SELL','TP1','TP2','TP3','TP4','TP5'}
        cand_syms = [c for c in cand_syms if c.upper() not in reserved]
        preferred = [c for c in cand_syms if c.upper() in ('XAUUSD', 'XAGUSD', 'EURUSD', 'GBPUSD', 'USDJPY', 'US30', 'GER40')]
        if cand_syms:
            symbol = (preferred[0] if preferred else cand_syms[0]).upper()
    entries: list[float] = []
    m = ENTRY_RE.search(s)
    if m:
        e1 = float(m.group(1))
        e2 = float(m.group(2)) if m.group(2) else None
        entries = [e1] if e2 is None else [e1, e2]
    tps: list[Optional[float]] = []
    for m in TP_LINE_RE.finditer(s):
        v = m.group(2).upper()
        tps.append(None if v == 'OPEN' else float(v))
    sl = None
    m = SL_RE.search(s)
    if m:
        sl = float(m.group(1))
    max_slip = None
    for pat in (SLIP_RE, WORSE_RE):
        m = pat.search(s)
        if m:
            try:
                max_slip = float(m.group(1))
                break
            except Exception:
                pass
    return ParseSignal(side=side, symbol=symbol, entries=entries, tps=tps, sl=sl, max_slip_pips=max_slip, raw=raw)

def _has_price_info(ps: ParseSignal) -> bool:
    return bool(ps.entries) or ps.sl is not None or any((tp is not None for tp in ps.tps))

def _is_valid_signal(ps: ParseSignal, raw: str, *, chat_id: Optional[int]=None, sender_username: Optional[str]=None) -> bool:
    if len((raw or '').strip()) < SIGNAL_MIN_TEXT_LEN:
        return False
    if ps.side is None:
        return False
    if SIGNAL_REQUIRE_SYMBOL and (not ps.symbol):
        return False
    if SIGNAL_REQUIRE_PRICE and (not _has_price_info(ps)):
        return False
    if CHAT_ID_WHITELIST and chat_id is not None and (chat_id not in CHAT_ID_WHITELIST):
        return False
    if SENDER_WHITELIST and sender_username and (sender_username.lower() not in SENDER_WHITELIST):
        return False
    return True

def _pip_value(symbol: str) -> float:
    sym = (symbol or '').upper()
    if 'JPY' in sym:
        return 0.01
    return 0.1 if sym.startswith('XAU') else 0.0001

def _is_same_price(a: Optional[float], b: Optional[float], symbol: str) -> bool:
    if a is None or b is None:
        return False
    tol = _pip_value(symbol) * 1.0
    return abs(float(a) - float(b)) <= tol

def _tp_block_from_list(tp_values: list[Optional[float]], has_open: bool) -> list[Optional[float]]:
    nums = [t for t in tp_values if t is not None]
    blk: list[Optional[float]] = list(nums[:4])
    while len(blk) < 4:
        blk.append(blk[-1] if blk else None)
    if has_open:
        blk[3] = None
    return blk[:4]

def _tps_for_legs(ps: ParseSignal, legs_count: int) -> list[Optional[float]]:
    if legs_count <= 0:
        return []
    numeric = [t for t in ps.tps or [] if t is not None]
    has_open = any((t is None for t in ps.tps or []))

    def block4(vals: list[float]) -> list[Optional[float]]:
        if not vals:
            return [None, None, None, None if has_open else None]
        blk = list(vals[:4])
        while len(blk) < 4:
            blk.append(blk[-1])
        if has_open:
            blk[3] = None
        return blk
    if len(ps.entries or []) >= 2 and legs_count >= 8:
        blk = block4(numeric)
        out = (blk + blk)[:legs_count]
        while len(out) < legs_count:
            out.append(blk[-1])
        return out
    if len(ps.entries or []) == 1 and legs_count >= 4:
        blk = block4(numeric)
        out = blk[:legs_count]
        while len(out) < legs_count:
            out.append(blk[-1])
        return out

    def idx_for_pos(pos: int) -> int:
        if pos == 0:
            return 0
        return pos - 1
    out: list[Optional[float]] = [None] * legs_count
    fill_upto = legs_count
    if has_open and legs_count > 0:
        out[-1] = None
        fill_upto = legs_count - 1
    for pos in range(fill_upto):
        if not numeric:
            out[pos] = None
            continue
        idx = idx_for_pos(pos)
        if idx >= len(numeric):
            idx = len(numeric) - 1
        out[pos] = numeric[idx]
    return out

def _make_action_id(action_type: str, source_msg_id: str, legs: List[Leg]) -> str:
    blob = f'{action_type}|{source_msg_id}|{[(l.symbol, l.side, l.leg_id, l.tp, l.sl, l.entry) for l in legs]}'
    h = hashlib.sha1(blob.encode('utf-8'), usedforsecurity=False).hexdigest()[:10]
    dt = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    return f'{action_type}-{dt}-{h}'

def _make_leg_id(client_id: str, leg_no: int) -> str:
    return f'{client_id}#{leg_no}'

def _coalesce_modify_legs(legs: list[Leg], gk: str) -> list[Leg]:
    grouped: dict[str, list[Leg]] = {}

    def _key_for(l: Leg) -> str:
        if getattr(l, 'position_ticket', None):
            return f'pos:{l.position_ticket}'
        if getattr(l, 'order_ticket', None):
            return f'ord:{l.order_ticket}'
        return f"tag:{getattr(l, 'tag', getattr(l, 'leg_id', ''))}"
    for _l in legs:
        grouped.setdefault(_key_for(_l), []).append(_l)
    out: list[Leg] = []
    for k, arr in grouped.items():
        if k.startswith('pos:') or k.startswith('ord:'):
            arr_sorted = sorted(arr, key=lambda l: str(getattr(l, 'tag', getattr(l, 'leg_id', ''))))
            keep = arr_sorted[0]
            dropped = arr_sorted[1:]
            if dropped:
                logging.getLogger('processing').info('MGMT_COALESCE', extra={'event': 'MGMT_COALESCE', 'gk': gk, 'ticket_key': k, 'kept': getattr(keep, 'tag', keep.leg_id), 'dropped': [getattr(x, 'tag', x.leg_id) for x in dropped]})
            out.append(keep)
        else:
            out.extend(arr)
    return out

def _reason_for_unparsed(ps: ParseSignal, raw: str) -> str:
    """Derive a specific reason string for unparsed/ignored messages."""
    
    # Check for invalid range first (new check)
    if ps.side and len(ps.entries or []) >= 2:
        worst_price = ps.entries[0]
        better_price = ps.entries[1]
        
        if ps.side == 'BUY' and worst_price <= better_price:
            return 'INVALID_RANGE'
        elif ps.side == 'SELL' and worst_price >= better_price:
            return 'INVALID_RANGE'

    text = (raw or '').strip()
    try:
        side_present = bool(_SIDE_RE.search(text))
    except Exception:
        side_present = ps.side is not None
    has_digits = any((ch.isdigit() for ch in text))
    has_at = '@' in text
    if side_present and has_digits and (not has_at):
        return 'MISSING_AT'
    return 'NO_MATCH'

def _report_unparsed(unparsed_reporter: Optional['UnparsedReporter'], unparsed_raw_msg: Optional[object], **kwargs) -> None:
    """
    Call or schedule the reporter safely, and emit a standardized UNPARSED log.
    Hardened: if no running loop, schedule the coroutine on a dedicated background loop thread
    instead of using asyncio.run, to avoid clashing with Telethon or the main loop.
    """
    if not (unparsed_reporter and unparsed_raw_msg):
        return
    # Emit standardized UNPARSED log first
    _log_emit('UNPARSED',
              action=None,
              gk=kwargs.get('gk'),
              source_msg_id=kwargs.get('source_msg_id', getattr(unparsed_raw_msg, 'id', None)),
              reason=kwargs.get('reason'))

    method = None
    if hasattr(unparsed_reporter, 'report_unparsed'):
        method = getattr(unparsed_reporter, 'report_unparsed')
    elif hasattr(unparsed_reporter, 'report'):
        method = getattr(unparsed_reporter, 'report')
    if method is None:
        return

    async def _call_async():
        return await method(unparsed_raw_msg, **kwargs)

    def _ensure_reporter_loop():
        # lazy-start a private loop thread for reporter coroutines
        global _REPORTER_LOOP, _REPORTER_THREAD
        try:
            loop = _REPORTER_LOOP  # type: ignore
            thr = _REPORTER_THREAD  # type: ignore
        except NameError:
            loop = None
            thr = None
        if loop and thr and thr.is_alive():
            return loop
        loop = asyncio.new_event_loop()
        def _run():
            asyncio.set_event_loop(loop)
            loop.run_forever()
        thr = threading.Thread(target=_run, name='unparsed-reporter-loop', daemon=True)
        thr.start()
        globals()['_REPORTER_LOOP'] = loop
        globals()['_REPORTER_THREAD'] = thr
        return loop

    try:
        if inspect.iscoroutinefunction(method):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(method(unparsed_raw_msg, **kwargs))
            except RuntimeError:
                loop = _ensure_reporter_loop()
                asyncio.run_coroutine_threadsafe(_call_async(), loop)
        else:
            method(unparsed_raw_msg, **kwargs)
    except Exception:
        # Swallow to avoid breaking the main flow
        pass
    method = None
    if hasattr(unparsed_reporter, 'report_unparsed'):
        method = getattr(unparsed_reporter, 'report_unparsed')
    elif hasattr(unparsed_reporter, 'report'):
        method = getattr(unparsed_reporter, 'report')
    if method is None:
        return
    try:
        if inspect.iscoroutinefunction(method):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(method(unparsed_raw_msg, **kwargs))
            except RuntimeError:
                asyncio.run(method(unparsed_raw_msg, **kwargs))
        else:
            method(unparsed_raw_msg, **kwargs)
    except Exception:
        pass

def _ps_to_ast(ps: ParseSignal, raw_text: str, quoted_msg_id: Optional[str]=None) -> dict:
    entry_exists = bool(ps.entries)
    entry_obj = None
    if ps.entries:
        if len(ps.entries) == 1:
            entry_obj = {'type': 'value', 'value': ps.entries[0]}
        elif len(ps.entries) >= 2:
            a, b = (ps.entries[0], ps.entries[1])
            lo, hi = (min(a, b), max(a, b))
            entry_obj = {'type': 'band', 'min': lo, 'max': hi}
    tps_raw = ps.tps or []
    tps_out = ['OPEN' if v is None else v for v in tps_raw]
    return {'side': ps.side, 'entry': {'exists': entry_exists, 'value': entry_obj.get('value') if isinstance(entry_obj, dict) else None, 'band': {'min': entry_obj.get('min'), 'max': entry_obj.get('max')} if isinstance(entry_obj, dict) and 'min' in entry_obj else None} if entry_obj else {'exists': False}, 'tps': tps_out, 'sl': ps.sl, 'symbol': ps.symbol, 'text': {'raw': raw_text, 'norm': (raw_text or '').lower()}, 'meta': {'quoted_msg_id': quoted_msg_id}}

def semantic_route(ps: ParseSignal, text: str, reply_to_msg_id: Optional[str]):
    """Build AST and evaluate against YAML rules. Returns a small route object."""
    ast = _ps_to_ast(ps, text, reply_to_msg_id)
    try:
        log.info('SEMANTIC: side=%s entry_exists=%s sl_exists=%s tps_len=%d', ps.side, bool(ps.entries), ps.sl is not None, len(ps.tps or []))
    except Exception:
        pass
    match = sem_evaluate(ast, _SEM_DICT)
    if not match:
        log.info('SEMANTIC: no rule matched')
        return {'kind': 'NONE', 'intent': None, 'rule_id': None, 'ast': ast, 'ps': ps}
    rule_id = match.rule.get('id')
    intent = match.rule.get('intent')
    log.info('SEMANTIC: matched rule=%s intent=%s', rule_id, intent)
    kind = 'MGMT' if str(intent or '').startswith('MGMT') else 'OPEN' if str(intent or '').upper() == 'OPEN' else 'NONE'
    return {'kind': kind, 'intent': str(intent) if intent else None, 'rule_id': rule_id, 'ast': ast, 'ps': ps}


def plan_legs(ps: ParseSignal, legs_count: int) -> tuple[list[Optional[float]], list[Optional[float]], int]:
    """Plan per-leg entries and TPs in one place. Returns (entries, tps, effective_legs).
    
    For single price: Creates 4 legs at that price
    For dual price: Creates 16 legs distributed across 4 equidistant entry points
    """
    
    # Single price: 4 legs only (unchanged)
    if len(ps.entries or []) == 1:
        effective = 4
        entries = [ps.entries[0]] * effective
        
    # Dual price: 16 legs across 4 entry points  
    elif len(ps.entries or []) >= 2:
        worst_price = ps.entries[0]   # First price is always worst
        better_price = ps.entries[1]  # Second price is always better
        
        # Validate range based on direction
        if ps.side == 'BUY':
            if worst_price <= better_price:
                # Invalid: for BUY, worst should be higher
                raise ValueError(f"Invalid BUY range: {worst_price}/{better_price} - first price must be higher than second")
        elif ps.side == 'SELL':
            if worst_price >= better_price:
                # Invalid: for SELL, worst should be lower
                raise ValueError(f"Invalid SELL range: {worst_price}/{better_price} - first price must be lower than second")
        # Note: If no side specified yet (shouldn't happen), we skip validation
        
        # Calculate 4 equidistant entry points
        price_range = abs(better_price - worst_price)
        step = price_range / 3  # 3 steps between 4 points
        
        # Generate entry points (worst to best order)
        entry_points = []
        for i in range(4):
            if ps.side == 'BUY':
                # BUY: worst (high) to best (low)
                price = worst_price - (i * step)
            elif ps.side == 'SELL':
                # SELL: worst (low) to best (high)
                price = worst_price + (i * step)
            else:
                # Fallback if no side (shouldn't happen in practice)
                # Use ascending order as default
                if worst_price < better_price:
                    price = worst_price + (i * step)
                else:
                    price = worst_price - (i * step)
            
            # Round to 2 decimal places
            entry_points.append(round(price, 2))
        
        # Create 16 legs: 4 per entry point (worst to best)
        effective = 16
        entries = []
        for i in range(16):
            entries.append(entry_points[i // 4])
            
    else:
        # No entries specified - use default leg count
        effective = legs_count
        entries = [None] * effective

    # TPs distribution
    has_open = any((t is None for t in (ps.tps or [])))
    symbol_for_pips = (ps.symbol or DEFAULT_SYMBOL)
    
    # For 16 legs with distinct entries, repeat the 4-TP block pattern
    if effective == 16 and entries and (entries[0] is not None):
        blk = _tp_block_from_list(ps.tps or [], has_open)
        # Repeat the block 4 times for 16 legs
        tp_list = (blk * 4)[:effective]
        
    # For 8 legs - this shouldn't happen anymore with new logic, but keep for safety
    elif effective == 8 and entries and (entries[0] is not None) and (len(entries) > 4) and (entries[4] is not None) and (not _is_same_price(entries[0], entries[4], symbol_for_pips)):
        blk = _tp_block_from_list(ps.tps or [], has_open)
        tp_list = (blk * ((effective + 3)//4))[:effective]
        
    # For 4 legs (single price)
    elif effective == 4:
        tp_list = _tp_block_from_list(ps.tps or [], has_open)
        
    # Fallback for other cases
    else:
        tp_list = _tps_for_legs(ps, effective)
    
    return entries, tp_list, effective

def build_open_action(ps: ParseSignal, source_msg_id: str, legs_count: int, leg_volume: float) -> Action:
    side: Side = ps.side
    symbol = (ps.symbol or DEFAULT_SYMBOL).upper()
    # Plan entries/TPs consistently
    entries, tp_list, effective_legs = plan_legs(ps, legs_count)
    legs: List[Leg] = []
    client_id = _client_id_for_message(symbol, source_msg_id)
    for i in range(effective_legs):
        leg_id = _make_leg_id(client_id, i + 1)
        entry_i = entries[i] if i < len(entries) else None
        tp_i = tp_list[i] if i < len(tp_list) else None
        legs.append(Leg(
            leg_id=leg_id,
            symbol=symbol,
            side=side,
            volume=float(leg_volume),
            entry=entry_i,
            sl=ps.sl,
            tp=tp_i,
            tag=leg_id
        ))
    action_id = _make_action_id('OPEN', source_msg_id, legs)
    return Action(action_id=action_id, type='OPEN', legs=legs, source_msg_id=str(source_msg_id))

def post_open_side_effects(action: Action) -> None:
    """Record open in index + optional mock ACKs (file/paper)."""
    try:
        if action.type == 'OPEN':
            action_dict = {'action_id': action.action_id, 'type': action.type, 'source_msg_id': action.source_msg_id, 'legs': [{'tag': getattr(L, 'tag', getattr(L, 'leg_id', None)), 'leg_id': getattr(L, 'leg_id', None), 'symbol': L.symbol, 'volume': L.volume, 'entry': L.entry, 'sl': L.sl, 'tp': L.tp} for L in action.legs]}
            record_open(action_dict)
            if os.getenv('ROUTER_BACKEND', 'file') == 'file' and os.getenv('ROUTER_MODE', 'paper') == 'paper':
                generate_mock_ack_for_open(action_dict)
    except Exception as _e:
        log.warning('REFINDEX/ACK mock failed: %s', _e)

def build_modify_action_from_gk(gk: str, ps: ParseSignal, source_msg_id: str, mgmt_intent: str) -> Optional[Action]:
    legs_meta = list_open_legs(gk) or []
    if not legs_meta:
        return None
    base_symbol = legs_meta[0].get('symbol') if legs_meta and legs_meta[0].get('symbol') else ps.symbol or DEFAULT_SYMBOL
    client_id = _client_id_for_message(str(base_symbol).upper(), source_msg_id)
    legs: List[Leg] = []
    for i, meta in enumerate(legs_meta, start=1):
        leg_id = _make_leg_id(client_id, i)
        base_entry = meta.get('entry')
        # For break-even / risk-free, target SL to entry; otherwise keep existing SL
        target_sl = base_entry if mgmt_intent in ('MGMT_BREAK_EVEN', 'MGMT_RISK_FREE') else meta.get('sl')
        # Side is not used by MT5 for MODIFY, but Pydantic requires it; default safely to 'BUY'
        leg_side = meta.get('side') or ps.side or 'BUY'
        legs.append(Leg(
            leg_id=leg_id,
            symbol=meta.get('symbol') or base_symbol,
            side=leg_side,
            volume=float(meta.get('volume') or DEFAULT_LEG_VOLUME),
            entry=base_entry,
            sl=target_sl,
            tp=meta.get('tp'),
            tag=meta.get('leg_tag') or meta.get('tag'),
            position_ticket=meta.get('position_ticket'),
            order_ticket=meta.get('order_ticket'),
        ))
    legs = _coalesce_modify_legs(legs, gk)

    action_id = _make_action_id('MODIFY', source_msg_id, legs)
    return Action(action_id=action_id, type='MODIFY', legs=legs, source_msg_id=str(source_msg_id))





def post_modify_side_effects(action: Action, gk: str) -> None:
    """Persist target changes to index after a MODIFY action."""
    if not action or action.type != 'MODIFY':
        return
    try:
        for L in action.legs:
            tag = getattr(L, 'tag', None) or getattr(L, 'leg_id', None)
            update_leg_targets(gk, tag, sl=L.sl, tp=L.tp)
    except Exception as _e:
        logging.getLogger('processing').warning('IDX_TARGETS_SAVE_WARN %s', _e, extra={'event': 'IDX_TARGETS_SAVE_WARN'})

def build_cancel_pending_action_from_gk(gk: str, ps: ParseSignal, source_msg_id: str) -> Optional[Action]:
    """Build a CANCEL Action for all *pending* legs under a GK (never touch filled positions)."""
    legs_meta = list_open_legs(gk) or []
    if not legs_meta:
        return None

    # Local, side‑effect free helpers
    def _upper(x):
        try:
            return str(x or "").upper()
        except Exception:
            return ""

    # Common MT5 pending order type labels (hint only; we still require "order" without "position/deal")
    PENDING_TYPE_HINTS = (
        "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP",
        "BUY_STOP_LIMIT", "SELL_STOP_LIMIT", "PENDING"
    )

    def _is_pending(meta: dict) -> bool:
        """
        A leg is *pending* iff there is an order ticket and there is
        NOT a position ticket and NOT a deal. Be conservative.
        """
        res = meta.get("result") or {}
        ord_t = meta.get("order_ticket") or meta.get("order") or res.get("order")
        pos_t = meta.get("position_ticket") or meta.get("position") or res.get("position")
        deal  = meta.get("deal") or meta.get("deal_ticket") or res.get("deal")

        # Must have an order ticket to cancel; if already filled (deal/position), do NOT cancel.
        if not ord_t:
            return False
        if deal or pos_t:
            return False

        # Hints from order type (non-binding; we already required ord_t and no deal/position)
        typ = _upper(meta.get("order_type") or meta.get("type") or res.get("order_type_label") or res.get("type_label"))
        if typ and any(h in typ for h in PENDING_TYPE_HINTS):
            return True

        # Default: treat as pending if there's an order and no position/deal
        return True

    base_symbol = (legs_meta[0].get("symbol") if legs_meta and legs_meta[0].get("symbol") else (ps.symbol or DEFAULT_SYMBOL))
    client_id = _client_id_for_message(str(base_symbol).upper(), source_msg_id)

    legs: List[Leg] = []
    for i, meta in enumerate(legs_meta, start=1):
        if not _is_pending(meta):
            continue
        leg_id = f"{client_id}#{i}"
        res = meta.get("result") or {}
        pos_t = meta.get("position_ticket") or res.get("position")
        ord_t = meta.get("order_ticket") or meta.get("order") or res.get("order")
        if ord_t:
            legs.append(Leg(
                leg_id=leg_id,
                symbol=meta.get("symbol") or base_symbol,
                side=ps.side or "BUY",  # side unused by MT5 for CANCEL; keep for schema
                volume=float(meta.get("volume") or getattr(ps, 'leg_volume', None) or DEFAULT_LEG_VOLUME),
                entry=None, sl=None, tp=None,
                tag=str(meta.get("leg_tag") or meta.get("tag") or i),
                position_ticket=int(pos_t) if pos_t else None,
                order_ticket=int(ord_t) if ord_t else None
            ))

    logging.getLogger('processing').info(
        'CANCEL_COUNTS',
        extra={'event': 'CANCEL_COUNTS', 'gk': gk, 'total': len(legs_meta), 'pending_targeted': len(legs)}
    )

    if not legs:
        return None

    action_id = _make_action_id('CANCEL', source_msg_id, legs)
    act = Action(action_id=action_id, type='CANCEL', legs=legs, source_msg_id=str(source_msg_id))
    _log_emit('CANCEL', action=act, gk=gk, source_msg_id=source_msg_id)
    return act

def build_modify_from_edit(ps: ParseSignal, source_msg_id: str, legs_count: int) -> Optional[Action]:
    """Build MODIFY action when an OPEN message is edited, properly handling 16 legs."""
    gk = resolve_group_key(text=ps.raw, reply_to_msg_id=str(source_msg_id))
    log.info('OPEN_EDIT: GK=%s', gk)
    if not gk:
        return None
    legs_meta = list_open_legs(gk) or []
    if not legs_meta:
        return None
    
    # Use the unified planner for consistency across OPEN and EDIT/MODIFY
    planned_entries, tp_list, planned_size = plan_legs(ps, legs_count)
    
    def _resolve_symbol_for_pips(ps_):
        # 1) explicit single symbol parsed
        sym = getattr(ps_, "symbol", None)
        if sym:
            return sym
        # 2) first candidate in symbols list
        syms = getattr(ps_, "symbols", None)
        if syms and isinstance(syms, (list, tuple)) and syms:
            return syms[0]
        # 3) meta.symbol if present
        meta = getattr(ps_, "meta", None)
        if meta and getattr(meta, "symbol", None):
            return meta.symbol
        # 4) final fallback
        return DEFAULT_SYMBOL
    
    symbol_for_pips = _resolve_symbol_for_pips(ps)
    
    # FIX: Handle 16 legs properly
    if planned_size == 16 and planned_entries and (planned_entries[0] is not None):
        # For 16 legs, repeat the TP block 4 times
        has_open = any((t is None for t in ps.tps or []))
        blk = _tp_block_from_list(ps.tps or [], has_open)
        tp_list = (blk * 4)[:planned_size]  # Repeat block 4 times for 16 legs
        
    elif planned_size >= 8 and planned_entries and (planned_entries[0] is not None) and \
         len(planned_entries) > 4 and (planned_entries[4] is not None) and \
         (not _is_same_price(planned_entries[0], planned_entries[4], symbol_for_pips)):
        # Legacy 8-leg handling (shouldn't happen with new logic, but keep for safety)
        has_open = any((t is None for t in ps.tps or []))
        blk = _tp_block_from_list(ps.tps or [], has_open)
        tp_list = (blk * 2)[:planned_size]  # Repeat block 2 times for 8 legs
        
    elif planned_size == 4:
        has_open = any((t is None for t in ps.tps or []))
        tp_list = _tp_block_from_list(ps.tps or [], has_open)
        
    elif len(ps.entries or []) == 1:
        planned_entries = [ps.entries[0]] * 4
        tp_list = _tps_for_legs(ps, 4)
        
    else:
        planned_entries = [m.get('entry') for m in legs_meta]
        tp_list = _tps_for_legs(ps, len(planned_entries))
    
    # Build legs
    legs: List[Leg] = []
    meta_symbol = legs_meta[0].get('symbol') if legs_meta and legs_meta[0].get('symbol') else ps.symbol or DEFAULT_SYMBOL
    client_id = _client_id_for_message(str(meta_symbol), source_msg_id)
    
    for i, meta in enumerate(legs_meta, start=1):
        leg_id = _make_leg_id(client_id, i)
        tag = meta.get('leg_tag') or leg_id
        entry_i = planned_entries[i - 1] if i - 1 < len(planned_entries) else meta.get('entry')
        new_sl = ps.sl if ps.sl is not None else meta.get('sl')
        new_tp = tp_list[i - 1] if i - 1 < len(tp_list) else meta.get('tp')
        
        legs.append(Leg(
            leg_id=leg_id,
            symbol=meta.get('symbol', meta_symbol),
            side=meta.get('side', ps.side),
            volume=float(meta.get('volume') or DEFAULT_LEG_VOLUME),
            entry=entry_i,
            sl=new_sl,
            tp=new_tp,
            tag=tag,
            position_ticket=meta.get('position_ticket'),
            order_ticket=meta.get('order_ticket')
        ))
        
        # Log resolution for debugging
        resolved_by = 'position_ticket' if meta.get('position_ticket') else 'order_ticket' if meta.get('order_ticket') else 'tag'
        logging.getLogger('processing').info(
            'MGMT_RESOLVE',
            extra={
                'event': 'MGMT_RESOLVE',
                'gk': gk,
                'tag': tag,
                'symbol': meta.get('symbol', meta_symbol),
                'resolved_by': resolved_by,
                'position_ticket': meta.get('position_ticket'),
                'order_ticket': meta.get('order_ticket')
            }
        )
    
    # Handle case where new entries are added (shouldn't happen in normal edit)
    if len(planned_entries) > len(legs_meta):
        for j in range(len(legs_meta) + 1, len(planned_entries) + 1):
            tag_new = f'{meta_symbol or DEFAULT_SYMBOL}#{j}'
            leg_new = Leg(
                leg_id=_make_leg_id(client_id, j),
                symbol=str(meta_symbol),
                side=ps.side,
                volume=float(DEFAULT_LEG_VOLUME),
                entry=planned_entries[j - 1],
                sl=ps.sl,
                tp=tp_list[j - 1] if j - 1 < len(tp_list) else None,
                tag=tag_new
            )
            legs.append(leg_new)
    
    legs = _coalesce_modify_legs(legs, gk)
    action_id = _make_action_id('MODIFY', source_msg_id, legs)
    return Action(action_id=action_id, type='MODIFY', legs=legs, source_msg_id=str(source_msg_id))


def _validate_action(action: 'Action') -> bool:
    try:
        if not action or not getattr(action, 'legs', None):
            return False
        for L in action.legs:
            if not getattr(L, 'symbol', None):
                return False
            if getattr(L, 'volume', None) is None:
                return False
        return True
    except Exception:
        return False


# --- MGMT handler registry ------------------------------------------------
def _handler_modify(intent: str):
    """Factory: returns a handler that builds a MODIFY for a given MGMT intent."""
    def _h(gk: str, ps: ParseSignal, source_msg_id: str) -> Optional[Action]:
        return build_modify_action_from_gk(gk, ps, source_msg_id, intent)
    return _h

def handle_tp2_hit(gk: str, ps: ParseSignal, source_msg_id: str) -> Optional[Action]:
    return build_cancel_pending_action_from_gk(gk, ps, source_msg_id)

MGMT_HANDLERS: dict[str, callable] = {
    'MGMT_BREAK_EVEN': _handler_modify('MGMT_BREAK_EVEN'),
    'MGMT_RISK_FREE':  _handler_modify('MGMT_RISK_FREE'),
    'MGMT_TP2_HIT':    handle_tp2_hit,
}
def build_actions_from_message(source_msg_id: str, text: str, *, is_edit: bool=False, legs_count: int=5, leg_volume: float=DEFAULT_LEG_VOLUME, unparsed_reporter: Optional['UnparsedReporter']=None, unparsed_raw_msg: Optional[object]=None, reply_to_msg_id: Optional[str]=None) -> List[Action]:
    """Parse a Telegram message and build one Action (OPEN/MODIFY) with 1..N legs."""
    if _maybe_ack_ignore(source_msg_id, text, unparsed_raw_msg):
        return []
    _log_rules_state_once()
    legs_count = max(1, min(int(legs_count), MAX_LEGS))
    ps = parse_signal_text(text)
    route = semantic_route(ps, text, reply_to_msg_id)

    # Validate range early for dual-price entries
    if ps.side and len(ps.entries or []) >= 2:
        worst_price = ps.entries[0]
        better_price = ps.entries[1]
        
        # Check for invalid range based on direction
        is_invalid_range = False
        error_msg = ""
        
        if ps.side == 'BUY' and worst_price <= better_price:
            is_invalid_range = True
            error_msg = f"Invalid BUY range: {worst_price}/{better_price} - first price must be higher than second"
        elif ps.side == 'SELL' and worst_price >= better_price:
            is_invalid_range = True
            error_msg = f"Invalid SELL range: {worst_price}/{better_price} - first price must be lower than second"
        
        if is_invalid_range:
            log.warning("INVALID_RANGE: %s", error_msg)
            if ENGINE_FAILSAFE_ON_UNPARSED:
                _report_unparsed(
                    unparsed_reporter, 
                    unparsed_raw_msg, 
                    reason='INVALID_RANGE', 
                    source_msg_id=source_msg_id, 
                    symbol_guess=ps.symbol or DEFAULT_SYMBOL, 
                    side_guess=ps.side
                )
            return []

    if route['kind'] != 'MGMT':
        has_side = bool(_SIDE_RE.search(text or ''))
        has_at = '@' in (text or '')
        missing_entries = not ps.entries
        if SIGNAL_REQUIRE_PRICE and (missing_entries or (has_side and (not has_at))):
            reason = 'MISSING_AT' if has_side and (not has_at) else 'NO_PRICE'
            if ENGINE_FAILSAFE_ON_UNPARSED:
                _report_unparsed(unparsed_reporter, unparsed_raw_msg, reason=reason, source_msg_id=source_msg_id, symbol_guess=ps.symbol or DEFAULT_SYMBOL, side_guess=ps.side)
            return []
    if route['kind'] == 'MGMT' and route['intent']:
        try:
            _reply_nested = getattr(unparsed_raw_msg, 'reply_to_msg_id', None)
        except Exception:
            _reply_nested = None
        quoted = reply_to_msg_id or _reply_nested
        if not quoted:
            _report_unparsed(unparsed_reporter, unparsed_raw_msg, reason='MGMT_NO_QUOTED', source_msg_id=source_msg_id)
            return []
        gk = resolve_group_key(text=text, reply_to_msg_id=str(quoted))
        log.info('MGMT intent=%s reply_to=%s GK=%s', route['intent'], quoted, gk)
        if not gk:
            _report_unparsed(unparsed_reporter, unparsed_raw_msg, reason='MGMT_NO_GK', source_msg_id=source_msg_id)
            return []
        handler = MGMT_HANDLERS.get(route['intent'])
        if not handler:
            _report_unparsed(unparsed_reporter, unparsed_raw_msg, reason='MGMT_NO_HANDLER', source_msg_id=source_msg_id, gk=gk)
            return []
        act = handler(gk, ps, source_msg_id)
        if act and _validate_action(act):
            _log_emit(getattr(act, 'type', 'MODIFY'), action=act, gk=gk, source_msg_id=source_msg_id)
            if getattr(act, 'type', '') == 'MODIFY':
                try:
                    post_modify_side_effects(act, gk)
                except Exception:
                    pass
            return [act]
        return []
    if is_edit:
        act = build_modify_from_edit(ps, source_msg_id, legs_count)
        if act and _validate_action(act):
            try:
                gk_edit = resolve_group_key(text=text, reply_to_msg_id=str(source_msg_id))
                if gk_edit:
                    _log_emit('MODIFY', action=act, gk=gk_edit, source_msg_id=source_msg_id)
                    post_modify_side_effects(act, gk_edit)
            except Exception:
                pass
            return [act]
    chat_id = getattr(unparsed_raw_msg, 'chat_id', None)
    try:
        sender_username = getattr(getattr(unparsed_raw_msg, 'sender', None), 'username', None)
    except Exception:
        sender_username = None
    if not _is_valid_signal(ps, text, chat_id=chat_id, sender_username=sender_username):
        if is_edit and ps.side is not None:
            pass
        else:
            if ENGINE_FAILSAFE_ON_UNPARSED:
                _report_unparsed(unparsed_reporter, unparsed_raw_msg, reason=_reason_for_unparsed(ps, text), symbol_guess=ps.symbol or DEFAULT_SYMBOL, side_guess=ps.side)
            return []
    open_action = build_open_action(ps, source_msg_id, legs_count, leg_volume)
    if not _validate_action(open_action):
        return []
    post_open_side_effects(open_action)
    return [open_action]