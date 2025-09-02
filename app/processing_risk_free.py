# app/processing_risk_free.py - FINAL VERSION
"""
Risk-free management for trades - handles GOING RISK FREE messages.
Only uses the quoted/replied-to message ID, ignores any numbers in the text.
"""

import logging
import os
import re
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from app.models import Action, Leg

# Avoid circular import
if TYPE_CHECKING:
    from app.processing import ParseSignal
else:
    ParseSignal = None

log = logging.getLogger("processing.risk_free")


def get_pip_multiplier(symbol: str) -> float:
    """Get pip multiplier for a symbol."""
    clean_symbol = re.sub(r'[^A-Z0-9]', '', symbol.upper())
    
    env_key = f"PIP_MULT_{clean_symbol}"
    if env_key in os.environ:
        try:
            return float(os.environ[env_key])
        except ValueError:
            pass
    
    defaults = {
        'XAUUSD': 10.0,
        'GOLD': 10.0,
        'BTCUSD': 1.0,
        'EURUSD': 10000.0,
        'GBPUSD': 10000.0,
        'USDJPY': 100.0,
    }
    
    result = defaults.get(clean_symbol, 10000.0)
    log.debug(f"Pip multiplier for {clean_symbol}: {result}")
    return result


def calculate_breakeven_price(symbol: str, side: str, entry_price: float, 
                             pip_offset: float = 1.0) -> float:
    """Calculate breakeven + offset price for a position."""
    pip_mult = get_pip_multiplier(symbol)
    pip_value = pip_offset / pip_mult
    
    if side == "BUY":
        new_sl = entry_price + pip_value
    else:
        new_sl = entry_price - pip_value
    
    log.info(f"Calculated SL: symbol={symbol} side={side} entry={entry_price:.2f} "
             f"offset={pip_offset} pips, pip_value={pip_value:.5f}, "
             f"new_sl={new_sl:.2f}")
    
    return new_sl


def build_risk_free_action(group_key: str, ps, source_msg_id: str, router=None) -> Optional[Action]:
    """
    Build a MODIFY action to move positions to risk-free.
    Gets actual fill prices from MT5 positions.
    """
    log.info(f"Building risk-free action for {group_key}")
    
    # Extract message ID from group key
    match = re.match(r'OPEN_(\d+)', group_key)
    if not match:
        log.error(f"Cannot extract message ID from group_key: {group_key}")
        return None
    
    target_msg_id = match.group(1)
    log.info(f"Looking for positions from message {target_msg_id}")
    
    # Get positions from MT5 if router is available
    filled_positions = {}
    
    if router and hasattr(router, 'mt5'):
        log.info("Router available, querying MT5 positions")
        
        try:
            positions = router.mt5.positions_get()
            log.info(f"MT5 returned {len(positions) if positions else 0} total positions")
            
            if positions:
                for pos in positions:
                    comment = getattr(pos, 'comment', '')
                    log.debug(f"Position {pos.ticket}: comment='{comment}' symbol={pos.symbol} "
                             f"type={pos.type} price_open={pos.price_open}")
                    
                    # Parse comment: "msgid_legindex:symbol"
                    comment_match = re.match(r'^(\d+)_(\d+):(.+)$', comment)
                    
                    if comment_match:
                        msg_id = comment_match.group(1)
                        leg_idx = comment_match.group(2)
                        
                        if msg_id == target_msg_id:
                            leg_tag = f"#{leg_idx}"
                            
                            filled_positions[leg_tag] = {
                                'position_ticket': pos.ticket,
                                'symbol': pos.symbol,
                                'side': 'BUY' if pos.type == 0 else 'SELL',
                                'volume': pos.volume,
                                'filled_price': pos.price_open,  # ACTUAL fill price
                                'current_sl': pos.sl if pos.sl > 0 else None,
                                'current_tp': pos.tp if pos.tp > 0 else None,
                            }
                            
                            log.info(f"FOUND MT5 POSITION: {leg_tag} ticket={pos.ticket} "
                                   f"filled_at={pos.price_open:.2f} current_sl={pos.sl:.2f}")
                            
        except Exception as e:
            log.error(f"Error getting MT5 positions: {e}", exc_info=True)
    else:
        log.warning("No router available for MT5 positions")
    
    # If no MT5 positions found, try database as fallback
    if not filled_positions:
        log.info("No MT5 positions found, checking database")
        
        try:
            from app.refindex import list_open_legs
            legs_meta = list_open_legs(group_key) or []
            log.info(f"Database returned {len(legs_meta)} legs")
            
            for meta in legs_meta:
                if meta.get('position_ticket'):
                    leg_tag = meta.get('leg_tag') or meta.get('tag')
                    
                    # Try to get actual position from MT5 using ticket
                    if router and hasattr(router, 'mt5'):
                        try:
                            pos_ticket = meta.get('position_ticket')
                            positions = router.mt5.positions_get(ticket=pos_ticket)
                            if positions and len(positions) > 0:
                                pos = positions[0]
                                filled_positions[leg_tag] = {
                                    'position_ticket': pos.ticket,
                                    'symbol': pos.symbol,
                                    'side': 'BUY' if pos.type == 0 else 'SELL',
                                    'volume': pos.volume,
                                    'filled_price': pos.price_open,  # ACTUAL from MT5
                                    'current_sl': pos.sl if pos.sl > 0 else None,
                                    'current_tp': pos.tp if pos.tp > 0 else None,
                                }
                                log.info(f"Got position from MT5 by ticket: {leg_tag} "
                                       f"ticket={pos.ticket} filled_at={pos.price_open:.2f}")
                                continue
                        except:
                            pass
                    
                    # Last resort - use database entry
                    filled_positions[leg_tag] = {
                        'position_ticket': meta.get('position_ticket'),
                        'symbol': meta.get('symbol'),
                        'side': meta.get('side'),
                        'volume': meta.get('volume'),
                        'filled_price': meta.get('entry'),  # WARNING: planned entry, not actual
                        'current_sl': meta.get('sl'),
                        'current_tp': meta.get('tp')
                    }
                    
                    log.warning(f"Using DATABASE entry for {leg_tag}: {meta.get('entry'):.2f} "
                              f"(NOT ACTUAL FILL PRICE!)")
                    
        except Exception as e:
            log.error(f"Error getting database legs: {e}", exc_info=True)
    
    if not filled_positions:
        log.error(f"No positions found for {group_key}")
        return None
    
    log.info(f"Processing {len(filled_positions)} positions for risk-free")
    
    # Calculate weighted average fill price
    total_volume = 0.0
    weighted_sum = 0.0
    symbol = None
    side = None
    
    for leg_tag, pos_data in filled_positions.items():
        if pos_data['filled_price'] and pos_data['volume']:
            price = float(pos_data['filled_price'])
            volume = float(pos_data['volume'])
            weighted_sum += price * volume
            total_volume += volume
            
            # Get symbol and side from first position
            if not symbol:
                symbol = pos_data['symbol']
                side = pos_data['side']
    
    if total_volume == 0:
        log.error("No valid volumes found for weighted average")
        return None
    
    weighted_avg_price = weighted_sum / total_volume
    log.info(f"Weighted average fill price: {weighted_avg_price:.2f} "
             f"(total volume: {total_volume:.2f})")
    
    # Calculate single SL based on weighted average
    be_offset = float(os.getenv("RISK_FREE_BE_OFFSET", "1.0"))
    new_sl = calculate_breakeven_price(
        symbol=symbol,
        side=side,
        entry_price=weighted_avg_price,  # Use weighted average
        pip_offset=be_offset
    )
    
    log.info(f"Single SL for all positions: {new_sl:.2f} "
             f"(weighted avg {weighted_avg_price:.2f} + {be_offset} pips)")
    
    # Build MODIFY legs with the same SL for all
    modify_legs = []
    
    for leg_tag, pos_data in filled_positions.items():
        if not pos_data.get('position_ticket'):
            continue
            
        current_sl = pos_data.get('current_sl', 0) or 0
        
        # Check if update needed
        should_update = False
        if side == 'BUY':
            should_update = (current_sl == 0 or new_sl > current_sl)
        else:
            should_update = (current_sl == 0 or new_sl < current_sl)
        
        if should_update:
            leg_num = re.search(r'#?(\d+)', leg_tag)
            leg_num_str = leg_num.group(1) if leg_num else "1"
            
            leg = Leg(
                leg_id=f"RF_{source_msg_id}#{leg_num_str}",
                symbol=pos_data['symbol'],
                side=pos_data['side'] or 'BUY',
                volume=pos_data['volume'] or 0.01,
                sl=new_sl,  # Same SL for all positions
                tp=pos_data.get('current_tp'),
                tag=leg_tag,
                position_ticket=pos_data['position_ticket']
            )
            modify_legs.append(leg)
            
            log.info(f"MODIFY: {leg_tag} ticket={pos_data['position_ticket']} "
                   f"SL={new_sl:.2f} (from weighted avg)")
    
    if not modify_legs:
        log.warning(f"No positions need SL update")
        return None
    
    log.info(f"Creating MODIFY action with {len(modify_legs)} legs")
    
    # Create the MODIFY action
    action = Action(
        action_id=f"RISK_FREE_{source_msg_id}_{group_key}",
        type="MODIFY",
        legs=modify_legs,
        source_msg_id=source_msg_id
    )
    
    return action


def process_risk_free_message(text: str, ps, source_msg_id: str, router=None, reply_to_msg_id: str = None) -> Optional[Action]:
    """
    Main entry point for processing GOING RISK FREE messages.
    
    IMPORTANT: Only uses the replied-to message ID, ignores any numbers in the text.
    
    Args:
        text: The message text (ignored for parsing numbers)
        ps: ParseSignal object
        source_msg_id: Message ID of the RISK FREE message itself
        router: MT5 router (required for getting actual fill prices)
        reply_to_msg_id: The message ID this is replying to (REQUIRED - this is the trade to go risk-free)
    """
    log.info(f"Processing RISK FREE message (reply_to={reply_to_msg_id})")
    
    # ONLY use the replied-to message ID
    if not reply_to_msg_id:
        log.error("RISK FREE message must be a reply to the trade message")
        return None
    
    group_key = f"OPEN_{reply_to_msg_id}"
    log.info(f"Using replied-to message for group key: {group_key}")
    
    if not router:
        log.warning("No router provided - will try to get fill prices from database")
    
    # Build the risk-free action
    action = build_risk_free_action(group_key, ps, source_msg_id, router)
    
    if action:
        log.info(f"Created RISK FREE action with {len(action.legs)} legs")
        for leg in action.legs:
            log.info(f"  Leg: {leg.tag} sl={leg.sl:.2f}")
    else:
        log.error(f"Failed to create RISK FREE action for {group_key}")
    
    return action