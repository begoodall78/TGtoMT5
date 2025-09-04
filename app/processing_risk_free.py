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

def delete_pending_orders_for_group(group_key: str, router, source_msg_id: str) -> List[dict]:
    """
    Delete all pending orders for a group when going risk-free.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
        router: MT5 router with mt5 connection
        source_msg_id: Source message ID for logging
    
    Returns:
        List of deletion results
    """
    import re
    
    # Extract message ID from group key
    match = re.match(r'OPEN_(\d+)', group_key)
    if not match:
        log.error(f"Cannot extract message ID from group_key: {group_key}")
        return []
    
    target_msg_id = match.group(1)
    deletion_results = []
    
    if not router or not hasattr(router, 'mt5'):
        log.error("No MT5 connection available for order deletion")
        return []
    
    try:
        # Get all pending orders
        orders = router.mt5.orders_get()
        if not orders:
            log.info("No pending orders found")
            return []
        
        orders_to_delete = []
        
        # Find orders matching this message ID
        for order in orders:
            comment = getattr(order, 'comment', '')
            
            # Parse comment: "msgid_legindex:symbol" or "msgid#legindex:symbol"
            comment_match = re.match(r'^(\d+)[_#](\d+)(?::.*)?', comment)
            
            if comment_match:
                msg_id = comment_match.group(1)
                leg_idx = comment_match.group(2)
                
                if msg_id == target_msg_id:
                    orders_to_delete.append({
                        'ticket': order.ticket,
                        'symbol': order.symbol,
                        'comment': comment,
                        'leg': leg_idx
                    })
                    log.info(f"Found pending order to delete: ticket={order.ticket} "
                           f"leg={leg_idx} symbol={order.symbol}")
        
        # Delete each order
        for order_info in orders_to_delete:
            try:
                request = {
                    "action": router.mt5.TRADE_ACTION_REMOVE,
                    "order": order_info['ticket'],
                    "symbol": order_info['symbol'],
                    "magic": getattr(router, 'magic', 99999),
                    "comment": f"Risk free delete msg={target_msg_id}"
                }
                
                result = router.mt5.order_send(request)
                
                if result and result.retcode == router.mt5.TRADE_RETCODE_DONE:
                    deletion_results.append({
                        'ticket': order_info['ticket'],
                        'success': True,
                        'leg': order_info['leg']
                    })
                    log.info(f"Successfully deleted order {order_info['ticket']} "
                           f"(leg {order_info['leg']})")
                else:
                    error_msg = result.comment if result else "Unknown error"
                    deletion_results.append({
                        'ticket': order_info['ticket'],
                        'success': False,
                        'error': error_msg,
                        'leg': order_info['leg']
                    })
                    log.error(f"Failed to delete order {order_info['ticket']}: {error_msg}")
                    
            except Exception as e:
                deletion_results.append({
                    'ticket': order_info['ticket'],
                    'success': False,
                    'error': str(e),
                    'leg': order_info['leg']
                })
                log.error(f"Exception deleting order {order_info['ticket']}: {e}")
        
        # Summary logging
        if deletion_results:
            success_count = sum(1 for r in deletion_results if r['success'])
            fail_count = sum(1 for r in deletion_results if not r['success'])
            log.info(f"Order deletion complete for msg={target_msg_id}: "
                   f"{success_count} succeeded, {fail_count} failed")
        else:
            log.info(f"No pending orders found for msg={target_msg_id}")
            
    except Exception as e:
        log.error(f"Error in delete_pending_orders_for_group: {e}", exc_info=True)
    
    return deletion_results

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
    ENHANCED: Also deletes pending orders for the same message.
    
    Gets actual fill prices from MT5 positions and deletes pending orders.
    """
    log.info(f"Building risk-free action for {group_key}")
    
    # Extract message ID from group key
    match = re.match(r'OPEN_(\d+)', group_key)
    if not match:
        log.error(f"Cannot extract message ID from group_key: {group_key}")
        return None
    
    target_msg_id = match.group(1)
    log.info(f"Looking for positions from message {target_msg_id}")
    
    # STEP 1: Delete pending orders FIRST (before modifying positions)
    if router:
        log.info("Deleting pending orders before setting risk-free SL")
        deletion_results = delete_pending_orders_for_group(group_key, router, source_msg_id)
        
        if deletion_results:
            success_count = sum(1 for r in deletion_results if r['success'])
            log.info(f"Deleted {success_count} pending orders for risk-free")
    else:
        log.warning("No router available - cannot delete pending orders")
    
    # STEP 2: Get positions and calculate SL (your existing code)
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
                                'filled_price': pos.price_open,
                                'current_sl': pos.sl if pos.sl > 0 else None,
                                'current_tp': pos.tp if pos.tp > 0 else None
                            }
                            
                            log.info(f"Found position for leg {leg_tag}: ticket={pos.ticket} "
                                   f"fill={pos.price_open:.2f}")
                            
        except Exception as e:
            log.error(f"Error getting MT5 positions: {e}", exc_info=True)
    
    # Fallback to database if no MT5 positions
    if not filled_positions:
        log.info("Checking database for positions")
        try:
            from app.refindex import list_open_legs
            legs_meta = list_open_legs(group_key) or []
            
            for meta in legs_meta:
                if meta.get('position_ticket'):
                    leg_tag = meta.get('leg_tag') or meta.get('tag')
                    filled_positions[leg_tag] = {
                        'position_ticket': meta.get('position_ticket'),
                        'symbol': meta.get('symbol'),
                        'side': meta.get('side'),
                        'volume': meta.get('volume'),
                        'filled_price': meta.get('entry'),
                        'current_sl': meta.get('sl'),
                        'current_tp': meta.get('tp')
                    }
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
        entry_price=weighted_avg_price,
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