# app/monitors/position_poller.py
"""
MT5 Position Poller - Continuously polls MT5 for position updates and stores filled prices.
This runs as a background thread to keep the database synchronized with actual MT5 state.
"""

import logging
import os
import re
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from app.common.database import get_db_manager

log = logging.getLogger("position_poller")

class PositionPoller:
    """
    Polls MT5 positions and orders at regular intervals to track filled prices
    and position states for risk management.
    """
    
    def __init__(self, router, interval: float = None):
        """
        Initialize the position poller.
        
        Args:
            router: Mt5NativeRouter instance
            interval: Polling interval in seconds (default from env or 2.0)
        """
        self.router = router
        self.mt5 = router.mt5
        self.interval = interval or float(os.getenv("POSITION_POLL_INTERVAL", "2.0"))
        self.running = False
        self.thread = None
        
        # Track processed positions to avoid duplicate updates
        self._processed_fills: Set[int] = set()
        
        # Regex for parsing comment field: "msgid_legidx:symbol"
        self._comment_pattern = re.compile(r'^(\d+)_(\d+):(.+)$')
        
        log.info(f"PositionPoller initialized with interval={self.interval}s")
    
    def start(self):
        """Start the polling thread."""
        if self.running:
            log.warning("PositionPoller already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()
        log.info("PositionPoller started")
    
    def stop(self):
        """Stop the polling thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=self.interval + 1)
        log.info("PositionPoller stopped")
    
    def _poll_loop(self):
        """Main polling loop that runs in background thread."""
        while self.running:
            try:
                stats = self._poll_positions()
                if stats['updated'] > 0 or stats['new_fills'] > 0:
                    log.debug(
                        f"Poll stats: checked={stats['checked']} "
                        f"updated={stats['updated']} new_fills={stats['new_fills']}"
                    )
            except Exception as e:
                log.error(f"Position poll error: {e}", exc_info=True)
            
            time.sleep(self.interval)
    
    def _poll_positions(self) -> Dict[str, int]:
        """
        Poll all MT5 positions and update database with filled prices.
        Returns statistics about the polling operation.
        """
        stats = {'checked': 0, 'updated': 0, 'new_fills': 0}
        
        try:
            positions = self.mt5.positions_get()
            if positions is None:
                return stats
            
            for pos in positions:
                stats['checked'] += 1
                
                # Parse the comment to extract message and leg info
                comment = getattr(pos, 'comment', '')
                match = self._comment_pattern.match(comment)
                
                if not match:
                    continue  # Skip positions without our comment pattern
                
                msg_id = match.group(1)
                leg_idx = match.group(2)
                symbol_from_comment = match.group(3)
                
                # Build the group key and leg tag
                group_key = f"OPEN_{msg_id}"
                leg_tag = f"#{leg_idx}"
                
                # Check if this is a new fill
                ticket = pos.ticket
                is_new_fill = ticket not in self._processed_fills
                
                # Update the database
                updated = self._update_position_data(
                    group_key=group_key,
                    leg_tag=leg_tag,
                    position=pos,
                    is_new_fill=is_new_fill
                )
                
                if updated:
                    stats['updated'] += 1
                    if is_new_fill:
                        stats['new_fills'] += 1
                        self._processed_fills.add(ticket)
                        log.info(
                            f"New fill detected: msg={msg_id} leg={leg_idx} "
                            f"ticket={ticket} price={pos.price_open:.5f}"
                        )
        
        except Exception as e:
            log.error(f"Error polling positions: {e}")
        
        # Also check pending orders that might have filled
        self._check_order_fills(stats)
        
        return stats
    
    def _check_order_fills(self, stats: Dict[str, int]):
        """
        Check if any pending orders have been filled by comparing
        stored order tickets with current orders.
        """
        try:
            current_orders = self.mt5.orders_get()
            current_order_tickets = {o.ticket for o in (current_orders or [])}
            
            # Query database for tracked orders
            db_manager = get_db_manager()
            rows = db_manager.fetchall("""
                SELECT group_key, leg_tag, order_ticket 
                FROM legs_index 
                WHERE order_ticket IS NOT NULL 
                  AND is_filled = 0
            """)
            
            for group_key, leg_tag, order_ticket in rows:
                if order_ticket not in current_order_tickets:
                    # Order is gone - might have filled
                    # Try to find corresponding position
                    self._check_for_filled_order(group_key, leg_tag, order_ticket, stats)
        
        except Exception as e:
            log.error(f"Error checking order fills: {e}")
    
    def _check_for_filled_order(self, group_key: str, leg_tag: str, order_ticket: int, stats: Dict[str, int]):
        """
        Check if a missing order has become a position (i.e., filled).
        """
        # Extract message ID and leg index from group_key and leg_tag
        msg_match = re.match(r'OPEN_(\d+)', group_key)
        leg_match = re.match(r'#(\d+)', leg_tag)
        
        if not msg_match or not leg_match:
            return
        
        msg_id = msg_match.group(1)
        leg_idx = leg_match.group(1)
        
        # Look for position with matching comment
        positions = self.mt5.positions_get()
        for pos in (positions or []):
            comment = getattr(pos, 'comment', '')
            if comment == f"{msg_id}_{leg_idx}:{pos.symbol.rstrip(self.router.symbol_suffix)}":
                # Found the filled position!
                self._update_position_data(
                    group_key=group_key,
                    leg_tag=leg_tag,
                    position=pos,
                    is_new_fill=True
                )
                stats['new_fills'] += 1
                self._processed_fills.add(pos.ticket)
                log.info(f"Order {order_ticket} filled as position {pos.ticket}")
                break
    
    def _update_position_data(self, group_key: str, leg_tag: str, position, is_new_fill: bool) -> bool:
        """
        Update the database with position data.
        
        Returns:
            True if update was successful, False otherwise.
        """
        try:
            db_manager = get_db_manager()
            
            # Prepare the update data
            filled_price = position.price_open
            position_ticket = position.ticket
            current_sl = position.sl if position.sl > 0 else None
            current_tp = position.tp if position.tp > 0 else None
            
            if is_new_fill:
                # New fill - update filled status and price
                db_manager.execute_one("""
                    UPDATE legs_index 
                    SET position_ticket = ?,
                        filled_price = ?,
                        entry_price = ?,
                        filled_at = ?,
                        is_filled = 1,
                        current_sl = ?,
                        current_tp = ?
                    WHERE group_key = ? AND leg_tag = ?
                """, (
                    position_ticket,
                    filled_price,
                    filled_price,  # entry_price = filled_price initially
                    datetime.now().isoformat(),
                    current_sl,
                    current_tp,
                    group_key,
                    leg_tag
                ))
            else:
                # Existing position - just update SL/TP if changed
                db_manager.execute_one("""
                    UPDATE legs_index 
                    SET current_sl = ?,
                        current_tp = ?
                    WHERE group_key = ? AND leg_tag = ? AND position_ticket = ?
                """, (
                    current_sl,
                    current_tp,
                    group_key,
                    leg_tag,
                    position_ticket
                ))
            
            return True
            
        except Exception as e:
            log.error(f"Failed to update position data: {e}")
            return False
    
    def get_group_fills(self, group_key: str) -> List[Dict]:
        """
        Get all filled positions for a message group.
        
        Returns:
            List of dicts with position data.
        """
        db_manager = get_db_manager()
        rows = db_manager.fetchall("""
            SELECT leg_tag, position_ticket, filled_price, entry_price,
                   current_sl, current_tp, filled_at, is_risk_free
            FROM legs_index
            WHERE group_key = ? AND is_filled = 1
            ORDER BY leg_tag
        """, (group_key,))
        
        return [
            {
                'leg_tag': row[0],
                'position_ticket': row[1],
                'filled_price': row[2],
                'entry_price': row[3],
                'current_sl': row[4],
                'current_tp': row[5],
                'filled_at': row[6],
                'is_risk_free': row[7]
            }
            for row in rows
        ]
    
    def get_average_entry(self, group_key: str) -> Optional[float]:
        """
        Calculate the average entry price for all filled positions in a group.
        
        Returns:
            Average entry price or None if no fills.
        """
        fills = self.get_group_fills(group_key)
        if not fills:
            return None
        
        total_price = sum(f['entry_price'] for f in fills if f['entry_price'])
        count = sum(1 for f in fills if f['entry_price'])
        
        return total_price / count if count > 0 else None


# Singleton instance
_poller_instance: Optional[PositionPoller] = None

def get_position_poller(router=None) -> Optional[PositionPoller]:
    """Get or create the singleton position poller instance."""
    global _poller_instance
    
    if _poller_instance is None and router is not None:
        if os.getenv("POSITION_POLL_ENABLED", "true").lower() in ("true", "1", "yes"):
            _poller_instance = PositionPoller(router)
            log.info("Position poller instance created")
    
    return _poller_instance

def start_position_polling(router):
    """Convenience function to start position polling."""
    poller = get_position_poller(router)
    if poller:
        poller.start()
        return poller
    return None