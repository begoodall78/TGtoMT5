# app/storage.py - Updated to use DatabaseManager
"""
Storage operations using the new thread-safe DatabaseManager.
This replaces the old connection management with proper pooling and locking.
"""

from __future__ import annotations
import logging
import time
from typing import List, Optional
from pydantic import TypeAdapter

from app.models import Action, RouterResult
from app.common.database import get_db_manager

log = logging.getLogger("storage")

# Keep this function for backward compatibility
def get_db_path() -> str:
    """Get the database path."""
    return get_db_manager().db_path

def init_db() -> None:
    """Initialize database schema. Safe to call multiple times."""
    db_manager = get_db_manager()
    db_manager.initialize_schema()
    log.info("Database initialized", extra={"db_path": db_manager.db_path})

def enqueue(action: Action) -> bool:
    """
    Add an action to the processing queue.
    Returns True if added, False if already exists (dedup).
    """
    db_manager = get_db_manager()
    
    try:
        db_manager.execute_one(
            "INSERT INTO queue(action_id, payload, status, ts) VALUES(?,?,?,?)",
            (action.action_id, action.model_dump_json().encode("utf-8"), "PENDING", time.time())
        )
        log.debug("Action enqueued", extra={"action_id": action.action_id, "type": action.type})
        return True
        
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            # Already exists -> treat as dedup OK
            log.debug("Action already enqueued (dedup)", extra={"action_id": action.action_id})
            return False
        else:
            log.error("Failed to enqueue action", extra={"action_id": action.action_id, "error": str(e)})
            raise

def fetch_batch(limit: int = 32) -> List[Action]:
    """
    Fetch a batch of pending actions for processing.
    Returns them in order of creation (FIFO).
    """
    db_manager = get_db_manager()
    
    rows = db_manager.fetchall(
        "SELECT action_id, payload FROM queue WHERE status='PENDING' ORDER BY ts LIMIT ?",
        (limit,)
    )
    
    # Convert JSON payloads back to Action objects
    actions: List[Action] = []
    ta = TypeAdapter(Action)
    
    for action_id, payload in rows:
        try:
            action = ta.validate_json(payload)
            actions.append(action)
        except Exception as e:
            log.error("Failed to deserialize action", extra={
                "action_id": action_id, 
                "error": str(e)
            })
            # Mark this action as failed so we don't keep trying
            mark_failed(action_id, f"Deserialization error: {e}")
    
    if actions:
        log.debug("Fetched batch", extra={"count": len(actions), "requested": limit})
    
    return actions

def mark_in_progress(action_id: str) -> None:
    """Mark an action as currently being processed."""
    db_manager = get_db_manager()
    
    result = db_manager.execute_one(
        "UPDATE queue SET status='IN_PROGRESS' WHERE action_id=?", 
        (action_id,)
    )
    
    if result.rowcount == 0:
        log.warning("Tried to mark non-existent action as in-progress", 
                   extra={"action_id": action_id})

def mark_done(action_id: str, result: RouterResult) -> None:
    """Mark an action as completed and save the execution result."""
    db_manager = get_db_manager()
    
    with db_manager.get_connection() as conn:
        # Update queue status
        conn.execute("UPDATE queue SET status='DONE' WHERE action_id=?", (action_id,))
        
        # Save execution result
        conn.execute(
            "INSERT OR REPLACE INTO executions(action_id, status, router_result, ts) VALUES(?,?,?,?)",
            (action_id, result.status, result.model_dump_json().encode("utf-8"), time.time())
        )
    
    log.debug("Action marked as done", extra={
        "action_id": action_id, 
        "status": result.status,
        "has_error": bool(getattr(result, 'error_code', None))
    })

def mark_failed(action_id: str, error_message: str) -> None:
    """Mark an action as failed with an error message."""
    db_manager = get_db_manager()
    
    # Create a failed RouterResult
    from app.models import RouterResult
    failed_result = RouterResult(
        action_id=action_id,
        status="ERROR", 
        error_code=9999,
        error_text=error_message
    )
    
    mark_done(action_id, failed_result)

def already_executed(action_id: str) -> Optional[RouterResult]:
    """Check if an action has already been executed."""
    db_manager = get_db_manager()
    
    row = db_manager.fetchone(
        "SELECT router_result FROM executions WHERE action_id=?", 
        (action_id,)
    )
    
    if not row:
        return None
        
    try:
        ta = TypeAdapter(RouterResult)
        return ta.validate_json(row[0])
    except Exception as e:
        log.error("Failed to deserialize execution result", extra={
            "action_id": action_id, 
            "error": str(e)
        })
        return None

def reset_in_progress_to_pending() -> int:
    """
    Reset any IN_PROGRESS actions back to PENDING.
    This is useful on startup to recover from crashes.
    """
    db_manager = get_db_manager()
    
    result = db_manager.execute_one(
        "UPDATE queue SET status='PENDING' WHERE status='IN_PROGRESS'"
    )
    
    count = result.rowcount or 0
    if count > 0:
        log.info("Reset in-progress actions to pending", extra={"count": count})
    
    return count

def queue_counts() -> dict:
    """Get count of actions by status for monitoring."""
    db_manager = get_db_manager()
    
    rows = db_manager.fetchall("SELECT status, COUNT(*) FROM queue GROUP BY status")
    counts = {status: count for status, count in rows}
    
    # Always include common statuses even if count is 0
    for status in ['PENDING', 'IN_PROGRESS', 'DONE']:
        if status not in counts:
            counts[status] = 0
            
    return counts

def cleanup_old_records(days_old: int = 7) -> dict:
    """
    Clean up old completed actions and executions.
    Returns counts of records deleted.
    """
    db_manager = get_db_manager()
    cutoff_ts = time.time() - (days_old * 24 * 60 * 60)
    
    with db_manager.get_connection() as conn:
        # Delete old completed queue entries
        queue_result = conn.execute(
            "DELETE FROM queue WHERE status='DONE' AND ts < ?", 
            (cutoff_ts,)
        )
        
        # Delete old executions
        exec_result = conn.execute(
            "DELETE FROM executions WHERE ts < ?", 
            (cutoff_ts,)
        )
    
    counts = {
        "queue_deleted": queue_result.rowcount or 0,
        "executions_deleted": exec_result.rowcount or 0,
        "cutoff_days": days_old
    }
    
    if counts["queue_deleted"] > 0 or counts["executions_deleted"] > 0:
        log.info("Cleaned up old records", extra=counts)
    
    return counts

# Health check function
def get_storage_health() -> dict:
    """Get storage system health information."""
    db_manager = get_db_manager()
    
    try:
        # Basic connectivity test
        test_result = db_manager.fetchone("SELECT 1")
        is_healthy = test_result is not None
        
        # Get basic stats
        stats = {
            "healthy": is_healthy,
            "db_path": db_manager.db_path,
            "connection_stats": db_manager.get_stats(),
            "queue_counts": queue_counts() if is_healthy else {},
        }
        
        if is_healthy:
            # Get total execution count
            exec_count = db_manager.fetchone("SELECT COUNT(*) FROM executions")
            stats["total_executions"] = exec_count[0] if exec_count else 0
        
        return stats
        
    except Exception as e:
        return {
            "healthy": False,
            "error": str(e),
            "db_path": db_manager.db_path
        }
    
# Add these functions to the end of your existing storage.py file:

def update_filled_price(group_key: str, leg_tag: str, filled_price: float, 
                        position_ticket: int = None) -> bool:
    """
    Update the filled price for a specific leg.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
        leg_tag: The leg identifier (e.g., "#1")
        filled_price: The actual fill price from MT5
        position_ticket: Optional position ticket number
    
    Returns:
        True if updated successfully, False otherwise.
    """
    db_manager = get_db_manager()
    
    try:
        db_manager.execute_one("""
            UPDATE legs_index 
            SET filled_price = ?,
                entry_price = COALESCE(entry_price, ?),
                position_ticket = COALESCE(?, position_ticket),
                is_filled = 1,
                filled_at = COALESCE(filled_at, datetime('now'))
            WHERE group_key = ? AND leg_tag = ?
        """, (filled_price, filled_price, position_ticket, group_key, leg_tag))
        
        log.debug(f"Updated filled price: group={group_key} leg={leg_tag} price={filled_price}")
        return True
        
    except Exception as e:
        log.error(f"Failed to update filled price: {e}", extra={
            "group_key": group_key,
            "leg_tag": leg_tag,
            "error": str(e)
        })
        return False


def get_filled_legs(group_key: str) -> List[dict]:
    """
    Get all filled legs for a message group.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
    
    Returns:
        List of dicts containing leg information.
    """
    db_manager = get_db_manager()
    
    try:
        rows = db_manager.fetchall("""
            SELECT 
                leg_tag,
                symbol,
                side,
                volume,
                filled_price,
                entry_price,
                position_ticket,
                order_ticket,
                current_sl,
                current_tp,
                filled_at,
                is_risk_free
            FROM legs_index
            WHERE group_key = ? AND is_filled = 1
            ORDER BY leg_tag
        """, (group_key,))
        
        return [
            {
                'leg_tag': row[0],
                'symbol': row[1],
                'side': row[2],
                'volume': row[3],
                'filled_price': row[4],
                'entry_price': row[5],
                'position_ticket': row[6],
                'order_ticket': row[7],
                'current_sl': row[8],
                'current_tp': row[9],
                'filled_at': row[10],
                'is_risk_free': row[11]
            }
            for row in rows
        ]
        
    except Exception as e:
        log.error(f"Failed to get filled legs: {e}", extra={
            "group_key": group_key,
            "error": str(e)
        })
        return []


def get_average_filled_price(group_key: str) -> Optional[float]:
    """
    Calculate the average filled price for all filled positions in a group.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
    
    Returns:
        Average filled price or None if no filled positions.
    """
    filled_legs = get_filled_legs(group_key)
    
    if not filled_legs:
        return None
    
    # Calculate weighted average by volume
    total_volume = 0.0
    weighted_sum = 0.0
    
    for leg in filled_legs:
        if leg['filled_price'] and leg['volume']:
            volume = float(leg['volume'])
            price = float(leg['filled_price'])
            weighted_sum += price * volume
            total_volume += volume
    
    if total_volume > 0:
        return weighted_sum / total_volume
    
    return None


def get_pending_legs(group_key: str) -> List[dict]:
    """
    Get all pending (unfilled) legs for a message group.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
    
    Returns:
        List of dicts containing leg information.
    """
    db_manager = get_db_manager()
    
    try:
        rows = db_manager.fetchall("""
            SELECT 
                leg_tag,
                symbol,
                side,
                volume,
                order_ticket,
                sl,
                tp
            FROM legs_index
            WHERE group_key = ? 
              AND is_filled = 0 
              AND order_ticket IS NOT NULL
            ORDER BY leg_tag
        """, (group_key,))
        
        return [
            {
                'leg_tag': row[0],
                'symbol': row[1],
                'side': row[2],
                'volume': row[3],
                'order_ticket': row[4],
                'sl': row[5],
                'tp': row[6]
            }
            for row in rows
        ]
        
    except Exception as e:
        log.error(f"Failed to get pending legs: {e}", extra={
            "group_key": group_key,
            "error": str(e)
        })
        return []


def mark_legs_risk_free(group_key: str) -> bool:
    """
    Mark all filled legs in a group as having gone risk-free.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
    
    Returns:
        True if successful, False otherwise.
    """
    db_manager = get_db_manager()
    
    try:
        db_manager.execute_one("""
            UPDATE legs_index 
            SET is_risk_free = 1
            WHERE group_key = ? AND is_filled = 1
        """, (group_key,))
        
        log.info(f"Marked legs as risk-free: group={group_key}")
        return True
        
    except Exception as e:
        log.error(f"Failed to mark legs as risk-free: {e}", extra={
            "group_key": group_key,
            "error": str(e)
        })
        return False


def is_group_risk_free(group_key: str) -> bool:
    """
    Check if a group has already gone risk-free.
    
    Args:
        group_key: The group identifier (e.g., "OPEN_1234")
    
    Returns:
        True if any filled leg is marked as risk-free, False otherwise.
    """
    db_manager = get_db_manager()
    
    try:
        result = db_manager.fetchone("""
            SELECT COUNT(*) 
            FROM legs_index 
            WHERE group_key = ? AND is_risk_free = 1
        """, (group_key,))
        
        return result[0] > 0 if result else False
        
    except Exception as e:
        log.error(f"Failed to check risk-free status: {e}", extra={
            "group_key": group_key,
            "error": str(e)
        })
        return False