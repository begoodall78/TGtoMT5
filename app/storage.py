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