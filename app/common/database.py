# app/common/database.py
"""
Thread-safe database connection manager for TGtoMT5.
Replaces the scattered database connection code with a centralized, safe approach.
"""

from __future__ import annotations
import sqlite3
import threading
import time
import os
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Generator

log = logging.getLogger("database")

class DatabaseManager:
    """
    Thread-safe SQLite connection manager.
    Handles connection pooling, proper locking, and ensures database integrity.
    """
    
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = str(Path(db_path).resolve())
        self.max_connections = max_connections
        self._lock = threading.RLock()
        self._connections = []
        self._in_use = set()
        self._initialized = False
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection with proper settings."""
        conn = sqlite3.connect(
            self.db_path, 
            timeout=30.0,
            isolation_level=None,  # autocommit mode
            check_same_thread=False  # We handle thread safety ourselves
        )
        
        # Configure for reliability and performance
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL") 
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
        
        return conn
        
    def _get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool or create a new one."""
        with self._lock:
            # Try to reuse an existing connection
            for conn in self._connections:
                if conn not in self._in_use:
                    try:
                        # Test if connection is still valid
                        conn.execute("SELECT 1").fetchone()
                        self._in_use.add(conn)
                        return conn
                    except sqlite3.Error:
                        # Connection is dead, remove it
                        self._connections.remove(conn)
                        try:
                            conn.close()
                        except:
                            pass
                        
            # Create new connection if under limit
            if len(self._connections) < self.max_connections:
                conn = self._create_connection()
                self._connections.append(conn)
                self._in_use.add(conn)
                return conn
                
            # If we're at limit, wait and try again
            # This prevents database lock issues under high load
            log.warning("Database connection pool exhausted, waiting...")
            
        # Wait a bit and try again (recursive with implicit limit)
        time.sleep(0.1)
        return self._get_connection()
        
    def _return_connection(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool."""
        with self._lock:
            if conn in self._in_use:
                self._in_use.remove(conn)
                
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager for getting a database connection.
        
        Usage:
            with db_manager.get_connection() as conn:
                conn.execute("SELECT ...")
        """
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._return_connection(conn)
            
    def execute_one(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a single SQL statement and return the cursor."""
        with self.get_connection() as conn:
            return conn.execute(sql, params)
            
    def executemany(self, sql: str, params_list: list) -> sqlite3.Cursor:
        """Execute SQL with multiple parameter sets."""
        with self.get_connection() as conn:
            return conn.executemany(sql, params_list)
            
    def fetchone(self, sql: str, params: tuple = ()) -> Optional[tuple]:
        """Execute SQL and fetch one row."""
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.fetchone()
            
    def fetchall(self, sql: str, params: tuple = ()) -> list[tuple]:
        """Execute SQL and fetch all rows."""
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.fetchall()
            
    def initialize_schema(self) -> None:
        """Initialize database schema. Safe to call multiple times."""
        with self._lock:
            if self._initialized:
                return
                
            with self.get_connection() as conn:
                # Create all tables
                conn.executescript("""
                    -- Queue table for pending actions
                    CREATE TABLE IF NOT EXISTS queue(
                        action_id TEXT PRIMARY KEY,
                        payload   BLOB NOT NULL,
                        status    TEXT NOT NULL DEFAULT 'PENDING',
                        ts        REAL NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    -- Execution results
                    CREATE TABLE IF NOT EXISTS executions(
                        action_id     TEXT PRIMARY KEY,
                        status        TEXT NOT NULL,
                        router_result BLOB,
                        ts            REAL NOT NULL,
                        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    -- Signal tracking  
                    CREATE TABLE IF NOT EXISTS signals(
                        source_msg_id TEXT PRIMARY KEY,
                        chat_id       INTEGER,
                        msg_ts        TEXT,
                        group_key     TEXT,
                        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    -- Legs index for trade management
                    CREATE TABLE IF NOT EXISTS legs_index(
                        group_key         TEXT,
                        leg_tag           TEXT,
                        symbol            TEXT,
                        volume            REAL,
                        entry             REAL,
                        sl                REAL,
                        tp                REAL,
                        ticket            TEXT,
                        status            TEXT,
                        order_ticket      INTEGER,
                        position_ticket   INTEGER,
                        created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(group_key, leg_tag)
                    );
                    
                    -- Indexes for performance
                    CREATE INDEX IF NOT EXISTS idx_queue_status_ts ON queue(status, ts);
                    CREATE INDEX IF NOT EXISTS idx_executions_ts ON executions(ts);
                    CREATE INDEX IF NOT EXISTS idx_signals_group_key ON signals(group_key);
                    CREATE INDEX IF NOT EXISTS idx_legs_group_key ON legs_index(group_key);
                    CREATE INDEX IF NOT EXISTS idx_legs_tickets ON legs_index(order_ticket, position_ticket);
                """)
                
            self._initialized = True
            log.info("Database schema initialized", extra={"db_path": self.db_path})
            
    def close_all(self) -> None:
        """Close all connections. Call this on shutdown."""
        with self._lock:
            for conn in self._connections:
                try:
                    conn.close()
                except:
                    pass
            self._connections.clear()
            self._in_use.clear()
            
    def get_stats(self) -> dict:
        """Get connection pool statistics for monitoring."""
        with self._lock:
            return {
                "total_connections": len(self._connections),
                "in_use": len(self._in_use),
                "available": len(self._connections) - len(self._in_use),
                "max_connections": self.max_connections,
                "db_path": self.db_path
            }

# Global instance - will be initialized by storage.py
_db_manager: Optional[DatabaseManager] = None

def get_db_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        # Import here to avoid circular imports
        from app.common.config import Config
        config = Config()
        _db_manager = DatabaseManager(config.APP_DB_PATH)
        _db_manager.initialize_schema()
    return _db_manager

def init_database() -> DatabaseManager:
    """Initialize the database manager and return it."""
    db_manager = get_db_manager()
    db_manager.initialize_schema()
    return db_manager