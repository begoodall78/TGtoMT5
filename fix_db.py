#!/usr/bin/env python3
"""
Fix or recreate the corrupted SQLite database.
Save as: fix_database.py
"""

import os
import sqlite3
import shutil
from datetime import datetime

def check_database(db_path):
    """Check if database is corrupted."""
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA integrity_check")
        result = conn.fetchone()
        conn.close()
        return result[0] == "ok"
    except Exception as e:
        print(f"Database check failed: {e}")
        return False

def backup_database(db_path):
    """Create a backup of the existing database."""
    if os.path.exists(db_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{db_path}.backup_{timestamp}"
        shutil.copy2(db_path, backup_path)
        print(f"Backed up database to: {backup_path}")
        return backup_path
    return None

def recreate_database(db_path):
    """Recreate the database with proper schema."""
    # Remove corrupted database
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed corrupted database: {db_path}")
    
    # Create new database with schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables (adjust schema as needed based on your app)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leg_index (
            leg_id TEXT PRIMARY KEY,
            group_key TEXT NOT NULL,
            message_id TEXT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            volume REAL,
            status TEXT,
            position_ticket INTEGER,
            order_ticket INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_group_key ON leg_index(group_key)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_message_id ON leg_index(message_id)
    """)
    
    # Add any other tables your app needs
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS acks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT,
            leg_id TEXT,
            action_type TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"Created new database with schema: {db_path}")

def main():
    # Get database path from environment or use default
    db_path = os.getenv('APP_DB_PATH', 'runtime/data/app.db')
    
    # For Windows path
    if 'D:/0 Trading/TGtoMT5/runtime/data/app.db' in db_path or 'D:\\' in db_path:
        db_path = db_path.replace('/', '\\')
    
    print(f"Database path: {db_path}")
    
    # Check if database exists
    if not os.path.exists(db_path):
        print("Database doesn't exist. Creating new one...")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        recreate_database(db_path)
        return
    
    # Check if database is corrupted
    if check_database(db_path):
        print("Database is OK!")
        return
    
    print("Database is corrupted. Fixing...")
    
    # Backup the corrupted database
    backup_path = backup_database(db_path)
    
    # Try to recover data if possible
    try:
        old_conn = sqlite3.connect(db_path)
        old_conn.execute("PRAGMA integrity_check")
        
        # Try to dump the data
        with open(f"{db_path}.dump.sql", 'w') as f:
            for line in old_conn.iterdump():
                f.write(f"{line}\n")
        print(f"Dumped recoverable data to: {db_path}.dump.sql")
        old_conn.close()
    except Exception as e:
        print(f"Could not recover data: {e}")
    
    # Recreate the database
    recreate_database(db_path)
    
    print("\nDatabase fixed! You can now run your tests again.")
    print("If you need to restore old data, check the .dump.sql file")

if __name__ == "__main__":
    main()