#!/usr/bin/env python3
"""
One-time migration script to add risk-free columns to existing database.
Run this once before using the new risk-free features.

Usage:
    python scripts/migrate_risk_free.py
"""

import os
import sys
from pathlib import Path

# Add project root to path
script_path = Path(__file__).absolute()
# Go up to find project root (scripts -> project_root)
project_root = script_path.parent.parent
sys.path.insert(0, str(project_root))

print(f"Project root: {project_root}")
print(f"Python path: {sys.path[0]}")

# Now import app modules
from app.common.database import get_db_manager
from app.common.config import Config

def migrate_database():
    """Add new columns for risk-free management."""
    print("=" * 60)
    print("Risk-Free Feature Database Migration")
    print("=" * 60)
    
    # Get database path from config
    config = Config()
    db_path = config.APP_DB_PATH
    print(f"\nDatabase path: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"❌ Database does not exist at {db_path}")
        print("Please run the application first to create the database.")
        return False
    
    print("\nStarting database migration for risk-free features...")
    
    try:
        db_manager = get_db_manager()
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if legs_index table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='legs_index'
            """)
            if not cursor.fetchone():
                print("❌ Table 'legs_index' does not exist!")
                print("The application needs to run at least once to create tables.")
                return False
            
            # Check existing columns
            existing_columns = [col[1] for col in cursor.execute("PRAGMA table_info(legs_index)").fetchall()]
            print(f"\nExisting columns in legs_index: {', '.join(existing_columns)}")
            
            # Define new columns to add
            new_columns = [
                ("filled_price", "REAL", "Actual fill price from MT5"),
                ("filled_at", "DATETIME", "Timestamp when position was filled"),
                ("is_filled", "INTEGER DEFAULT 0", "Flag indicating if order has filled"),
                ("entry_price", "REAL", "Entry price for the position"),
                ("current_sl", "REAL", "Current stop loss level"),
                ("current_tp", "REAL", "Current take profit level"),
                ("is_risk_free", "INTEGER DEFAULT 0", "Flag indicating if position has gone risk-free")
            ]
            
            print("\nAdding new columns...")
            added_count = 0
            skipped_count = 0
            
            for col_name, col_type, description in new_columns:
                if col_name not in existing_columns:
                    sql = f"ALTER TABLE legs_index ADD COLUMN {col_name} {col_type}"
                    print(f"  ✓ Adding {col_name:<15} - {description}")
                    cursor.execute(sql)
                    added_count += 1
                else:
                    print(f"  ⏭️  Skipping {col_name:<15} - already exists")
                    skipped_count += 1
            
            # Add index for faster queries
            print("\nAdding indexes...")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_legs_filled 
                ON legs_index(group_key, is_filled)
            """)
            print("  ✓ Added index: idx_legs_filled")
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_legs_risk_free 
                ON legs_index(group_key, is_risk_free)
            """)
            print("  ✓ Added index: idx_legs_risk_free")
            
            # Commit changes
            conn.commit()
            
            # Verify the changes
            print("\nVerifying migration...")
            new_columns_check = [col[1] for col in cursor.execute("PRAGMA table_info(legs_index)").fetchall()]
            
            all_present = True
            for col_name, _, _ in new_columns:
                if col_name in new_columns_check:
                    print(f"  ✓ Column '{col_name}' verified")
                else:
                    print(f"  ❌ Column '{col_name}' NOT found!")
                    all_present = False
            
            print("\n" + "=" * 60)
            if all_present:
                print("✅ Migration completed successfully!")
                print(f"   Added {added_count} new columns")
                print(f"   Skipped {skipped_count} existing columns")
                print("\n🎉 Database is ready for risk-free management features!")
            else:
                print("❌ Migration may have failed - some columns missing")
                return False
            
            print("=" * 60)
            return True
            
    except Exception as e:
        print(f"\n❌ Migration failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_database_status():
    """Check current database status before migration."""
    print("\nChecking database status...")
    
    try:
        db_manager = get_db_manager()
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"  Tables: {', '.join(tables)}")
            
            # Check legs_index row count
            cursor.execute("SELECT COUNT(*) FROM legs_index")
            count = cursor.fetchone()[0]
            print(f"  Rows in legs_index: {count}")
            
            # Check if any positions are tracked
            cursor.execute("SELECT COUNT(*) FROM legs_index WHERE position_ticket IS NOT NULL")
            pos_count = cursor.fetchone()[0]
            print(f"  Tracked positions: {pos_count}")
            
    except Exception as e:
        print(f"  Warning: Could not check status - {e}")


def main():
    """Main migration function."""
    print("\n🔧 TGtoMT5 Risk-Free Feature Migration Tool")
    
    # Check Python version
    if sys.version_info < (3, 7):
        print(f"❌ Python {sys.version_info.major}.{sys.version_info.minor} detected.")
        print("This migration requires Python 3.7 or higher.")
        return 1
    
    # Check if we're in the right directory
    if not os.path.exists("app"):
        print("❌ 'app' directory not found!")
        print("Please run this script from your TGtoMT5 project root directory.")
        print(f"Current directory: {os.getcwd()}")
        return 1
    
    # Load environment
    from dotenv import load_dotenv
    env_loaded = load_dotenv(override=True)
    if env_loaded:
        print("✓ Environment loaded from .env")
    else:
        print("⚠️ No .env file found, using defaults")
    
    # Check database status
    check_database_status()
    
    # Run migration
    print("\nDo you want to proceed with the migration? (y/n): ", end="")
    response = input().strip().lower()
    
    if response != 'y':
        print("Migration cancelled.")
        return 0
    
    success = migrate_database()
    
    if success:
        print("\n✅ Next steps:")
        print("1. Update your .env file with risk-free settings:")
        print("   POSITION_POLL_ENABLED=true")
        print("   POSITION_POLL_INTERVAL=2.0")
        print("   RISK_FREE_PIPS=10.0")
        print("   RISK_FREE_BE_OFFSET=1.0")
        print("2. Restart your actions_runner to enable position polling")
        print("3. Test with a 'GOING RISK FREE' message")
        return 0
    else:
        print("\n❌ Migration failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())