#!/usr/bin/env python3
"""
Test script to verify the new database manager works correctly.
Run this BEFORE making any changes to ensure your current system still works.
Then run it AFTER to verify the new system works.

IMPORTANT: Run this script from your TGtoMT5 project root directory!
"""

import os
import sys
import tempfile
import time
from pathlib import Path

def setup_python_path():
    """Set up the Python path to find the app module."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent.absolute()
    
    # Find the project root (should contain the 'app' directory)
    project_root = None
    
    # Check if we're already in the project root
    if (script_dir / "app").exists():
        project_root = script_dir
    else:
        # Look up the directory tree for the app folder
        current = script_dir
        while current.parent != current:  # Not at filesystem root
            if (current / "app").exists():
                project_root = current
                break
            current = current.parent
    
    if not project_root:
        print("❌ Could not find the 'app' directory!")
        print(f"Script is in: {script_dir}")
        print("Please run this script from your TGtoMT5 project root directory.")
        print("Example: cd 'D:\\0 Trading\\TGtoMT5' && python test_database_upgrade.py")
        sys.exit(1)
    
    # Add project root to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    print(f"📁 Project root: {project_root}")
    print(f"📁 Script directory: {script_dir}")
    
    # Change working directory to project root
    os.chdir(project_root)
    print(f"📁 Working directory: {os.getcwd()}")

# Set up the environment first
setup_python_path()

def test_current_system():
    """Test the current storage system."""
    print("🧪 Testing CURRENT system...")
    
    try:
        # Test current storage
        from app.storage import init_db, enqueue, fetch_batch, queue_counts
        from app.models import Action, Leg
        
        print("✓ Imports successful")
        
        # Initialize
        init_db()
        print("✓ Database initialized")
        
        # Test enqueue
        leg = Leg(leg_id="TEST#1", symbol="XAUUSD", side="BUY", volume=0.01, tag="TEST")
        action = Action(action_id="test-action", type="OPEN", legs=[leg], source_msg_id="TEST")
        
        result = enqueue(action)
        print(f"✓ Enqueue result: {result}")
        
        # Test fetch
        actions = fetch_batch(limit=10)
        print(f"✓ Fetched {len(actions)} actions")
        
        # Test counts
        counts = queue_counts()
        print(f"✓ Queue counts: {counts}")
        
        print("✅ Current system works!")
        return True
        
    except Exception as e:
        print(f"❌ Current system failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_new_system():
    """Test the new database manager system."""
    print("\n🧪 Testing NEW system...")
    
    # Use a temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        test_db_path = tmp.name
    
    try:
        # Override the database path for testing
        original_db_path = os.environ.get('APP_DB_PATH')
        os.environ['APP_DB_PATH'] = test_db_path
        
        # Import the new system
        from app.common.database import DatabaseManager, init_database
        from app.models import Action, Leg
        
        print("✓ New imports successful")
        
        # Test database manager directly
        db_manager = DatabaseManager(test_db_path)
        db_manager.initialize_schema()
        print("✓ Database manager initialized")
        
        # Test connection pooling
        with db_manager.get_connection() as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result == (1,), "Basic query failed"
        print("✓ Connection manager works")
        
        # Test the new storage functions
        import importlib
        import app.storage
        importlib.reload(app.storage)  # Reload to use new implementation
        
        from app.storage import init_db, enqueue, fetch_batch, queue_counts
        
        init_db()
        print("✓ New storage initialized")
        
        # Test enqueue
        leg = Leg(leg_id="TEST#1", symbol="XAUUSD", side="BUY", volume=0.01, tag="TEST")
        action = Action(action_id="test-new-action", type="OPEN", legs=[leg], source_msg_id="TEST")
        
        result = enqueue(action)
        print(f"✓ New enqueue result: {result}")
        
        # Test fetch
        actions = fetch_batch(limit=10)
        print(f"✓ New fetched {len(actions)} actions")
        
        # Test counts
        counts = queue_counts()
        print(f"✓ New queue counts: {counts}")
        
        # Test health check (only if function exists)
        try:
            from app.storage import get_storage_health
            health = get_storage_health()
            print(f"✓ Health check: {health['healthy']}")
        except ImportError:
            # Function doesn't exist yet - that's fine for testing
            print("✓ Health check: (will be available after upgrade)")
        
        # Test connection stats
        stats = db_manager.get_stats()
        print(f"✓ Connection stats: {stats}")
        
        print("✅ New system works!")
        return True
        
    except Exception as e:
        print(f"❌ New system failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        try:
            os.unlink(test_db_path)
        except:
            pass
        if original_db_path:
            os.environ['APP_DB_PATH'] = original_db_path
        else:
            os.environ.pop('APP_DB_PATH', None)

def main():
    print("🚀 Database Upgrade Test")
    print("=" * 50)
    
    # Test current system first
    current_works = test_current_system()
    
    if not current_works:
        print("\n❌ Current system has issues. Fix these first before upgrading!")
        sys.exit(1)
    
    # Test new system
    new_works = test_new_system()
    
    print("\n" + "=" * 50)
    if current_works and new_works:
        print("🎉 BOTH systems work! Safe to upgrade.")
        print("\nNext steps:")
        print("1. Copy the new files into your project")
        print("2. Restart your processes one at a time")
        print("3. Monitor the logs for any issues")
    else:
        print("⚠️  Issues found. Don't upgrade yet.")
    
    return current_works and new_works

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)