# app/common/config_validator.py
"""
Configuration validation and health checking.
Catches common configuration issues before they cause problems.
"""

import os
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from app.common.config import Config

log = logging.getLogger("config_validator")

@dataclass
class ValidationResult:
    """Result of a configuration validation check."""
    is_valid: bool
    level: str  # "error", "warning", "info"
    message: str
    suggestion: Optional[str] = None

class ConfigValidator:
    """Validates TGtoMT5 configuration and provides helpful diagnostics."""
    
    def __init__(self, config: Config):
        self.config = config
        self.results: List[ValidationResult] = []
    
    def validate_all(self) -> List[ValidationResult]:
        """Run all validation checks and return results."""
        self.results.clear()
        
        # Core validations
        self._validate_database_config()
        self._validate_telegram_config()
        self._validate_mt5_config()
        self._validate_directories()
        self._validate_trading_params()
        self._validate_environment()
        
        return self.results
    
    def _add_result(self, is_valid: bool, level: str, message: str, suggestion: str = None):
        """Add a validation result."""
        self.results.append(ValidationResult(is_valid, level, message, suggestion))
    
    def _validate_database_config(self):
        """Validate database configuration."""
        db_path = Path(self.config.APP_DB_PATH)
        
        # Check if database directory exists and is writable
        try:
            db_dir = db_path.parent
            if not db_dir.exists():
                self._add_result(False, "warning", 
                    f"Database directory doesn't exist: {db_dir}",
                    "It will be created automatically, but check permissions")
            elif not os.access(db_dir, os.W_OK):
                self._add_result(False, "error",
                    f"Database directory not writable: {db_dir}",
                    "Fix directory permissions or change APP_DB_PATH")
            else:
                self._add_result(True, "info", f"Database directory OK: {db_dir}")
        except Exception as e:
            self._add_result(False, "error", 
                f"Database path configuration error: {e}",
                "Check APP_DB_PATH setting")
    
    def _validate_telegram_config(self):
        """Validate Telegram configuration."""
        # API credentials
        if not self.config.TG_API_ID or self.config.TG_API_ID == "0":
            self._add_result(False, "error",
                "TG_API_ID not configured",
                "Get API credentials from https://my.telegram.org/apps")
        elif not str(self.config.TG_API_ID).isdigit():
            self._add_result(False, "error",
                f"TG_API_ID should be numeric, got: {self.config.TG_API_ID}",
                "Check your API ID from Telegram")
        else:
            self._add_result(True, "info", "TG_API_ID configured")
        
        if not self.config.TG_API_HASH:
            self._add_result(False, "error",
                "TG_API_HASH not configured", 
                "Get API hash from https://my.telegram.org/apps")
        elif len(self.config.TG_API_HASH) < 32:
            self._add_result(False, "warning",
                "TG_API_HASH looks too short",
                "Verify your API hash from Telegram")
        else:
            self._add_result(True, "info", "TG_API_HASH configured")
        
        # Session configuration
        session_dir = Path(self.config.TG_SESSION_DIR)
        if not session_dir.exists():
            self._add_result(False, "warning",
                f"Session directory doesn't exist: {session_dir}",
                "Will be created on first run")
        else:
            self._add_result(True, "info", f"Session directory OK: {session_dir}")
        
        # Source chats - handle both string and list formats
        if not hasattr(self.config, 'TG_SOURCE_CHATS') or not self.config.TG_SOURCE_CHATS:
            self._add_result(False, "error",
                "No source chats configured",
                "Set TG_SOURCE_CHATS to specify which Telegram channels/chats to monitor")
        else:
            # Handle both string (comma-separated) and list formats
            if isinstance(self.config.TG_SOURCE_CHATS, str):
                sources = [s.strip() for s in self.config.TG_SOURCE_CHATS.split(',') if s.strip()]
            elif isinstance(self.config.TG_SOURCE_CHATS, list):
                sources = [str(s).strip() for s in self.config.TG_SOURCE_CHATS if str(s).strip()]
            else:
                sources = []
            
            if len(sources) == 0:
                self._add_result(False, "error",
                    "TG_SOURCE_CHATS is empty",
                    "Specify channel names or IDs to monitor")
            else:
                self._add_result(True, "info", 
                    f"Monitoring {len(sources)} source(s): {', '.join(sources[:3])}{'...' if len(sources) > 3 else ''}")
    
    def _validate_mt5_config(self):
        """Validate MetaTrader 5 configuration."""
        # Actions directory
        actions_dir = Path(self.config.MT5_ACTIONS_DIR)
        try:
            if not actions_dir.exists():
                self._add_result(False, "warning",
                    f"MT5 actions directory doesn't exist: {actions_dir}",
                    "It will be created automatically")
            elif not os.access(actions_dir, os.W_OK):
                self._add_result(False, "error",
                    f"MT5 actions directory not writable: {actions_dir}",
                    "Fix directory permissions")
            else:
                self._add_result(True, "info", f"MT5 actions directory OK: {actions_dir}")
        except Exception as e:
            self._add_result(False, "error",
                f"MT5 actions directory error: {e}",
                "Check MT5_ACTIONS_DIR setting")
        
        # Router backend
        router_backend = getattr(self.config, 'ROUTER_BACKEND', 'file')
        if router_backend not in ['file', 'native']:
            self._add_result(False, "warning",
                f"Unknown router backend: {router_backend}",
                "Should be 'file' or 'native'")
        else:
            self._add_result(True, "info", f"Router backend: {router_backend}")
            
        # If using native backend, check MT5 availability
        if router_backend == 'native':
            try:
                import MetaTrader5 as mt5
                if mt5.initialize():
                    info = mt5.account_info()
                    if info:
                        self._add_result(True, "info", 
                            f"MT5 connection OK - Account: {info.login} on {info.server}")
                    else:
                        self._add_result(False, "warning",
                            "MT5 connected but no account info",
                            "Check if you're logged into MetaTrader 5")
                    mt5.shutdown()
                else:
                    self._add_result(False, "error",
                        "Cannot initialize MT5",
                        "Make sure MetaTrader 5 is installed and running")
            except ImportError:
                self._add_result(False, "error",
                    "MetaTrader5 package not installed",
                    "Install with: pip install MetaTrader5")
            except Exception as e:
                self._add_result(False, "warning",
                    f"MT5 check failed: {e}",
                    "MetaTrader 5 may not be running")
    
    def _validate_directories(self):
        """Validate all required directories."""
        directories = [
            ("Log directory", self.config.LOG_DIR),
            ("Temp directory", self.config.TMP_DIR), 
            ("Data directory", self.config.DATA_DIR),
            ("Archive directory", self.config.MT5_ARCHIVE_DIR),
        ]
        
        for name, dir_path in directories:
            path = Path(dir_path)
            try:
                if not path.exists():
                    self._add_result(False, "info",
                        f"{name} doesn't exist: {path}",
                        "Will be created automatically")
                elif not os.access(path, os.W_OK):
                    self._add_result(False, "warning",
                        f"{name} not writable: {path}",
                        "May cause issues with file operations")
                else:
                    self._add_result(True, "info", f"{name} OK: {path}")
            except Exception as e:
                self._add_result(False, "warning",
                    f"{name} check failed: {e}",
                    "Verify directory configuration")
    
    def _validate_trading_params(self):
        """Validate trading parameters."""
        # Default leg volume
        try:
            if hasattr(self.config, 'DEFAULT_LEG_VOLUME'):
                volume = float(self.config.DEFAULT_LEG_VOLUME)
                if volume <= 0:
                    self._add_result(False, "error",
                        f"Invalid default volume: {volume}",
                        "Volume must be positive")
                elif volume > 1.0:
                    self._add_result(False, "warning",
                        f"Large default volume: {volume}",
                        "Consider using smaller volumes for safety")
                else:
                    self._add_result(True, "info", f"Default volume: {volume}")
        except (ValueError, TypeError) as e:
            self._add_result(False, "error",
                f"Invalid DEFAULT_LEG_VOLUME: {getattr(self.config, 'DEFAULT_LEG_VOLUME', 'not set')}",
                "Must be a positive number")
        
        # Default number of legs
        try:
            if hasattr(self.config, 'DEFAULT_NUM_LEGS'):
                legs = int(self.config.DEFAULT_NUM_LEGS)
                if legs <= 0 or legs > 20:
                    self._add_result(False, "warning",
                        f"Unusual leg count: {legs}",
                        "Typical range is 1-8 legs per signal")
                else:
                    self._add_result(True, "info", f"Default legs: {legs}")
        except (ValueError, TypeError) as e:
            self._add_result(False, "error",
                f"Invalid DEFAULT_NUM_LEGS: {getattr(self.config, 'DEFAULT_NUM_LEGS', 'not set')}",
                "Must be a positive integer")
    
    def _validate_environment(self):
        """Validate environment settings."""
        env = self.config.APP_ENV.lower()
        if env not in ['dev', 'development', 'prod', 'production', 'test']:
            self._add_result(False, "info",
                f"Unusual APP_ENV: {env}",
                "Consider using 'dev' or 'prod'")
        else:
            self._add_result(True, "info", f"Environment: {env}")
        
        # Log level
        log_level = self.config.LOG_LEVEL.upper()
        if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            self._add_result(False, "warning",
                f"Invalid log level: {log_level}",
                "Should be DEBUG, INFO, WARNING, or ERROR")
        else:
            self._add_result(True, "info", f"Log level: {log_level}")

def validate_config(config: Config = None) -> Tuple[bool, List[ValidationResult]]:
    """
    Validate configuration and return (is_valid, results).
    
    Returns:
        Tuple of (overall_valid, list_of_validation_results)
    """
    if config is None:
        config = Config()
    
    validator = ConfigValidator(config)
    results = validator.validate_all()
    
    # Overall validity - no errors
    has_errors = any(r.level == "error" for r in results)
    is_valid = not has_errors
    
    return is_valid, results

def print_validation_results(results: List[ValidationResult], show_info: bool = True):
    """Print validation results in a nice format."""
    errors = [r for r in results if r.level == "error"]
    warnings = [r for r in results if r.level == "warning"] 
    infos = [r for r in results if r.level == "info"]
    
    print(f"🔍 Configuration Validation Results")
    print("=" * 50)
    
    if errors:
        print(f"\n❌ ERRORS ({len(errors)}):")
        for result in errors:
            print(f"   {result.message}")
            if result.suggestion:
                print(f"   → {result.suggestion}")
    
    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)}):")
        for result in warnings:
            print(f"   {result.message}")
            if result.suggestion:
                print(f"   → {result.suggestion}")
    
    if show_info and infos:
        print(f"\n✅ OK ({len(infos)}):")
        for result in infos:
            print(f"   {result.message}")
    
    # Summary
    print(f"\nSummary: {len(errors)} errors, {len(warnings)} warnings, {len(infos)} OK")
    
    if errors:
        print("❌ Configuration has ERRORS - fix these before running!")
    elif warnings:
        print("⚠️  Configuration has warnings - review recommended")
    else:
        print("✅ Configuration looks good!")

# CLI-friendly validation function
def main():
    """Run configuration validation from command line."""
    import sys
    
    try:
        is_valid, results = validate_config()
        print_validation_results(results)
        
        if not is_valid:
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()