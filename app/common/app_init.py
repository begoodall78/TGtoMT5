# app/common/app_init.py
"""
Centralized application initialization.
Replaces scattered logging/database setup with one simple function call.
"""

import logging
import os
from typing import Optional

from app.common.logging_setup import setup_logging
from app.common.database import init_database
from app.common.config import Config

log = logging.getLogger("app_init")

def initialize_app(
    log_level: str = "INFO", 
    require_database: bool = True,
    quiet_noisy_loggers: bool = None,
    component_name: str = None
) -> Config:
    """
    Initialize the TGtoMT5 application with logging, database, and config.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        require_database: Whether to initialize the database
        quiet_noisy_loggers: Whether to quiet telethon/MT5 loggers (auto-detects if None)
        component_name: Name of the component being initialized (for logging)
    
    Returns:
        Config: The application configuration object
    """
    
    # Setup logging first
    setup_logging(log_level)
    
    # Quiet noisy third-party loggers unless we're debugging
    if quiet_noisy_loggers is None:
        quiet_noisy_loggers = log_level.upper() != 'DEBUG'
    
    if quiet_noisy_loggers:
        logging.getLogger('telethon').setLevel(logging.WARNING)
        logging.getLogger('telethon.network').setLevel(logging.WARNING) 
        logging.getLogger('MetaTrader5').setLevel(logging.WARNING)
        # Add more noisy loggers as needed
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    # Initialize database if required
    if require_database:
        db_manager = init_database()
        db_stats = db_manager.get_stats()
        log.debug("Database initialized", extra={
            "event": "DB_INIT",
            "db_path": db_stats["db_path"],
            "max_connections": db_stats["max_connections"]
        })
    else:
        log.debug("Database initialization skipped")
    
    # Load and validate configuration
    config = Config()
    
    # Run configuration validation
    try:
        from app.common.config_validator import validate_config
        is_config_valid, validation_results = validate_config(config)
        
        # Log critical configuration issues
        errors = [r for r in validation_results if r.level == "error"]
        if errors:
            log.error("Configuration errors found", extra={
                "event": "CONFIG_ERRORS", 
                "errors": [r.message for r in errors[:3]]  # First 3 errors
            })
            # Don't fail startup, but warn user
            
        # Log a summary
        warnings = [r for r in validation_results if r.level == "warning"]
        log.info("Configuration validated", extra={
            "event": "CONFIG_VALIDATION",
            "is_valid": is_config_valid,
            "error_count": len(errors),
            "warning_count": len(warnings)
        })
        
    except Exception as e:
        log.warning(f"Configuration validation failed: {e}", extra={
            "event": "CONFIG_VALIDATION_FAILED"
        })
    
    # Log initialization complete
    init_info = {
        "event": "APP_INIT",
        "component": component_name or "unknown",
        "log_level": log_level,
        "database_enabled": require_database,
        "config_app_env": config.APP_ENV
    }
    
    if component_name:
        log.info(f"{component_name.upper()}_INIT", extra=init_info)
    else:
        log.info("APP_INIT", extra=init_info)
    
    return config

def initialize_telegram_queue() -> Config:
    """Initialize for the Telegram queue process."""
    return initialize_app(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        require_database=True,
        component_name="telegram_queue"
    )

def initialize_actions_runner() -> Config:
    """Initialize for the actions runner process.""" 
    return initialize_app(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        require_database=True,
        component_name="actions_runner"
    )

def initialize_account_monitor() -> Config:
    """Initialize for the account monitor process."""
    return initialize_app(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        require_database=True,
        component_name="account_monitor"
    )

def initialize_cli_tool(log_level: str = "INFO") -> Config:
    """Initialize for CLI tools and commands."""
    return initialize_app(
        log_level=log_level,
        require_database=True,
        component_name="cli"
    )

def initialize_preview_tool() -> Config:
    """Initialize for preview/diagnostic tools (no database needed)."""
    return initialize_app(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        require_database=False,
        component_name="preview"
    )

# Health check function
def get_app_health() -> dict:
    """Get overall application health status."""
    try:
        # Test database
        from app.storage import get_storage_health
        storage_health = get_storage_health()
        
        # Test configuration
        config = Config()
        config_health = {
            "app_env": config.APP_ENV,
            "db_path_exists": os.path.exists(config.APP_DB_PATH),
            "session_dir_exists": os.path.exists(config.TG_SESSION_DIR),
            "actions_dir_exists": os.path.exists(config.MT5_ACTIONS_DIR)
        }
        
        # Overall health
        overall_healthy = (
            storage_health.get("healthy", False) and
            config_health["db_path_exists"] and
            config_health["session_dir_exists"]
        )
        
        return {
            "healthy": overall_healthy,
            "storage": storage_health,
            "config": config_health,
            "timestamp": os.time.time() if hasattr(os, 'time') else None
        }
        
    except Exception as e:
        return {
            "healthy": False,
            "error": str(e),
            "timestamp": None
        }