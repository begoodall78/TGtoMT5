This archive contains the fixed project structure with the 'app' package root,
fully implemented modules (no '...' placeholders), and __init__.py files for packages.

Run examples:
  python -m app.cli.main run
  python -m app.cli.main drain
  python -m app.services.orchestrator

Environment:
- Supports TG_SESSION_NAME or TG_SESSION, TG_SOURCE_CHAT or TG_SOURCE.
- Uses MT5_ACTIONS_DIR for per-leg CSV drops.
- APP_DB_PATH for SQLite location (defaults to ./data/app.db).
