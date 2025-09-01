
# TGtoMT5 — Directory Tidy (2025‑08‑15)

This build updates all default paths to the new `runtime/*` structure.
Paths are now centralized in `app/common/config.py` and can be overridden via `.env`.

**Key runtime directories:**
- `runtime/sessions/` — Telethon sessions
- `runtime/actions/inbox/` — incoming action CSV (Python → MT5 bridge)
- `runtime/actions/ack/` — ACK/ERROR CSV (MT5 bridge → Python)
- `runtime/actions/archive/` — processed actions
- `runtime/logs/` — logs (if file logging is used)
- `runtime/tmp/` — temp/atomic writes
- `runtime/outputs/` — reports, deals, analysis
- `runtime/data/` — durable datasets (non-report DBs)

**Config:**
- Copy `.env.example` to `.env` and adjust if needed.
- Or set environment variables before launching.

**Logging:**
- Use `app/common/logging_setup.py:setup_logging()` early in your main entrypoint.
