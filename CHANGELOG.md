
## Added
- MGMT: `TP2 HIT` cancels remaining pending legs (requires quoted entry → GK).

## 2025-08-18
- Fix: Segmenter now selects the first block that contains a signal header (`BUY|SELL @`), avoiding misses on forwarded/annotated messages.
- Add: Fallback YAML rule `open_block_anywhere_v1` (mode: `whole_message`, prio 90) to catch valid OPEN messages anywhere in text.
- Bump: `dictionary_version` → `+2025-08-18-fwdblock`.

# Changelog

## 006.4 — 2025-08-15
- Moved `app/domain/unparsed_reporter.py` → `app/infra/unparsed_reporter.py` (infra concern).
- `telegram_queue` now imports `UnparsedReporter` from `app.infra.unparsed_reporter` and calls `await reporter.report_unparsed(event.message, ...)`.
- Removed now-empty `app/domain/` package.
