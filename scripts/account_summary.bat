@echo off

cd "D:\0 Trading\TGtoMT5"   


python -m app.monitors.mt5_history_analyzer --reconcile --debug

rem python -m app.monitors.mt5_history_analyzer --reconcile --show-all
rem python -m app.monitors.mt5_history_analyzer --show-all


rem Run with debug info to see what's happening
rem python -m app.monitors.mt5_history_analyzer --debug --show-legs
rem python -m app.monitors.mt5_history_analyzer --debug --show-positions

rem Check just today's trades
rem python -m app.monitors.mt5_history_analyzer --days 1 --debug

rem Check last 30 days with full details
rem python -m app.monitors.mt5_history_analyzer --days 30 --debug --show-legs


rem Basic usage - analyze last 90 days
rem python -m app.monitors.mt5_history_analyzer

rem Analyze last 30 days with debug logging
rem python -m app.monitors.mt5_history_analyzer --days 180 --log-level DEBUG

rem Export to CSV
rem python -m app.monitors.mt5_history_analyzer --csv trade_summary.csv

rem Show breakdown by legs
rem python -m app.monitors.mt5_history_analyzer --show-legs

rem All options combined
rem python -m app.monitors.mt5_history_analyzer --days 180 --csv report.csv --show-legs

pause