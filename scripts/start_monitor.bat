@echo off
REM Launch the MT5 account monitor in its own console

@echo off

cd "D:\0 Trading\TGtoMT5"   

setlocal

REM Optional overrides:
REM   set MONITOR_HEARTBEAT_SYMBOL=XAUUSD
REM   set MONITOR_SYMBOLS=XAUUSD,EURUSD,USDJPY
REM   set MONITOR_INTERVAL_SEC=0.25

rem python -m app.monitors.mt5_account_monitor --apply-actions --log-level INFO 

rem python -m app.monitors.mt5_account_monitor --plain-console 

python -m app.monitors.mt5_account_monitor --plain-console --apply-actions




endlocal

pause
