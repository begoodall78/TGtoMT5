@echo off

cd "D:\0 Trading\TGtoMT5"   

rem Test the new config validation command
python -m app.main validate-config

rem Test that it's now built into other commands
python -m app.main health

rem Test existing commands still work
python -m app.main status

pause
