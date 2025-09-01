
cd "D:\0 Trading\TGtoMT5"   

rem smoke test

rem queue a test open + close
python -m app.main smoke --log-level DEBUG

rem process queue
python -m app.main drain --log-level DEBUG

rem You should see EXECUTED logs and per-leg files in MT5_ACTIONS_DIR.
rem Running drain again should say Processed 0 actions (idempotency working).


pause