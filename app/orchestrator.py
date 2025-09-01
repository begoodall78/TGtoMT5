import os
import logging
from __future__ import annotations
import asyncio, logging
from app.common.logging_config import setup_logging
from app.storage import init_db
from app.infra.telegram_queue import run_telegram_ingest
from app.infra.actions_runner import run_forever

log = logging.getLogger("orchestrator")

async def _runner_task():
    # run the runner in a thread executor to avoid blocking the loop
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, run_forever)

async def main_async():
    # quiet noisy loggers during idle
    if (os.getenv('LOG_LEVEL','INFO').upper() != 'DEBUG'):
        logging.getLogger('telethon').setLevel(logging.WARNING)
        logging.getLogger('telethon.network').setLevel(logging.WARNING)
        logging.getLogger('MetaTrader5').setLevel(logging.WARNING)  # if present

    setup_logging()
    init_db()
    await asyncio.gather(run_telegram_ingest(), _runner_task())

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
