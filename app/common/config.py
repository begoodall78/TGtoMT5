import os
from pathlib import Path
from .env_loader import load_dotenv

class Config:
    def __init__(self, dotenv=None):
        load_dotenv(dotenv)
        # Core runtime locations
        self.TG_SESSION_DIR   = self._resolve(os.getenv('TG_SESSION_DIR', 'runtime/sessions'))
        self.MT5_ACTIONS_DIR  = self._resolve(os.getenv('MT5_ACTIONS_DIR', 'runtime/actions/inbox'))
        self.MT5_ACK_DIR      = self._resolve(os.getenv('MT5_ACK_DIR', 'runtime/actions/ack'))
        self.MT5_ARCHIVE_DIR  = self._resolve(os.getenv('MT5_ARCHIVE_DIR', 'runtime/actions/archive'))
        self.LOG_DIR          = self._resolve(os.getenv('LOG_DIR', 'runtime/logs'))
        self.TMP_DIR          = self._resolve(os.getenv('TMP_DIR', 'runtime/tmp'))
        self.OUTPUT_BASE      = self._resolve(os.getenv('OUTPUT_BASE', 'runtime/outputs'))
        self.REPORTS_HTML_DIR = self._resolve(os.getenv('REPORTS_HTML_DIR', 'runtime/outputs/reports_html'))
        self.REPORTS_XLSX_DIR = self._resolve(os.getenv('REPORTS_XLSX_DIR', 'runtime/outputs/reports_xlsx'))
        self.DEALS_CLEAN_DIR  = self._resolve(os.getenv('DEALS_CLEAN_DIR', 'runtime/outputs/deals_clean'))
        self.ANALYSIS_DIR     = self._resolve(os.getenv('ANALYSIS_DIR', 'runtime/outputs/analysis'))
        self.DATA_DIR         = self._resolve(os.getenv('DATA_DIR', 'runtime/data'))
        self.APP_DB_PATH      = self._resolve(os.getenv('APP_DB_PATH', str(self.DATA_DIR / 'app.db')))

        # Modes & logging
        self.APP_ENV   = os.getenv('APP_ENV', 'prod').lower()
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

        # Telegram
        self.TG_API_ID     = os.getenv('TG_API_ID')
        self.TG_API_HASH   = os.getenv('TG_API_HASH')
        self.TG_PHONE      = os.getenv('TG_PHONE', '')
        self.TG_PASSWORD   = os.getenv('TG_PASSWORD', '')
        self.TG_LOGIN_CODE = os.getenv('TG_LOGIN_CODE', '')
        self.TG_SESSION_NAME = os.getenv('TG_SESSION_NAME', 'tg')
        self.TG_SESSION    = os.getenv('TG_SESSION', None)
        self.TG_SOURCE_CHATS = [s.strip() for s in os.getenv('TG_SOURCE_CHATS', '').split(',') if s.strip()]

        # Parser & legs
        self.DEFAULT_NUM_LEGS    = int(os.getenv('DEFAULT_NUM_LEGS', '4'))
        self.DEFAULT_LEG_VOLUME  = float(os.getenv('DEFAULT_LEG_VOLUME', '0.01'))
        self.TP_DUP_FIRST        = os.getenv('TP_DUP_FIRST', 'false').lower() == 'true'
        self.PARSER_DEBUG        = int(os.getenv('PARSER_DEBUG', '0'))
        self.SIGNAL_REQUIRE_SYMBOL = os.getenv('SIGNAL_REQUIRE_SYMBOL', 'false').lower() == 'true'
        self.SIGNAL_REQUIRE_PRICE  = os.getenv('SIGNAL_REQUIRE_PRICE', 'true').lower() == 'true'
        self.SIGNAL_MIN_TEXT_LEN   = int(os.getenv('SIGNAL_MIN_TEXT_LEN', '0'))
        self.DEFAULT_SYMBOL        = os.getenv('DEFAULT_SYMBOL', '')

        # Router
        self.ROUTER_BACKEND = os.getenv('ROUTER_BACKEND', 'file').lower()
        self.ROUTER_MODE    = os.getenv('ROUTER_MODE', 'paper').lower()

        # MT5
        self.MT5_PATH     = os.getenv('MT5_PATH', '')
        self.MT5_LOGIN    = os.getenv('MT5_LOGIN', '')
        self.MT5_PASSWORD = os.getenv('MT5_PASSWORD', '')
        self.MT5_SERVER   = os.getenv('MT5_SERVER', '')
        self.MT5_MAGIC    = int(os.getenv('MT5_MAGIC', '1'))
        self.MT5_DEVIATION= int(os.getenv('MT5_DEVIATION', '10'))
        self.MT5_FILLING  = int(os.getenv('MT5_FILLING', '2'))
        self.SYMBOL_SUFFIX= os.getenv('SYMBOL_SUFFIX', '')
        self.MT5_SUPPRESS_NETTING_WARNING = os.getenv('MT5_SUPPRESS_NETTING_WARNING', 'false').lower() == 'true'
        self.MT5_FIRST_LEG_WORSE_PIPS  = float(os.getenv('MT5_FIRST_LEG_WORSE_PIPS', '0'))
        self.MT5_FIRST_LEG_WORSE_PRICE = float(os.getenv('MT5_FIRST_LEG_WORSE_PRICE', '0'))
        # New canonical first-price tolerance (in pips)
        self.MT5_FIRST_PRICE_WORSE_PIPS = float(os.getenv('MT5_FIRST_PRICE_WORSE_PIPS', '0'))
        # Optional overrides: 'XAUUSD=0.10;XAGUSD=0.01'
        _ov = os.getenv('PIP_SIZE_OVERRIDES', '')
        ov_map = {}
        if _ov.strip():
            for part in _ov.split(';'):
                part = part.strip()
                if not part: continue
                if '=' in part:
                    k,v = part.split('=',1)
                    try: ov_map[k.strip().upper()] = float(v.strip())
                    except Exception: pass
        self.PIP_SIZE_OVERRIDES = ov_map

        # Unparsed forwarding ops
        self.UNPARSED_FORWARD_ENABLED = os.getenv('UNPARSED_FORWARD_ENABLED', 'false').lower() == 'true'
        self.UNPARSED_REVIEW_CHAT_ID  = os.getenv('UNPARSED_REVIEW_CHAT_ID', '')
        self.UNPARSED_OPS_ACK_CHAT_ID = os.getenv('UNPARSED_OPS_ACK_CHAT_ID', '')
        self.UNPARSED_LOG_DIR         = self._resolve(os.getenv('UNPARSED_LOG_DIR', str(self.LOG_DIR)))
        self.UNPARSED_DEDUP_WINDOW_SECONDS = int(os.getenv('UNPARSED_DEDUP_WINDOW_SECONDS', '300'))
        self.UNPARSED_KEEP_DAYS            = int(os.getenv('UNPARSED_KEEP_DAYS', '30'))

        # Retries/slippage
        self.MAX_SLIPPAGE_POINTS = int(os.getenv('MAX_SLIPPAGE_POINTS', '50'))
        self.RETRY_POLICY        = int(os.getenv('RETRY_POLICY', '3'))

        for p in [
            self.TG_SESSION_DIR, self.MT5_ACTIONS_DIR, self.MT5_ACK_DIR, self.MT5_ARCHIVE_DIR,
            self.LOG_DIR, self.TMP_DIR, self.OUTPUT_BASE, self.REPORTS_HTML_DIR,
            self.REPORTS_XLSX_DIR, self.DEALS_CLEAN_DIR, self.ANALYSIS_DIR, self.DATA_DIR
        ]:
            try:
                Path(p).mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

    def _resolve(self, value):
        p = Path(value)
        return p if p.is_absolute() else (Path.cwd() / p).resolve()
