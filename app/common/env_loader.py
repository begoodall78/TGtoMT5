import os
from pathlib import Path

def load_dotenv(dotenv_path=None):
    path = Path(dotenv_path) if dotenv_path else Path.cwd() / '.env'
    if not path.exists():
        return
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        # strip inline comments (simple)
        if '#' in line:
            parts = line.split('#', 1)
            kv = parts[0].strip()
        else:
            kv = line
        if '=' not in kv:
            continue
        k, v = kv.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v
