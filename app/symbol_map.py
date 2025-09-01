SYMBOL_ALIASES = {
    "GOLD": "XAUUSD",
    "XAU": "XAUUSD",
    "US30": "US30",
}

def normalize_symbol(sym: str) -> str:
    s = sym.upper().strip()
    return SYMBOL_ALIASES.get(s, s)
