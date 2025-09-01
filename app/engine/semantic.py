from __future__ import annotations
import os
from typing import Any, Dict, List, Optional
import yaml
import re

class SemanticDictionary:
    def __init__(self, data: dict):
        self.data = data or {}
        self.defaults = self.data.get("defaults") or {}
        self.rules = self.data.get("rules") or []
        self.version = self.data.get("dictionary_version") or "NA"


# Add near the top with the other imports
import re

def _split_words(s: str):
    # simple word tokenizer; all lowercase, ignores punctuation
    # e.g. "Move to BE!" -> ["move","to","be"]
    return re.findall(r"[A-Za-z0-9+]+", (s or "").lower())


def load_semantic_dictionary(path: str) -> SemanticDictionary:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return SemanticDictionary(data)
    except Exception:
        return SemanticDictionary({})

# --- simple dotted path getter ---
def _get_path(obj: Any, path: str) -> Any:
    if path is None or path == "":
        return obj
    parts = str(path).split(".")
    cur = obj
    for p in parts:
        if p == "length":
            try:
                return len(cur)
            except Exception:
                return None
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur

def _check_predicate(cond: dict, msg: dict) -> bool:
    # support always: true
    if cond.get("always") is True:
        return True
    field = cond.get("field")
    val = _get_path(msg, field) if field else None

    if "exists" in cond:
        return (val is not None) == bool(cond["exists"])
    if "not_exists" in cond:
        return val is None
    if "is_in" in cond:
        return val in cond["is_in"]
    if "eq" in cond:
        return val == cond["eq"]
    if "neq" in cond:
        return val != cond["neq"]
    if "gte" in cond:
        try:
            return float(val) >= float(cond["gte"])
        except Exception:
            return False
    if "lte" in cond:
        try:
            return float(val) <= float(cond["lte"])
        except Exception:
            return False
    if "contains" in cond:
        try:
            return str(cond["contains"]) in str(val or "")
        except Exception:
            return False
    if "contains_any" in cond:
        try:
            sval = str(val or "")
            return any(str(tok) in sval for tok in cond["contains_any"])
        except Exception:
            return False

    if "contains_any" in cond:
        try:
            sval = str(val or "")
            return any(str(tok) in sval for tok in cond["contains_any"])
        except Exception:
            return False

    # NEW: whole-word match without regex in YAML
    if "contains_word_any" in cond:
        try:
            tokens = set(_split_words(val))
            # cond list matched case-insensitively
            return any((t or "").lower() in tokens for t in cond["contains_word_any"])
        except Exception:
            return False
    return False

def _matches(rule: dict, msg: dict) -> bool:
    wa = rule.get("when_all") or []
    wy = rule.get("when_any") or [{"always": True}]
    wn = rule.get("when_not") or []

    return all(_check_predicate(c, msg) for c in wa) and                any(_check_predicate(c, msg) for c in wy) and                not any(_check_predicate(c, msg) for c in wn)

def _rule_enabled(rule: dict) -> bool:
    if rule.get("enabled") is False:
        return False
    req = rule.get("require_env_true") or []
    import os
    for k in req:
        if not (os.getenv(k, "").lower() in ("1","true","yes")):
            return False
    return True

class MatchResult:

    def __init__(self, rule: dict):
        self.rule = rule

def evaluate(msg: dict, d: SemanticDictionary) -> Optional[MatchResult]:
    matched: List[dict] = []
    for r in d.rules:
        if not _rule_enabled(r):
            continue
        try:
            if _matches(r, msg):
                matched.append(r)
        except Exception:
            pass
    if not matched:
        return None
    # highest priority wins
    matched.sort(key=lambda r: int(r.get("priority", 0)), reverse=True)
    return MatchResult(matched[0])
