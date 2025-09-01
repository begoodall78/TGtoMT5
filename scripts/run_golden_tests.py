import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json, sys, os, glob
os.environ.setdefault("ENGINE_RULES_MODE", "semantic")
os.environ.setdefault("SEMANTIC_DICT_PATH", "runtime/data/parser_semantic.yaml")
os.environ.setdefault("ENGINE_FAILSAFE_ON_UNPARSED", "true")
os.environ.setdefault("MAX_LEGS", "5")
os.environ.setdefault("DEFAULT_LEG_VOLUME", "0.01")
from app.processing import build_actions_from_message

def summarize_action(a):
    return {
        "type": a.type,
        "legs": len(getattr(a,"legs",[])),
        "sl_any": any(L.sl is not None for L in getattr(a,"legs",[])),
        "tp_any": any(L.tp is not None for L in getattr(a,"legs",[])),
    }

def run(cases_dir):
    txts = sorted(glob.glob(os.path.join(cases_dir, "*.txt")))
    total = 0
    failed = 0
    for txt in txts:
        name = os.path.splitext(os.path.basename(txt))[0]
        exp_path = os.path.join(cases_dir, name + ".json")
        with open(txt, "r", encoding="utf-8") as f:
            raw = f.read()
        acts = build_actions_from_message(name, raw, is_edit=False, legs_count=5, leg_volume=0.01)
        got = [summarize_action(a) for a in (acts or [])]
        if os.path.exists(exp_path):
            exp = json.load(open(exp_path, "r", encoding="utf-8"))
        else:
            exp = []
        ok = (got == exp)
        total += 1
        if not ok:
            failed += 1
            print(f"[FAIL] {name}")
            print("  expected:", json.dumps(exp))
            print("  got     :", json.dumps(got))
        else:
            print(f"[OK]   {name}")
    print(f"Summary: {total-failed}/{total} passed")
    return 1 if failed else 0

if __name__ == "__main__":
    cases_dir = sys.argv[1] if len(sys.argv)>1 else "runtime/cases"
    raise SystemExit(run(cases_dir))
