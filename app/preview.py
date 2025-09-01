from __future__ import annotations
import json, argparse, os
from app.processing import build_actions_from_message

def main():
    p = argparse.ArgumentParser(description="Preview how a signal maps to legs.")
    p.add_argument("--legs", type=int, default=int(os.environ.get("DEFAULT_NUM_LEGS", "5")))
    p.add_argument("--volume", type=float, default=float(os.environ.get("DEFAULT_LEG_VOLUME", "0.01")))
    p.add_argument("--edit", action="store_true", help="Preview as an edit (MODIFY)")
    p.add_argument("text", help="Signal text. Tip: wrap in quotes; use \\n for newlines.")
    args = p.parse_args()

    acts = build_actions_from_message(
        source_msg_id="preview",
        text=args.text.replace("\\n", "\n"),
        is_edit=args.edit,
        legs_count=args.legs,
        leg_volume=args.volume,
    )
    if not acts:
        print("No action parsed.")
        return
    a = acts[0]
    out = {
        "type": a.type,
        "legs": [{
            "i": i+1,
            "leg_id": L.leg_id,
            "entry": L.entry,
            "tp": L.tp,
            "sl": L.sl,
            "side": L.side,
            "tag": L.tag,
        } for i, L in enumerate(a.legs)]
    }
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
