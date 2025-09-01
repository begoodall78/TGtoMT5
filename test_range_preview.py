#!/usr/bin/env python3
"""
Preview script to test the new 16-leg range entry functionality.
Run this to verify behavior before deployment.

Save this file as: test_range_preview.py in your project root
"""

import json
import sys
from typing import List
from app.processing import build_actions_from_message

def preview_signal(text: str, msg_id: str = "test_msg") -> None:
    """Preview how a signal will be processed"""
    print("\n" + "="*60)
    print("INPUT SIGNAL:")
    print("-"*60)
    print(text)
    print("-"*60)
    
    try:
        actions = build_actions_from_message(msg_id, text, legs_count=5, leg_volume=0.01)
        
        if not actions:
            print("❌ NO ACTION PARSED (likely invalid or incomplete signal)")
            return
        
        action = actions[0]
        print(f"✅ ACTION TYPE: {action.type}")
        print(f"   LEGS COUNT: {len(action.legs)}")
        
        # Group legs by entry price
        entry_groups = {}
        for leg in action.legs:
            entry = leg.entry
            if entry not in entry_groups:
                entry_groups[entry] = []
            entry_groups[entry].append(leg)
        
        print(f"\n   ENTRY DISTRIBUTION:")
        for i, (entry, legs) in enumerate(sorted(entry_groups.items()), 1):
            print(f"   Entry {i}: {entry:.2f} → {len(legs)} legs")
        
        print(f"\n   LEG DETAILS:")
        print(f"   {'Leg':<5} {'Entry':<8} {'TP':<8} {'SL':<8} {'Side':<5} {'Vol':<5}")
        print(f"   {'-'*5} {'-'*8} {'-'*8} {'-'*8} {'-'*5} {'-'*5}")
        
        for i, leg in enumerate(action.legs, 1):
            tp_str = f"{leg.tp:.2f}" if leg.tp else "OPEN"
            sl_str = f"{leg.sl:.2f}" if leg.sl else "None"
            print(f"   {i:<5} {leg.entry:.2f} {tp_str:<8} {sl_str:<8} {leg.side:<5} {leg.volume:.2f}")
        
        # Summary
        print(f"\n   SUMMARY:")
        print(f"   - Total volume: {sum(l.volume for l in action.legs):.2f}")
        print(f"   - Entry range: {min(l.entry for l in action.legs):.2f} - {max(l.entry for l in action.legs):.2f}")
        unique_tps = set(l.tp for l in action.legs if l.tp is not None)
        print(f"   - Unique TPs: {sorted(unique_tps)} + OPEN" if None in [l.tp for l in action.legs] else f"   - Unique TPs: {sorted(unique_tps)}")
        
    except ValueError as e:
        print(f"❌ VALIDATION ERROR: {e}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run test cases"""
    
    test_cases = [
        # Valid BUY range (16 legs)
        """BUY @ 3468/3465
            TP 3480
            TP 3485
            TP 3490
            TP OPEN
            SL 3450""",
        
        # Valid SELL range (16 legs)
        """SELL @ 3465/3468
            TP 3461
            TP 3457
            TP 3452
            TP OPEN
            SL 3475""",
        
        # Single price BUY (4 legs only)
        """BUY @ 3468
            TP 3480
            TP 3485
            TP 3490
            TP OPEN
            SL 3450""",
        
        # Invalid BUY range (should error)
        """BUY @ 3465/3468
            TP 3480
            TP 3485
            SL 3450""",
        
        # Invalid SELL range (should error)
        """SELL @ 3468/3465
            TP 3461
            TP 3457
            SL 3475""",
        
        # Fractional range
        """BUY @ 3468.50/3465.25
            TP 3480
            TP 3485
            TP 3490
            TP OPEN
            SL 3450""",
        
        # Very small range
        """BUY @ 3468.15/3468.00
            TP 3480
            SL 3450""",
        
        # Partial signal (no TPs/SL)
        """BUY @ 3468/3465""",
    ]
    
    print("RANGE ENTRY PREVIEW TEST")
    print("=" * 60)
    print(f"Testing {len(test_cases)} scenarios...")
    
    for i, text in enumerate(test_cases, 1):
        print(f"\n[TEST CASE {i}]")
        preview_signal(text.strip())
    
    print("\n" + "=" * 60)
    print("TEST COMPLETE")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Allow custom signal as argument
        custom_signal = " ".join(sys.argv[1:]).replace("\\n", "\n")
        print("CUSTOM SIGNAL TEST")
        preview_signal(custom_signal)
    else:
        # Run default test cases
        main()