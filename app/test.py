import os
os.environ["TP_DUP_FIRST"]="true"
from app.processing import build_actions_from_message
text = """BUY @ 3354/3350

TP 3357
TP 3361
TP 3366
TP OPEN
SL 3349
"""
acts = build_actions_from_message("msg123", text, is_edit=False, legs_count=5, leg_volume=0.01)
a = acts[0]
print([L.tp for L in a.legs])  # expect [3357, 3357, 3361, 3366, None]
