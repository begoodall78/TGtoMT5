# YAML Structure Overview

- dictionary_version: 2025-08-17.2
- locale: en-GB
- entities: 3
- pattern groups: open_signal, quoted_signal, management, admin
- action groups: OPEN, MGMT_BREAK_EVEN, MGMT_MOVE_SL, MGMT_RISK_FREE

## Patterns (first 10)
- [open_signal] open_basic → OPEN
  - Regexes: 1
  - Preview: `(?i)^(?P<side>BUY|SELL)\s*@\s*(?P<p1>\d{3,5}(?:[.\d]{0,4})?)(?:\s*/\s*(?P<p2>\d{3,5}(?:[.\d]{0,4})?)…`
- [quoted_signal] quoted_block → QUOTED_ORIGIN
  - Regexes: 1
  - Preview: `(?s)\x22(?P<quote>[\s\S]+?)\x22 `
- [management] to_break_even → MGMT_BREAK_EVEN
  - Regexes: 1
  - Preview: `(?i)break\s*even|\bBE\b`
- [management] move_sl_to → MGMT_MOVE_SL
  - Regexes: 1
  - Preview: `(?i)move\s+sl\s+to\s+(?P<new_sl>\d{3,5}(?:[.\d]{0,4})?)`
- [management] go_risk_free → MGMT_RISK_FREE
  - Regexes: 1
  - Preview: `(?i)risk\s*free|protect\s*zero`
- [admin] admin_ack → ADMIN_ACK
  - Regexes: 1
  - Preview: `(?i)filled|executed|done|opened`

## Actions (all)
- [OPEN] build → OPEN
  - Required: —
  - Optional: —
- [MGMT_BREAK_EVEN] apply → MGMT_BREAK_EVEN
  - Required: —
  - Optional: —
- [MGMT_MOVE_SL] apply → MGMT_MOVE_SL
  - Required: —
  - Optional: —
- [MGMT_RISK_FREE] apply → MGMT_RISK_FREE
  - Required: —
  - Optional: —


### MGMT: TP2 HIT — Cancel Pending Legs
- **Trigger**: `TP2 HIT` or `TP 2 HIT` with a quoted original entry message.
- **Intent**: `MGMT_TP2_HIT`
- **Resolution**: by quote → GK = `OPEN_{quoted_msg_id}`
- **Action**: emit `CANCEL` for **pending** legs only.
