# Entry Definitions & IDs
**Version:** 2025-08-19  
**Scope:** This document lists stable rule IDs, what each rule matches, required fields, and examples. Keep it in sync with `runtime/data/parser_semantic.yaml` and `runtime/data/entry_catalog.yaml`.

## Legend
- **single_price**: `BUY @ 3353`
- **price_pair**: `SELL @ 3344/3349` (band/range)
- **management**: Replies/quotes that modify an existing entry.

---

## OPEN Rules
### `open.strict.v1`
- **Intent:** `OPEN`
- **Matches:** Complete signals (side, entry, SL, ≥1 TP).  
- **Handles:** `single_price`, `price_pair`  
- **Requires:** `side`, `entry`, `sl`, `tps`  
- **Optional:** `symbol`, `legs`, `leg_volume`  
- **Examples:**
  ```
  BUY @ 3342
  TP 3346
  TP 3350
  TP 3355
  TP OPEN
  SL 3333
  ```
  ```
  SELL @ 3344/3349
  TP 3341
  TP 3337
  TP 3332
  SL 3350
  ```

### `open.partial.v1`
- **Intent:** `OPEN`
- **Matches:** Quick/partial signals (side + entry only).  
- **Handles:** `single_price`, `price_pair`  
- **Requires:** `side`, `entry`  
- **Upgrades To:** `open.strict.v1` when SL/TPs arrive (edited message or management messages).  
- **Examples:**
  ```
  SELL @ 3344/3349
  ```
  ```
  BUY @ 3353
  ```

---

## MANAGEMENT Rules
### `mgmt.break_even.v1`
- **Intent:** `MODIFY`
- **Effect:** Move SL to entry (BE).  
- **Requires:** Quote/reply to original entry (`quoted_msg_id`).  
- **Resolves target:** by quote → by comment tag → by ticket.  
- **Examples:**
  - _Reply:_ `move to BE`
  - _Reply:_ `risk free`

### `mgmt.set.tp.v1`
- **Intent:** `MODIFY`
- **Effect:** Set/update TPs (supports `TP OPEN`).  
- **Requires:** Quote + `tps`.  
- **Examples:**
  ```
  TP 3357
  TP 3361
  TP 3366
  TP OPEN
  ```

### `mgmt.set.sl.v1`
- **Intent:** `MODIFY`
- **Effect:** Set/update SL (price or `entry`/`BE`).  
- **Requires:** Quote + `sl`.  
- **Examples:**
  - `SL 3349`
  - `SL to entry`

### `mgmt.close.partial.v1`
- **Intent:** `CLOSE_PARTIAL`
- **Effect:** Close a % of the quoted position (default 25% if not specified).  
- **Requires:** Quote. Optional: `%`.  
- **Example:** `close 50%`

### `mgmt.close.all.v1`
- **Intent:** `CLOSE_ALL`
- **Effect:** Close all legs for the quoted entry.  
- **Requires:** Quote.  
- **Example:** `close all`

---

## Versioning & Stability
- IDs are **stable**; bump the suffix (`.v1` → `.v2`) only when semantics change in a breaking way.
- Additive improvements (new synonyms, extra safe-guards) should keep the same ID and bump the `dictionary_version` at the top of the YAML.

---

## Next Additions (proposed)
- `mgmt.trail.sl.v1` — trail SL by X pips or to last swing.
- `mgmt.scale.in.v1` — add N more legs to existing entry.
- `mgmt.scale.out.v1` — reduce legs to N (evenly across TPs).
