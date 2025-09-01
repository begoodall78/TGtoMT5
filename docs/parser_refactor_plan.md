# Parser Refactor & Management Message Roadmap

## Phase 0 — Prep (half-day) ✅ **Complete** (half‑day)
- **Decide file paths & env key:** `PARSER_DICT_YAML=runtime/data/parser_dict.yaml`.
- **Versioning:** Add `dictionary_version: "2025-08-15.1"` at top of YAML (will help debugging).
- **Test corpus:** Collect ~20 real messages (open + edits + management intents) for repeatable tests.

---

## Phase 1 — Pull dictionary into YAML (single source, no fallbacks) ✅ **Complete** (single source, no fallbacks)
**Goal:** Parser reads exclusively from YAML; code contains **no embedded defaults**.

1. **Define schema (YAML)**
   ```yaml
   dictionary_version: "2025-08-15.1"
   symbols:
     aliases:
       XAUUSD: ["XAU", "GOLD", "XAUUSD"]
   keywords:
     buy:  ["buy", "long"]
     sell: ["sell", "short"]
     tp:   ["tp", "take profit"]
     sl:   ["sl", "stop", "stop loss"]
     tp_open: ["tp open", "open tp"]
   parsing:
     entry_patterns:
       - '^@?\s*(?P<entry>\d+(?:\.\d+)?)\s*/\s*(?P<worse>\d+(?:\.\d+)?)$'
       - '^@\s*(?P<entry>\d+(?:\.\d+)?)$'
     tp_line:  '^(?:tp)\s+(?P<price>open|\d+(?:\.\d+)?)$'
     sl_line:  '^(?:sl|stop(?:\s+loss)?)\s+(?P<price>\d+(?:\.\d+)?)$'
   defaults:
     default_symbol: "XAUUSD"
     default_num_legs: 5
     require_symbol: false
     require_price: true
   ```

2. **Loader + validator**
   - Reads YAML, validates against a Pydantic schema, compiles regexes.
   - No fallbacks: invalid/missing YAML → fail fast with error.

3. **Parser refactor**
   - Replace in‑code constants/regex/aliases with reads from dict object.
   - External behavior identical to baseline.

4. **Tests**
   - Use ~20‑message corpus to compare JSON output to baseline.

5. **Acceptance**
   - YAML present → parser runs.
   - No hardcoded dictionaries remain.
   - 100% parity vs baseline corpus.

---

## Phase 2 — Quoted message understanding ✅ **Complete**
**Goal:** Resolve the “original OPEN” that a management message refers to.

1. **Reference extraction module**
   - Resolve in order: `reply_to_msg_id` → deep‑link → (later) text fingerprint.
   - Output group key: `GK = f"OPEN_{source_msg_id}"`.

2. **Minimal DB indexing**
   - `signals(source_msg_id PK, chat_id, msg_ts, group_key, text_hash NULL)`.
   - On each OPEN → insert row.
   - ACK handler: `legs_index(group_key, leg_tag, symbol, volume, broker_ticket, status)`.

3. **APIs**
   - `resolve_group_key(event) -> GK|None`
   - `list_open_legs(GK) -> List[LegRef]`

4. **Tests**
   - Reply‑based management resolves correct GK and enumerates live legs.

5. **Acceptance**
   - Quoted/replied messages resolve GK.
   - Can list open legs for GK.

---

## Phase 3 — Expand dictionary for more entry definitions 🟨 **Nearly complete (deferred)**
*Note:* Convenience variants (e.g., `entry @ 1234`, `buy 1234`) intentionally deferred to a separate project.
**Goal:** Let YAML define additional open signal variations.

1. **Extend YAML**
   ```yaml
   parsing:
     entry_patterns:
       - '^@?\s*(?P<entry>\d+(?:\.\d+)?)\s*/\s*(?P<worse>\d+(?:\.\d+)?)$'
       - '^@\s*(?P<entry>\d+(?:\.\d+)?)$'
       - '^(?:entry|buy|sell)\s*@\s*(?P<entry>\d+(?:\.\d+)?)$'
   ```

2. **Parser changes**
   - Iterate patterns in order; stop at first match.

3. **Tests**
   - Existing corpus unchanged.
   - Add new variants and verify.

4. **Acceptance**
   - Old signals parse identically.
   - New variants parse as intended.

---

## Phase 4 — Add trade management message types ✅ **Complete**
Guards hardened: missing reference or empty target group returns a single ERROR/ACK with clear reason. All MGMT paths emit an `MGMT_RESOLVE` audit log.
**Goal:** YAML‑driven verbs for break‑even, risk‑free, move‑SL.

1. **Extend YAML**
   ```yaml
   management:
     break_even:
       triggers: ["break even", "breakeven", "be"]
       behavior: { type: "move_sl_to_entry", cushion: 0.0 }
     risk_free:
       triggers: ["risk free", "risk-free"]
       behavior: { type: "move_sl_to_entry", cushion: 0.10 }
     move_sl:
       pattern: '^move (?:sl|stop) to (?P<price>\d+(?:\.\d+)?)$'
       behavior: { type: "move_sl_to_price" }
     scope:
       require_reference: true
       target: "group"
   ```

2. **Builder**
   - `build_management_actions(GK, mgmt_intent) -> List[MODIFY]`
   - Resolve GK, load legs from index, apply behavior, emit per‑leg MODIFY.

3. **Guards**
   - Require reference; no GK → ERROR.
   - GK resolves but no open legs → ERROR.

4. **Tests**
   - Each verb resolves and produces correct MODIFYs.

5. **Acceptance**
   - Management verbs work without affecting OPEN flow.
   - Clear errors for missing/ambiguous refs.

---

## Sequencing
- Phase 0–1: 0.5–1 day
- Phase 2: 0.5 day
- Phase 3: 0.5 day
- Phase 4: 0.5–1 day
