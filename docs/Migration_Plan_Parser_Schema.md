# Parser Schema Migration Plan (Low-Risk, Incremental)

**Goal:** Evolve from ad‑hoc `patterns/actions` to a scalable **intents + slots + rules** schema with segmentation and coexistence policies — without breaking current behavior.

---

## Overview of Phases

**Phase 0 — Prep & Safety Nets (No functional change)**
- Add feature flags (ENV or YAML) to control the new engine:
  - `ENGINE_ENABLE_RULES=false`
  - `ENGINE_SEGMENT_TOP_BLOCK=false`
  - `ENGINE_COEXIST_POLICY=false`
  - `ENGINE_SLOTS=false`
- Ensure `tools/describe_dictionary.py` can read both old and new shapes and show which engine is active.
- Create a *golden corpus* from real TG messages:
  - **golden/entry/**: 50–100 real entry messages (with + without trailing text).
  - **golden/mgmt/**: 50–100 management messages (with reply_to and without).
  - **golden/unparsed/**: ~50 deliberately noisy messages.
- Add a tiny test runner (no external deps):
  - `tools/test_dictionary.py` which loads the YAML and runs goldens to assert expected **intent** (or unparsed).

**Phase 1 — Add New Schema (Coexist with Old)**
- Introduce `intents`, `slots`, `rules` sections **alongside** existing `patterns/actions`.
- Migrate the current *entry* block regex into `rules.open_block_v1` (mode=`segment_top_block`).
- Keep old `patterns.entry` untouched.
- Parser change (behind flag): add **segmenter** (split into blocks by blank lines).
- Run tests in **shadow**: old engine decides; new engine logs verdict only (no effect).

**Phase 2 — Coexistence & Conflict Policies**
- Implement `coexist_policy` in parser (behind flag), supporting:
  - `skip_if_entry_present` and `prefer_entry` (start with these two).
- Migrate **BREAK_EVEN** to `rules.mgmt_break_even_sentence` with `coexist_policy: skip_if_entry_present` and `requires_reply_to: true`.
- Shadow-compare old vs new on the golden corpus and live logs; triage discrepancies.

**Phase 3 — Slot Extraction + Normalization**
- Add shared slot parsers for `entry`, `tps`, `sl`, `side`, `symbol`.
- Implement `extract:` mapping in rules (kept simple initially).
- Keep the output **Action**/CSV shape identical to today.
- Gate with `ENGINE_SLOTS=false` initially; enable only in tests first.

**Phase 4 — Cutover (Feature Flags Flip)**
- Enable `ENGINE_SEGMENT_TOP_BLOCK=true` and `ENGINE_COEXIST_POLICY=true` in staging.
- Enable `ENGINE_ENABLE_RULES=true` while still leaving old `patterns` compiled (for quick rollback).
- Compare counts/intents over a day of live messages (shadow vs active) and ensure deltas are within tolerance.
- When stable, remove old `patterns.entry` first; keep mgmt old patterns one more cycle.

**Phase 5 — Cleanup & Hardening**
- Deprecate `patterns` in docs; freeze it for compatibility.
- Build additional rules (TP synonyms, punctuation variants, “TP1/TP2”, lowercase).
- Add negative tests (e.g., “YOU CAN KEEP A BUY … SL AT BE.” after an entry).
- Documentation refresh & “How to add a rule” cookbook.

---

## Detailed Step-by-Step

### Phase 0 — Prep & Safety Nets
1. **Feature Flags**
   - Implement a tiny `get_flag(name, default)` helper reading from ENV and optional YAML `meta.flags`.
   - Flags: `ENGINE_ENABLE_RULES`, `ENGINE_SEGMENT_TOP_BLOCK`, `ENGINE_COEXIST_POLICY`, `ENGINE_SLOTS`.
2. **Golden Corpus & Runner**
   - `golden/entry/valid_*.txt` → expected `OPEN`.
   - `golden/mgmt/valid_*.txt` → expected `MGMT_*`.
   - `golden/unparsed/noise_*.txt` → expected `UNPARSED`.
   - `tools/test_dictionary.py`: loads YAML, compiles both engines (old/new if present), runs goldens, prints compact pass/fail and diffs.
3. **Describe Tool**
   - Show active flags, rule counts, top regex previews, and guard/coexist summaries.

**Exit Criteria:** Test runner passes on old engine. New flags compile and echo status.

---

### Phase 1 — New Schema (Additive)
1. **YAML scaffold**
   ```yaml
   intents:
     OPEN: { required_slots: [side, entry, tps, sl], optional_slots: [symbol, size] }
     MGMT_BREAK_EVEN: { required_slots: [], optional_slots: [target], requires_reply_to: true }

   slots:
     side:  { type: enum, values: [BUY, SELL] }
     entry: { type: price_or_band }
     tps:   { type: list_of_prices_or_open }
     sl:    { type: price }

   rules:
     - id: open_block_v1
       intent: OPEN
       priority: 100
       mode: segment_top_block
       coexist_policy: prefer_self
       regex:
         flags: [i, m, s]
         pattern: >
           ^\s*(?P<side>BUY|SELL)\s*@\s*
           (?P<entry>\d+(?:\.\d+)?(?:\s*/\s*\d+(?:\.\d+)?)?)
           (?P<tp_block>(?:\s*\n\s*TP\s+(?:OPEN|\d+(?:\.\d+)?))+)
           \s*\n\s*SL\s+(?P<sl>\d+(?:\.\d+)?)
       extract:
         entry: { from: entry, as: price_or_band }
         tps:   { from: tp_block, with: parse_tp_lines }
         sl:    { from: sl, as: price }
   ```
2. **Parser: Segmenter (flagged)**
   - `segment_top_block`: block 0 = start → first blank line.
   - When flag is off, current code path remains untouched.
3. **Shadow Runs**
   - New engine computes a decision but doesn’t affect output; logs both engines’ verdicts.

**Exit Criteria:** No crashes; shadow logs visible; decisions recorded.

---

### Phase 2 — Coexistence Policies
1. **Parser:** implement `coexist_policy`:
   - `skip_if_entry_present`: skip mgmt rule if an entry already matched anywhere.
   - `prefer_entry`: if both matched, pick `OPEN`.
2. **YAML:** add mgmt rule
   ```yaml
   - id: mgmt_break_even_sentence
     intent: MGMT_BREAK_EVEN
     priority: 10
     mode: whole_message
     coexist_policy: skip_if_entry_present
     regex:
       flags: [i, m]
       pattern: >
         ^(?=.*\bBE\b|\bbreak[- ]?even\b).*
   ```
3. **Shadow Compare**
   - Run golden + live shadow; triage differences.

**Exit Criteria:** Trailing-text issue resolved in shadow. No new false positives.

---

### Phase 3 — Slots Extraction
1. Implement `extract` handlers:
   - `as: price` / `price_or_band`.
   - `with: parse_tp_lines` → list of TP prices + OPEN markers.
2. Maintain current CSV/action fields.
3. Enable with `ENGINE_SLOTS=true` **only in tests** first.

**Exit Criteria:** Golden tests pass with parsed slots; CSV identical on known examples.

---

### Phase 4 — Cutover
1. Enable `ENGINE_SEGMENT_TOP_BLOCK=true` + `ENGINE_COEXIST_POLICY=true` and `ENGINE_ENABLE_RULES=true` in staging.
2. Keep old engine compiled for quick rollback.
3. Monitor KPIs:
   - Parse rate by type vs historical.
   - Error/ACK counts.
   - “MGMT without reply_to” rate should drop.
4. When stable, remove `patterns.entry`. Keep old mgmt patterns one more cycle.

**Exit Criteria:** Stable metrics over a defined volume; no elevated error ACKs.

---

### Phase 5 — Cleanup & Extensions
- Deprecate `patterns` in docs; freeze it.
- Build robust synonyms:
  - `TP1/TP2`, `TP:`, punctuation, lowercase.
- Add negative tests for common pitfalls.
- Extend `coexist_policy` where needed.

**Exit Criteria:** Docs + tests updated; contributors guide added.

---

## Risk Mitigation & Rollback

- **Dual-engine shadowing:** New engine doesn’t affect output until flags flip.
- **Feature flags:** Atomic toggles for segmentation, coexistence, rules, slots.
- **Golden corpus:** Locks in today’s behavior; prevents regressions.
- **Quick rollback:** Old `patterns` remain compiled until after stable window.
- **Audit logs:** Small, structured — `ENTRY from top_block`, `MGMT skipped: entry_present`.

---

## Deliverables Checklist

- [ ] Feature flags in parser
- [ ] Golden corpus + `tools/test_dictionary.py`
- [ ] Describe tool shows flags & rules
- [ ] YAML: `intents`, `slots`, `rules` scaffold with `open_block_v1`
- [ ] Parser: segmenter (flagged)
- [ ] Parser: coexist policy (flagged)
- [ ] YAML: `mgmt_break_even_sentence`
- [ ] Slot extractors (flagged)
- [ ] Staging cutover (flags on)
- [ ] Cleanup docs: migration guide + cookbook

---

## Appendix — Example Golden Test Expectations

- `golden/entry/with_trailing.txt` → expected `OPEN`
- `golden/mgmt/be_no_quote.txt` → expected `MGMT_BREAK_EVEN` **only if** `reply_to` present; else `ACK_ERROR`
- `golden/unparsed/chatty.txt` → expected `UNPARSED`
