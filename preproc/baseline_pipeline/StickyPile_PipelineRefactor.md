```md
# RewardCollectors Pipeline Notes (Jan 2026)

These notes capture the current “working truth” of the pipeline while we stabilize reprocessing and defer full timestamp unification.

---

## Goals and constraints

- **Primary behavioral time base:** `AppTime` (monotonic, session-internal).  
  Used for ordering, segmentation logic, elapsed-time, speed, distance, and interval tables.

- **Wall-clock-like ML time base:** `mLTimestamp` (AN) / `mLTimestamp_orig` (PO).  
  Used for event timestamp fields (especially synthetic event anchors) and later alignment tasks.

- **Enhanced ML timestamp:** `eMLTimestamp` / `eMLTimestamp_orig` exists in `_processed.csv` outputs, but the current event ecosystem does **not** reliably use it for synthetic event timestamps. Retrofitting eMLT start/end for synthetic events is deferred.

- **RPi alignment:** required for sessions that have RPi data; optional for others.  
  Alignment requires visual/manual QA via plotting scripts. RPi-aligned timestamps are added **after** segmentation and reprocessing as augmentation columns.

---

## Current pipeline posture

### 1) Preprocessing (`preprocRaw_*`)
Produces `_processed.csv` with stable columns:
- `origRow` (row identity for mapping events ↔ processed)
- `AppTime` (monotonic)
- ML timestamp columns (role-dependent):
  - AN: `mLTimestamp`, `eMLTimestamp`, plus raw/orig variants if present
  - PO: `mLTimestamp_orig`, `eMLTimestamp_orig`, plus raw/orig variants if present

**Sanity checks**
- Add warning-only `check_monotonic_apptime()` before saving.

**Logging**
- Send logs into stage-specific subfolders under `PreProcLogging/` (see below).

---

### 2) Event segmentation (`preFrontalCortex_unifiedEventSeg.py`)
Run **full segmentation directly** (glia-only no longer required if AN↔PO alignment is skipped).

**Ordering rule**
- Events are canonicalized by:
  - `start_AppTime`, `end_AppTime`, `origRow_start`
- Remove any additional `sort_values(timestamp_col)` after canonicalization.

**Synthetic events**
- Synthetic events may generate both:
  - `start_AppTime` / `end_AppTime`
  - `start_mLT` / `end_mLT` (or PO equivalents)
- Synthetic start/end ML timestamps are anchored to the ML timestamp domain being used by the event ecosystem (currently mLTimestamp-based), not eMLTimestamp.

**Important:** segmentation should not depend on RPi-aligned columns.

---

### 3) Reprocessing (`*_processed.csv → *_reprocessed.csv`)
We reprocess all sessions to add:
- `stepDist` (moment-to-moment distance; sanity check)
- `totDistRound`, `totDistBlock`
- `roundElapsed`, `blockElapsed`, `totalSessionElapsed`
- `currSpeed` (computed after stepDist is available)

**Sanity checks**
- Run warning-only `check_monotonic_apptime()` again before writing `_reprocessed.csv`.

---

### 4) Interval tables (post-segmentation / post-reproc)
Build:
- `BlockIntervals` and `RoundIntervals` (exclude `RoundNum > 100` in any “true round” analysis)
- merge into preliminary block-round interval table as needed

Augment interval tables with PinDrop-derived summaries by joining on:
- `(BlockNum, BlockInstance, RoundNum)` (or later `(BlockNum, BlockRepeatIndex, RoundNum)` if added)

---

## RPi alignment strategy (deferred to post-segmentation)

### Why alignment is deferred
- Not all sessions have RPi logs.
- Alignment requires visual/manual QA (plotting scripts).
- Segmentation + reprocessing should remain stable regardless of RPi availability.

### What RPi alignment produces
- Unified RPi marks tables:
  - `RPi_preproc/<Label>/RPi_unified/<session>_<Label>_RPi_unified.csv`

### What is *not* guaranteed yet
- A turn-key script that writes fully RPi-aligned `_processed.csv` for every session.

### Policy for timestamps
- RPi-aligned timestamp columns should be **added as new columns** to both:
  - last viable `_events*.csv`
  - `_reprocessed.csv`

Suggested naming:
- `*_RPi` columns are filled when available, else left NA.
- Downstream “timestamp-of-record” selection:
  - Prefer `eMLT_RPi` when populated; if mostly NA, fall back to `eMLT_orig`.

---

## Logging structure (recommended)

Under `PreProcLogging/`, create stage subfolders:
- `preProcRaw_AN/`
- `preProcRaw_PO/`
- `eventSegFull_AN/`
- `eventSegFull_PO/`
- (later) `alignML2RPi/`, `reproc/`, `intervals/`

Each script should write logs into its stage folder to avoid collisions and reduce confusion.

---

## Known TODOs (sticky pile items)

- Add `BlockRepeatIndex` (computed from `BlockStart` events) and propagate through `_events*.csv` for robust block-instance keys.
- Audit `batch_split_pipeline3.py` outputs; confirm whether any existing scripts already write RPi-aligned `_processed.csv`.
- Audit “marks_* / drift / visual” scripts to confirm plotting vs alignment behavior.
- Consolidate helpers into an importable package so alignment/reproc scripts can share `WarningLogger`, canonicalization, safe merges, etc.
- Standardize timestamp suffix naming:
  - `*_orig` / `*_raw` for ML-side
  - `*_RPi` for aligned outputs

---

## Practical decision record

**Decision:** Finish segmentation + reprocessing using `AppTime` and existing ML timestamp anchors (mLTimestamp-based).  
**Deferral:** Retroactively computing `eMLTimestamp` start/end for synthetic events is deferred due to complexity (no 1:1 origRow mapping for synthetic intervals).  
**Alignment:** RPi alignment is performed as a downstream augmentation step on “last viable” events + reprocessed time-series.

---
```
