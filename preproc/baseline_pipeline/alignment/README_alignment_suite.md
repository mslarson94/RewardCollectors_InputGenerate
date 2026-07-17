# Magic Leap ↔ Raspberry Pi Alignment Suite

## What this suite is for

This codebase prepares Raspberry Pi log-derived mark timestamps and aligns them to Magic Leap event timestamps, primarily to compare clocks, estimate drift, and attach Raspberry Pi mark provenance to Magic Leap event rows.

In practice, the suite does **event-level alignment**:
- it extracts or summarizes **RPi marks**
- it selects **Magic Leap `Mark` events**
- it matches ML marks to RPi marks by timestamp, with a clock offset and a nearest-match rule

It does **not** appear to perform a global resynchronization or warping of all Magic Leap samples. It is mainly matching ML event rows to identified RPi mark events.

---

## Short answer to the key question

### Was there any actual alignment of Magic Leap data?

**Yes, but only at the event level.**

The main alignment script, `merge_ml_with_rpi_marks3.py`, does the following:

1. loads the Magic Leap CSV
2. filters it to `lo_eventType == Mark` (or configured event values)
3. loads the precomputed unified RPi marks file
4. estimates or applies a time offset between RPi and ML clocks
5. performs a greedy one-to-one nearest timestamp match between ML marks and RPi marks
6. writes matched timestamps, drift, and provenance back into the output CSV
7. optionally creates synthetic rows for unmatched RPi marks

### What it does **not** do

It does **not** look like:
- continuous timebase correction for all ML rows
- interpolation / resampling of ML data
- affine or nonlinear clock warping across the full ML stream
- sensor-level synchronization beyond mark matching

So the suite is best described as:

> **“RPi mark extraction + ML mark-to-RPi mark alignment”**

rather than

> **“full Magic Leap timeline realignment.”**

---

## Likely main workflow

### Stage 0: optional inventory / raw-log prep
- `list_rpi_files.py`
- `read_rpi_logs_to_csv.py`

### Stage 1: preprocess RPi files into unified mark tables
- `rpi_preproc_pipeline3.py`
  - `extract_rpi_marks5.py`
  - `translate_verb_log.py`
  - `summarize_verb_marks.py`
  - `verb_to_rpi_marks.py`
  - `unify_rpi_marks.py`

### Stage 2: align ML marks to RPi marks
- `batch_split_pipeline3.py`
  - `merge_ml_with_rpi_marks3.py`
  - `summarize_drift2.py`
  - `merge_rpi_event_files2.py`

### Stage 3: inspect / QC / plotting
- `multi_stream_drift.py`
- `marks_elapsed_compare.py`
- `marks_elapsed_timeline.py`
- `marks_timeline_lfp.py`
- `marks_timeline_mlts.py`
- `visualMarks.py`

---

## Recommended “current” scripts

These are the scripts that look most like the active/current pipeline.

### `rpi_preproc_pipeline3.py`
Batch preprocessing driver for Raspberry Pi mark extraction.

What it appears to do:
- reads a collated spreadsheet / session table
- locates Raspberry Pi files per session
- runs the simple-mark extraction path
- runs the verbose-log parsing path
- unifies both into one final RPi marks file

Outputs are organized into:
- `RPi_simple`
- `RPi_verb_full`
- `RPi_verb`
- `RPi_unified`

Use this as the **RPi preprocessing entry point**.

---

### `extract_rpi_marks5.py`
Extracts **simple** Raspberry Pi marks from pre-parsed log CSVs.

What it does:
- reads one or more RPi CSV files
- reconstructs full datetimes from date + time-of-day
- handles day rollover when needed
- can dedupe nearby marks
- writes a target-IP marks CSV and an all-IP CSV

Use this when the source is the simpler parsed CSV log format.

---

### `translate_verb_log.py`
Parses verbose Raspberry Pi logs into structured rows.

What it extracts:
- `ipAddress`
- `markNumber`
- `ML_Time`
- `RPi_Time`
- monotonic timestamps and adjusted monotonic timestamps

This is the first step of the **verbose-log branch**.

---

### `summarize_verb_marks.py`
Reduces verbose parsed rows to one row per mark.

What it does:
- groups rows by IP and mark number
- keeps the final event as the main row
- preserves early rows as backup/reference fields
- adds counts / QC fields

This is the second step of the verbose branch.

---

### `verb_to_rpi_marks.py`
Converts summarized verbose output into the same schema used by simple RPi mark files.

What it creates:
- normalized RPi timestamp fields
- device / source / mark metadata

This is the third step of the verbose branch.

---

### `unify_rpi_marks.py`
Merges simple-derived and verbose-derived RPi mark files.

What it does:
- outer-joins the two tables
- retains both source timestamps where available
- chooses / records the resolved source timestamp
- writes a unified marks file for later ML matching

This is the endpoint of the RPi preprocessing branch.

---

### `merge_ml_with_rpi_marks3.py`
Main ML ↔ RPi event alignment script.

What it does:
- loads a Magic Leap CSV
- filters to Magic Leap mark events
- loads the unified RPi marks CSV
- resolves which RPi timestamp to use per row
- estimates or applies a timezone/clock offset
- performs nearest, order-preserving one-to-one matching
- writes match status, drift, and provenance back into the output
- can synthesize placeholder ML rows for unmatched RPi marks

Important interpretation:
- this is **actual alignment of ML event rows**
- this is **not full-stream ML timeline correction**
- alignment is based on **matching timestamps of identified marks**

---

### `merge_rpi_event_files2.py`
Combines multiple aligned event outputs into one file.

Typical use:
- merge BioPac-aligned output
- merge RNS-aligned output
- produce one combined event file

---

### `batch_split_pipeline3.py`
Batch driver for the alignment half of the workflow.

What it appears to do:
- runs `merge_ml_with_rpi_marks3.py` for each relevant source
- runs drift summarization
- merges outputs
- writes per-session alignment products

Use this as the **post-preprocessing batch alignment entry point**.

---

### `batchAlignHelpers.py`
Shared helper functions used by multiple scripts.

Includes helpers for:
- selecting ML mark rows
- estimating clock offset
- nearest unique matching
- naming outputs
- timestamp cleanup
- drift summary utilities

Not a standalone pipeline step; this is shared infrastructure.

---

## QC / diagnostic scripts

### `multi_stream_drift.py`
Compares timing drift across multiple streams such as ML, RPi, and LFP.

Use when you want:
- pairwise drift calculations
- multi-stream comparison
- drift summaries across sources

---

### `summarize_drift2.py`
Creates drift summaries and plots from aligned event files.

Use when you want:
- alignment QC
- summary CSVs
- drift visualizations

Note:
- the version in this folder may be mid-edit or partially broken

---

### `marks_elapsed_compare.py`
Compares elapsed-time spacing between marks in two streams.

Use when you want:
- to ignore absolute clock offset
- to check whether mark spacing patterns match

---

### `marks_elapsed_timeline.py`
Plots mark timelines on an elapsed-seconds axis.

Use for:
- simple visual timing comparison without absolute-clock dependence

---

### `marks_timeline_lfp.py`
Plots LFP marks and RPi marks on one absolute datetime axis.

Use for:
- direct LFP ↔ RPi visual overlay

---

### `marks_timeline_mlts.py`
Plots RPi marks against ML timestamps on one absolute datetime axis.

Use for:
- direct ML ↔ RPi visual inspection

---

### `visualMarks.py`
Older visual inspection script.

It appears to:
- load ML mark rows
- extract RPi-style timestamps from logs
- plot timelines quickly
- annotate some blocks / rounds

Use as a manual exploratory tool, not as the main pipeline.

---

## Older / likely legacy / special-case scripts

### `alignML2Phsyio.py`
Looks like an older standalone version of ML ↔ RPi alignment.

What it does:
- filters ML to mark rows
- extracts timestamps from raw log lines
- estimates offset
- performs nearest order-preserving matching
- exports drift

Why it looks older:
- overlaps heavily with `merge_ml_with_rpi_marks3.py`
- works more directly from raw logs
- naming and structure suggest an earlier iteration

Likely status:
- **legacy / prototype / superseded**

---

### `alignPO2AN.py`
Appears to be a separate alignment utility for a different pairing (`PO` to `AN`), not the core ML ↔ RPi workflow.

Likely status:
- **special-purpose side script**

---

### `read_rpi_logs_to_csv.py`
Converts raw Raspberry Pi log files into CSV.

Use when:
- the simple logs still need parsing before mark extraction

This is more of an upstream prep tool than part of the core alignment step.

---

### `list_rpi_files.py`
Inventories available RPi files into a CSV table.

Use when:
- you need a file manifest
- you are auditing what exists in a session tree

---

## What seems duplicated or versioned

These filenames suggest iterative replacement:
- `extract_rpi_marks5.py`
- `merge_ml_with_rpi_marks3.py`
- `merge_rpi_event_files2.py`
- `summarize_drift2.py`
- `batch_split_pipeline3.py`
- `rpi_preproc_pipeline3.py`

These are probably the newest kept versions in this folder.

Likely older or auxiliary:
- `alignML2Phsyio.py`
- `visualMarks.py`
- `read_rpi_logs_to_csv.py`
- `list_rpi_files.py`

---

## Practical interpretation of the suite

The core logic seems to be:

1. identify Raspberry Pi marks from one or more log formats
2. normalize them into one RPi mark table
3. select Magic Leap mark events
4. estimate clock offset
5. match ML marks to RPi marks
6. summarize drift and inspect quality

That means your workflow was likely focused on:
- verifying whether ML and RPi event markers correspond
- quantifying clock offset and drift
- producing aligned event tables for later downstream use

It does **not** look like you had a script here that globally “corrected” every ML timestamp in the raw stream.

---

## Keep / archive suggestion

### Keep at the top level
- `rpi_preproc_pipeline3.py`
- `extract_rpi_marks5.py`
- `translate_verb_log.py`
- `summarize_verb_marks.py`
- `verb_to_rpi_marks.py`
- `unify_rpi_marks.py`
- `merge_ml_with_rpi_marks3.py`
- `merge_rpi_event_files2.py`
- `batch_split_pipeline3.py`
- `batchAlignHelpers.py`

### Keep as QC utilities
- `multi_stream_drift.py`
- `summarize_drift2.py`
- `marks_elapsed_compare.py`
- `marks_elapsed_timeline.py`
- `marks_timeline_lfp.py`
- `marks_timeline_mlts.py`

### Consider archiving into `legacy/` or `exploratory/`
- `alignML2Phsyio.py`
- `visualMarks.py`
- `alignPO2AN.py`
- `read_rpi_logs_to_csv.py`
- `list_rpi_files.py`

---

## One-line memory refresh

- `list_rpi_files.py` — inventories available RPi log files
- `read_rpi_logs_to_csv.py` — converts raw RPi logs into CSV
- `extract_rpi_marks5.py` — extracts simple RPi marks from parsed CSV logs
- `translate_verb_log.py` — parses verbose logs into structured rows
- `summarize_verb_marks.py` — collapses verbose rows to one row per mark
- `verb_to_rpi_marks.py` — converts verbose summaries into RPi-mark schema
- `unify_rpi_marks.py` — merges simple and verbose marks into one unified table
- `rpi_preproc_pipeline3.py` — batch-runs the RPi preprocessing branch
- `merge_ml_with_rpi_marks3.py` — matches ML mark events to RPi marks
- `merge_rpi_event_files2.py` — merges aligned event outputs across sources
- `batch_split_pipeline3.py` — batch-runs the ML ↔ RPi alignment branch
- `batchAlignHelpers.py` — shared matching / timestamp / drift helpers
- `summarize_drift2.py` — drift summary and QC plotting
- `multi_stream_drift.py` — multi-stream drift comparison
- `marks_elapsed_compare.py` — compares mark spacing after zeroing starts
- `marks_elapsed_timeline.py` — plots elapsed-time mark timelines
- `marks_timeline_lfp.py` — overlays LFP and RPi on absolute time
- `marks_timeline_mlts.py` — overlays ML and RPi on absolute time
- `visualMarks.py` — older quick visual mark inspector
- `alignML2Phsyio.py` — older direct ML-mark ↔ raw-log alignment script
- `alignPO2AN.py` — separate PO ↔ AN alignment utility
