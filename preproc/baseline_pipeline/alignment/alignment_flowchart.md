# Alignment Suite Flowchart

## Executive summary

The codebase looks organized around a **batch ML↔RPi pipeline** driven by a collated spreadsheet, plus a separate set of **manual QC / LFP comparison utilities**.

- **Batch side:** preprocess Raspberry Pi logs into unified marks, then align ML mark events to those RPi marks.
- **Manual side:** inspect drift, plot timelines, and compare RPi against LFP with one-off script calls.

---

## High-level pipeline

```text
Raw RPi logs / inventories
    │
    ├─ list_rpi_files.py
    │    -> CSV inventory of available RPi files
    │
    ├─ read_rpi_logs_to_csv.py
    │    -> per-log CSVs derived from raw .log files
    │
    ▼
collatedData.xlsx + ML cleaned event files + RPi CSV/log paths
    │
    └─ rpi_preproc_pipeline3.py   [BATCH]
         │
         ├─ extract_rpi_marks5.py
         │    Input: per-log RPi CSVs
         │    Output: simple RPi marks CSV
         │
         ├─ translate_verb_log.py
         │    Input: concatenated *_verb.log text
         │    Output: structured verbose rows CSV
         │
         ├─ summarize_verb_marks.py
         │    Input: verbose rows CSV
         │    Output: one-row-per-mark verbose summary CSV
         │
         ├─ verb_to_rpi_marks.py
         │    Input: verbose summary CSV
         │    Output: verbose marks in RPi-mark schema
         │
         └─ unify_rpi_marks.py
              Input: simple marks + verbose marks
              Output: unified RPi marks CSV
```

```text
Unified RPi marks + ML cleaned event CSVs + collatedData.xlsx
    │
    └─ batch_split_pipeline3.py   [BATCH]
         │
         ├─ merge_ml_with_rpi_marks3.py
         │    Input: ML events + unified RPi marks
         │    Output: aligned ML↔RPi event CSV (per source)
         │
         ├─ summarize_drift2.py
         │    Input: aligned event CSV
         │    Output: drift plots + summary CSVs
         │
         └─ merge_rpi_event_files2.py
              Input: BioPac-aligned CSV + RNS-aligned CSV
              Output: combined BioPac/RNS event CSV
```

---

## What “alignment” means here

```text
ML cleaned events
    │
    ├─ filter to ML rows whose Events == "Mark"
    │
    ├─ load unified RPi marks
    │
    ├─ resolve which RPi timestamp column to trust
    │
    ├─ estimate/apply timezone offset for simple RPi timestamps
    │
    └─ nearest, order-preserving one-to-one matching
         between ML mark times and RPi mark times
              │
              └─ output drift / provenance columns on matched rows
```

This means the suite performs **event-level alignment of ML mark events to detected RPi mark events**.

It does **not** appear to:
- retime every ML sample in a continuous stream
- warp the entire ML timeline
- resample raw ML sensor data

---

## Input → output map by script

### 1) Inventory / prep

#### `list_rpi_files.py`
- **Input:** raw RPi directory tree
- **Output:** CSV inventory of discovered files
- **Role:** discovery / auditing

#### `read_rpi_logs_to_csv.py`
- **Input:** raw simple RPi `.log` files
- **Output:** per-log CSV files
- **Role:** upstream conversion into a format later scripts can parse more easily

---

### 2) Batch RPi preprocessing

#### `rpi_preproc_pipeline3.py`
- **Input:** `collatedData.xlsx`, ML file references, RPi simple files, verbose logs
- **Output:** per-session `RPi_preproc` folders containing simple, verbose, and unified marks
- **Role:** batch orchestrator for RPi-side preprocessing

#### `extract_rpi_marks5.py`
- **Input:** one or more RPi CSVs
- **Output:** simple RPi marks CSV(s)
- **Role:** extract identifiable mark timestamps from simple logs

#### `translate_verb_log.py`
- **Input:** verbose log text
- **Output:** row-wise verbose event table
- **Role:** parse verbose logs into structured rows

#### `summarize_verb_marks.py`
- **Input:** verbose event table
- **Output:** one row per `(ipAddress, markNumber)`
- **Role:** compress verbose micro-events into mark-level rows

#### `verb_to_rpi_marks.py`
- **Input:** summarized verbose marks
- **Output:** verbose marks in common RPi-mark schema
- **Role:** normalize verbose-derived marks

#### `unify_rpi_marks.py`
- **Input:** simple marks + verbose marks
- **Output:** unified RPi marks CSV
- **Role:** merge both detection routes into one final RPi mark table

---

### 3) Batch ML↔RPi alignment

#### `batch_split_pipeline3.py`
- **Input:** `collatedData.xlsx`, ML cleaned files, unified RPi marks
- **Output:** aligned per-source files, drift summaries, optional combined merged files
- **Role:** batch orchestrator for ML↔RPi alignment

#### `merge_ml_with_rpi_marks3.py`
- **Input:** ML cleaned event CSV + unified RPi marks CSV
- **Output:** aligned event CSV with matched RPi times and drift columns
- **Role:** core mark-matching / alignment step

#### `summarize_drift2.py`
- **Input:** aligned ML↔RPi file
- **Output:** drift plots and summary CSVs
- **Role:** post-alignment QC

#### `merge_rpi_event_files2.py`
- **Input:** BioPac aligned file + RNS aligned file
- **Output:** combined event file
- **Role:** combine both RPi-source-specific outputs

---

### 4) Manual QC / diagnostics

#### `multi_stream_drift.py`
- **Input:** ML / RPi / LFP CSVs
- **Output:** pairwise drift table + optional summaries/plots
- **Role:** cross-stream diagnostics

#### `marks_elapsed_compare.py`
- **Input:** LFP CSV + RPi CSV
- **Output:** elapsed-time comparison plot
- **Role:** compare mark spacing independent of absolute offset

#### `marks_elapsed_timeline.py`
- **Input:** LFP and/or RPi timestamps
- **Output:** elapsed-seconds timeline plot
- **Role:** quick temporal pattern visualization

#### `marks_timeline_lfp.py`
- **Input:** LFP CSV + RPi marks CSV
- **Output:** absolute-time timeline plot
- **Role:** visual QC for LFP vs RPi

#### `marks_timeline_mlts.py`
- **Input:** ML event CSV + RPi marks CSV
- **Output:** absolute-time timeline plot
- **Role:** visual QC for ML vs RPi

#### `visualMarks.py`
- **Input:** ML events + raw logs
- **Output:** exploratory plot(s)
- **Role:** older/manual visual inspection

---

### 5) Older or side-path scripts

#### `alignML2Phsyio.py`
- **Input:** ML marks + raw RPi log timestamps
- **Output:** older aligned/drift output
- **Role:** likely an earlier standalone aligner superseded by `merge_ml_with_rpi_marks3.py`

#### `alignPO2AN.py`
- **Input:** PO and AN files
- **Output:** aligned PO↔AN outputs
- **Role:** separate special-purpose alignment utility, not obviously part of the main ML↔RPi flow

---

## Batch vs manual: best interpretation from the code

### Looks batch-driven
The following are clearly written as batch runners:
- `rpi_preproc_pipeline3.py`
- `batch_split_pipeline3.py`

Why:
- both read a **collated Excel sheet**
- both iterate row-by-row over sessions / participant pairs
- both call downstream scripts via `subprocess`
- both resolve paths using columns like:
  - `cleanedFile`
  - `BioPac_RPi`
  - `RNS_RPi`
  - `pairID_py`
  - `testingDate`
  - `sessionType`
  - `device`

### Looks manual / one-off
The following look like scripts you probably ran ad hoc:
- `multi_stream_drift.py`
- `marks_elapsed_compare.py`
- `marks_elapsed_timeline.py`
- `marks_timeline_lfp.py`
- `marks_timeline_mlts.py`
- `visualMarks.py`

Why:
- they take explicit file paths on the command line
- they do not appear to iterate over the collated workbook
- they are oriented toward plotting and QC
- several are specifically framed around LFP comparison, which is not integrated into the batch wrappers in this folder

### Likely real-world workflow
Most likely:
1. **Batch-run** the ML↔RPi preprocessing/alignment over many sessions.
2. **Manually inspect** difficult cases, especially LFP-related participant pairs, with plotting and drift scripts.

That matches your memory that the overall project was batch-style, while some LFP work was still done by hand.
