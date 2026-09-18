# Alignment Suite Keep / Archive / Inspect Table

## How to use this table

- **Keep** = likely part of the current main workflow
- **Inspect** = useful, but probably manual, side-path, or uncertain
- **Archive** = likely older/superseded if you are cleaning the folder

---

| Script | Status | Why | Notes |
|---|---|---|---|
| `rpi_preproc_pipeline3.py` | **Keep** | Main batch preprocessor for RPi marks | Core orchestrator for simple + verbose → unified marks |
| `extract_rpi_marks5.py` | **Keep** | Current simple-mark extractor | Numbered version suggests latest kept iteration |
| `translate_verb_log.py` | **Keep** | Current verbose-log parser | Part of the active preprocess branch |
| `summarize_verb_marks.py` | **Keep** | Current verbose summarizer | Converts verbose rows to mark-level rows |
| `verb_to_rpi_marks.py` | **Keep** | Normalizes verbose marks into common schema | Feeds `unify_rpi_marks.py` |
| `unify_rpi_marks.py` | **Keep** | Produces final unified RPi marks file | Key handoff into alignment |
| `merge_ml_with_rpi_marks3.py` | **Keep** | Core ML↔RPi mark alignment script | Main event-level matching logic |
| `merge_rpi_event_files2.py` | **Keep** | Combines BioPac and RNS outputs | Downstream consolidation |
| `batch_split_pipeline3.py` | **Keep** | Main batch wrapper for alignment stage | Strongest sign your workflow was batch-driven |
| `batchAlignHelpers.py` | **Keep** | Shared helper module used across the suite | Required support code |

| `summarize_drift2.py` | **Inspect** | Clearly part of current flow, but uploaded version may be mid-edit | Keep if actively used; test before relying on it |
| `multi_stream_drift.py` | **Inspect** | Valuable QC tool, especially for ML/RPi/LFP comparisons | Looks manual, not batch |
| `marks_elapsed_compare.py` | **Inspect** | Useful for comparing timing patterns | Manual diagnostics |
| `marks_elapsed_timeline.py` | **Inspect** | Useful plot utility | Manual diagnostics |
| `marks_timeline_lfp.py` | **Inspect** | Useful LFP-vs-RPi plotter | Strong sign of manual LFP follow-up work |
| `marks_timeline_mlts.py` | **Inspect** | Useful ML-vs-RPi visualization | Manual diagnostics |
| `read_rpi_logs_to_csv.py` | **Inspect** | Upstream prep helper | May still be needed depending on raw-log format |
| `list_rpi_files.py` | **Inspect** | Discovery / inventory helper | Not core alignment, but useful admin tooling |
| `alignPO2AN.py` | **Inspect** | Separate alignment task, not clearly part of this main pipeline | Keep only if PO↔AN work still matters |

| `alignML2Phsyio.py` | **Archive** | Appears to be an older standalone ML↔RPi aligner | Likely superseded by `merge_ml_with_rpi_marks3.py` |
| `visualMarks.py` | **Archive** | Exploratory visual script with older/manual feel | Probably kept for ad hoc inspection rather than production |

---

## My best “safe cleanup” recommendation

### Keep together as the active suite
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

### Keep in a `qc_tools/` or `manual_checks/` folder
- `summarize_drift2.py`
- `multi_stream_drift.py`
- `marks_elapsed_compare.py`
- `marks_elapsed_timeline.py`
- `marks_timeline_lfp.py`
- `marks_timeline_mlts.py`
- `read_rpi_logs_to_csv.py`
- `list_rpi_files.py`

### Move to `archive/` unless you know you still use them
- `alignML2Phsyio.py`
- `visualMarks.py`

### Keep separate if still relevant to another project
- `alignPO2AN.py`

---

## Best answer to “was this batch or manual?”

### ML↔RPi
**Mostly batch.**
The strongest evidence is:
- `rpi_preproc_pipeline3.py`
- `batch_split_pipeline3.py`

Both are spreadsheet-driven wrappers that iterate over many rows and call downstream scripts automatically.

### LFP-related work
**Likely much more manual.**
The LFP scripts in this folder look like:
- standalone plotters
- standalone drift calculators
- explicit one-off command line tools

I do not see an equivalent batch wrapper for LFP inside this suite.

So your memory is probably right:
- **the main ML↔RPi workflow was batch-style**
- **the LFP participant-pair work was likely more hand-run / case-by-case**
