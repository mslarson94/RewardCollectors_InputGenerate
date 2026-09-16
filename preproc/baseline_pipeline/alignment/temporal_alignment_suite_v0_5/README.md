# Temporal Alignment Suite v0.1

A modular pipeline for Magic Leap ↔ Raspberry Pi temporal alignment.

## Scientific separation

1. **Master inventory** — establishes canonical ML/RPi mark identities and corrected timestamps.
2. **Mark matching** — toggles between manual and automatic correspondence.
3. **Drift characterization** — separates reference offset from clock-rate drift/skew; does not alter timestamps.
4. **Global affine alignment** — cluster-blind single affine model.
5. **Blind piecewise affine alignment** — cluster-blind data-driven piecewise model.
6. **Cluster-aware affine alignment** — uses reviewed chunk start/end clusters.
7. **Comparison** — compares standardized residual outputs.

`raw_offset_s` always means:

    rpi_time - corrected_ml_time

It is deliberately **not** called "drift".

## Install / run

Run from the directory containing this README:

```bash
export PYTHONPATH="$PWD"
```

Dependencies:

```bash
pip install numpy pandas matplotlib
```

## 1. Build master inventory

The corrected ML marks file should minimally contain:

- `mark_id`
- `corrected_ml_time`

Recommended:

- `ordinal`
- `source_row_index`
- `block`
- `exclude`
- `reason`

The RPi marks file should minimally contain:

- `mark_id`
- `mark_time`

Recommended:

- `ordinal`
- `RPi_Timestamp_Source`
- `exclude`
- `reason`

Example:

```bash
python -m temporal_alignment.master_marks.build_master_inventory \
  --corrected-ml-marks corrected_ml_marks.csv \
  --rpi-marks rpi_marks.csv \
  --session-id ObsReward_A_02_17_2025_15_11 \
  --label BioPac \
  --finalized-clusters finalized_clusters.csv \
  --finalized-chunks finalized_chunks.csv \
  --out-dir work/master
```

Until the clustering review is redone, omit `--finalized-clusters` and `--finalized-chunks`.

If your source column names differ, use the CLI `--*-col` arguments.

## 2. Match marks

### Manual

Manual match files use the existing schema:

- `events_mark_id`
- `rpi_mark_id`
- optional `exclude`, `reason`

```bash
python -m temporal_alignment.mark_matching.build_matched_marks \
  --master-dir work/master \
  --mode manual \
  --manual-matches ObsReward_A_..._mark_matches.csv \
  --out-dir work/matches_manual
```

### Automatic

Ordered, unique, nearest-forward matching:

```bash
python -m temporal_alignment.mark_matching.build_matched_marks \
  --master-dir work/master \
  --mode automatic \
  --max-match-gap-s 1.0 \
  --out-dir work/matches_auto
```

Compare matching choices:

```bash
python -m temporal_alignment.mark_matching.compare_matchings \
  --manual work/matches_manual/matched_marks_manual.csv \
  --automatic work/matches_auto/matched_marks_automatic.csv \
  --out work/matching_comparison.csv
```

## 3. Characterize offset + drift

```bash
python -m temporal_alignment.drift.calculate_clock_drift \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --out-dir work/drift_manual
```

Reported separately:

- reference offset
- affine slope
- clock skew in ppm
- drift seconds/hour
- drift milliseconds/minute
- residual statistics

## 4. Global affine alignment

```bash
python -m temporal_alignment.align_global.fit_global_affine \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --out-dir work/global_manual
```

## 5. Blind piecewise affine alignment

```bash
python -m temporal_alignment.align_blind_piecewise.fit_piecewise_affine \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --min-points 4 \
  --penalty 0.0025 \
  --out-dir work/piecewise_manual
```

The penalty is in squared seconds and should later be tuned/validated rather than treated as fixed scientific truth.

## 6. Cluster-aware affine alignment

After corrected-timestamp cluster review is complete:

```bash
python -m temporal_alignment.align_cluster_aware.fit_cluster_affine \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --out-dir work/cluster_manual
```

For each reviewed complete chunk, the current v0.1 estimator:

- takes the median `raw_offset_s` across usable start-cluster marks;
- takes the median across usable end-cluster marks;
- linearly interpolates offset between those boundary estimates.

## 7. Compare alignment methods

```bash
python -m temporal_alignment.comparison.compare_alignments \
  --alignment work/global_manual/alignment_global_affine.csv \
  --alignment work/piecewise_manual/alignment_blind_piecewise.csv \
  --alignment work/cluster_manual/alignment_cluster_aware.csv \
  --out-dir work/comparison_manual
```

Repeat all three alignment methods for automatic matching to produce the full 2 × 3 experiment:

| Matching | Global | Blind piecewise | Cluster-aware |
|---|---|---|---|
| Manual | ✓ | ✓ | ✓ |
| Automatic | ✓ | ✓ | ✓ |

## Important v0.1 limitation

The comparison script currently compares in-sample residuals. The next implementation step is a dedicated held-out / cross-validation module so that the more flexible models are evaluated fairly. That should be implemented before drawing a final conclusion about which alignment method is superior.


## v0.2 additions

### Normalize corrected ML mark exports

If your corrected timestamp generator does not already emit the canonical schema, normalize it first:

```bash
python -m temporal_alignment.master_marks.prepare_corrected_ml_marks \
  --input corrected_source.csv \
  --time-col corrected_timestamp \
  --ordinal-col ordinal \
  --source-row-col source_row_index \
  --out corrected_ml_marks.csv
```

If no mark ID column is supplied, IDs are generated as `events_0000`, `events_0001`, ... from ordinal order.

### Held-out validation

Global affine leave-one-mark-out:

```bash
python -m temporal_alignment.comparison.cross_validate_alignments \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --model global \
  --scheme loocv \
  --out-dir work/cv_global_manual
```

Global affine blocked k-fold:

```bash
python -m temporal_alignment.comparison.cross_validate_alignments \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --model global \
  --scheme blocked-kfold \
  --k 5 \
  --out-dir work/cv_global5_manual
```

Cluster-aware leave-one-boundary-mark-out:

```bash
python -m temporal_alignment.comparison.cross_validate_alignments \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --model cluster-aware \
  --scheme loocv \
  --out-dir work/cv_cluster_manual
```

The cluster-aware CV intentionally holds out one start/end boundary mark, rebuilds the boundary estimate without it, and predicts that mark.

Compare held-out outputs:

```bash
python -m temporal_alignment.comparison.compare_cross_validation \
  --cv work/cv_global_manual/cv_global_loocv.csv \
  --cv work/cv_cluster_manual/cv_cluster-aware_loocv.csv \
  --out-dir work/cv_comparison_manual
```

### Still to add

The blind piecewise model needs a stricter nested validation procedure because segmentation itself is learned from the timing data. A fair held-out test must learn breakpoints only from the training partition, then predict held-out observations without peeking at them.


## v0.3 — Complete alignment model suites

Each alignment method now has three distinct stages:

1. `fit_model.py` — fit only on matched synchronization marks and save a reusable JSON model.
2. `apply_model.py` — apply that model to every event timestamp in a full event CSV.
3. `summarize_model.py` — generate parameter tables and diagnostic plots.

This keeps model fitting separate from transformation of the experimental event stream.

### Global affine

```bash
python -m temporal_alignment.align_global.fit_model \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --out-dir work/global/fit

python -m temporal_alignment.align_global.apply_model \
  --events-csv corrected_events.csv \
  --model-json work/global/fit/global_affine_model.json \
  --time-col corrected_ml_time \
  --out work/global/events_aligned.csv

python -m temporal_alignment.align_global.summarize_model \
  --model-json work/global/fit/global_affine_model.json \
  --diagnostics-csv work/global/fit/global_affine_mark_diagnostics.csv \
  --out-dir work/global/summary
```

### Blind piecewise affine

```bash
python -m temporal_alignment.align_blind_piecewise.fit_model \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --min-points 4 \
  --penalty 0.0025 \
  --out-dir work/piecewise/fit
```

Application uses midpoint boundaries between adjacent fitted timing regimes. Events before the first or after the last fitted mark use the first/last segment respectively.

### Cluster-aware affine

```bash
python -m temporal_alignment.align_cluster_aware.fit_model \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --out-dir work/cluster/fit
```

The model stores one chunk transformation per complete reviewed chunk. Each chunk transformation is defined by the median start-cluster raw offset and median end-cluster raw offset and linearly interpolates between them.

When applying to full event data, `apply_model.py` can use an explicit chunk column if the events file has one; otherwise it chooses chunks from corrected ML time.

### One-command model runner

For convenience:

```bash
python -m temporal_alignment.run_alignment_model \
  --model global \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --events-csv corrected_events.csv \
  --time-col corrected_ml_time \
  --out-dir work/global_manual
```

Valid model names:

- `global`
- `piecewise`
- `cluster-aware`

The same runner can therefore be called six times for the 2 matching modes × 3 alignment methods.


## v0.4 — Dedicated drift characterization suite

The drift suite is diagnostic only. It does **not** transform timestamps.

It distinguishes:

- `raw_offset_s = RPi - corrected ML`
- reference clock offset
- linear clock-rate drift/skew
- residual timing error around the fitted clock model

The fitted model is:

```text
RPi_time = affine_slope * ML_time + intercept
```

Equivalently, the offset evolves approximately as:

```text
offset(t) = reference_offset + drift_rate * elapsed_time
```

Reported units include:

- seconds of reference offset
- clock skew in ppm
- seconds/hour of drift
- milliseconds/minute of drift
- residual MAD/RMSE/95th percentile/max error

### Run complete drift suite

```bash
python -m temporal_alignment.run_drift_suite \
  --matched-marks work/matches_manual/matched_marks_manual.csv \
  --by both \
  --out-dir work/drift_manual
```

`--by` can be:

- `session`
- `chunk`
- `both`

Per-chunk analysis requires a `chunk_id` column in the matched-mark table.

Outputs:

```text
drift_manual/
├── calculation/
│   ├── drift_summary.csv
│   ├── drift_points.csv
│   └── drift_run_summary.json
├── plots/
│   ├── session_offset_drift.png
│   ├── session_residuals.png
│   └── ... per chunk
└── drift_report.csv
```

This suite should generally be run separately for manual and automatic matching so that matching uncertainty is visible in the drift estimates.


## v0.5 — Comparison and validation suite

The comparison layer is separate from model fitting. It accepts standardized residual outputs from any alignment model and ranks models only within the same evaluation type.

Two evaluation categories are kept distinct:

- `in_sample`: fit diagnostics; useful for describing how well a model fits the marks used to construct it.
- `held_out`: predictions for marks not used to fit that prediction; preferred for scientific model selection.

The suite also accepts the manual-vs-automatic mark-matching comparison, allowing matching method and alignment model to be treated as separate factors.

### Full comparison run

```bash
python -m temporal_alignment.run_comparison_suite \
  --insample work/global_manual/fit/global_affine_mark_diagnostics.csv \
  --insample work/piecewise_manual/fit/blind_piecewise_mark_diagnostics.csv \
  --insample work/cluster_manual/fit/cluster_aware_mark_diagnostics.csv \
  --insample work/global_auto/fit/global_affine_mark_diagnostics.csv \
  --insample work/piecewise_auto/fit/blind_piecewise_mark_diagnostics.csv \
  --insample work/cluster_auto/fit/cluster_aware_mark_diagnostics.csv \
  --matching-comparison work/matching_comparison.csv \
  --out-dir work/comparison
```

When held-out results are available, add repeated `--heldout` arguments.

### Metrics

For each matching-mode × alignment-model condition:

- median absolute residual
- residual MAD
- RMSE
- 95th percentile absolute residual
- maximum absolute residual
- residual slope over elapsed time
- rank by median absolute residual
- rank by RMSE

The suite deliberately does not combine in-sample and held-out ranks.

### Outputs

```text
comparison/
├── tables/
│   ├── model_validation_summary.csv
│   ├── all_validation_points.csv
│   ├── matching_by_model_matrix.csv
│   ├── matching_agreement_summary.json
│   └── validation_run_summary.json
├── plots/
│   ├── in_sample_median_abs_residual.png
│   ├── in_sample_rmse.png
│   └── in_sample_residuals_over_time.png
└── summary/
    ├── validation_leaderboard.csv
    └── best_models.json
```

Once held-out validation exists for all three models, the `held_out` leaderboard should be the primary basis for choosing an alignment strategy.
