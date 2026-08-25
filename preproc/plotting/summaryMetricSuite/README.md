# Summary-metric plotting suite

This suite plots arbitrary numeric variables from `participantSummaryData_*` files.

## Files

- `summaryMetricHelpers.py` — input, filtering, validation, and saving helpers
- `summaryMetricPlots.py` — histogram/KDE and violin plots
- `summaryMetricStats.py` — descriptive summary tables
- `summaryMetricWrapper.py` — CLI runner

## Examples

Plot total rounds from one participant summary file:

```bash
python summaryMetricWrapper.py \
  --input participantSummaryData_AN.csv \
  --voi totRounds \
  --voi-str "Total rounds" \
  --voi-unit "(rounds)" \
  --out-dir summary_metric_results
```

Plot total score for main sessions only:

```bash
python summaryMetricWrapper.py \
  --input participantSummaryData_AN.csv \
  --voi totScore \
  --voi-str "Total score" \
  --where main_RR=main \
  --out-dir summary_metric_results
```

Plot swap-vote score and split outputs by `main_RR`:

```bash
python summaryMetricWrapper.py \
  --input participantSummaryData_AN.csv \
  --voi swapVoteScore \
  --voi-str "Swap vote score" \
  --facet-by main_RR \
  --out-dir summary_metric_results
```

Run the same metric over multiple participant-summary files:

```bash
python summaryMetricWrapper.py \
  --input "participantSummaryData_*.csv" \
  --voi rounds2criterion \
  --voi-str "Rounds to criterion" \
  --voi-unit "(rounds)" \
  --out-dir summary_metric_results
```

A future variable requires no suite changes: once the column exists, pass its name to `--voi`.

## Dependencies

```bash
pip install pandas numpy matplotlib seaborn openpyxl pyarrow
```

## Combined facet outputs

With `--facet-by`, the wrapper keeps the existing separate facet plots and also
creates three comparison figures:

- `hist_overlay__...` — all facet histograms overlaid with shared bin edges;
- `hist_panels__...` — facet histograms in shared-axis panels;
- `violin_combined__...` — all facets on one horizontal violin plot.

The histogram panel layout is adaptive. By default, 1-3 facets are arranged
side by side and 4+ facets are stacked vertically. Change the cutoff with:

```bash
--facet-panel-vertical-threshold 4
```

All combined histograms use one bin definition computed from the full filtered
dataset so the facet distributions are directly comparable.


## No-stats plot versions

The suite also saves clean versions of annotated plots with `__nostats` appended
to the filename. These retain titles, axes, legends, colors, facet labels, KDEs,
and participant marks while removing descriptive-statistics and bin-width
annotations.

Examples:

- `histkde__totRounds__main_RR-main__nostats.png`
- `violin__totRounds__main_RR-main__nostats.png`
- `hist_overlay__totRounds__by-main_RR__nostats.png`
- `hist_panels__totRounds__by-main_RR__nostats.png`

The combined violin currently contains no statistics annotation, so only one
version is saved.
