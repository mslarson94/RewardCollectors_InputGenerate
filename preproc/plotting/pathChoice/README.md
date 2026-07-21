# Path-choice faceted plotting suite

## Revised visualization behavior

- Excludes path codes `888` and `999`.
- Uses path codes `1` through `6` as compact x-axis labels on every facet.
- Colors each violin and proportion bar by path choice using a stable `tab10` palette.
- Places the full `path_order_round` strings in one shared figure legend.
- Preserves empty category positions when a participant never selected a path.
- Uses dark jittered points, quartile violin interiors, and a Seaborn `whitegrid` theme.
- Places `participantID` and `totRounds` in each facet title.
- Shows `PVSS_AvgScore`, `swapRate_tot`, `rounds2criterion`, and `totScore` in a compact annotation.
- Exports `path_choice_code_lookup.csv`.

## Outputs

- `path_choice_faceted_violins.*`
- `path_choice_faceted_proportions.*`
- `path_choice_code_lookup.csv`
- `path_choice_round_level_merged.csv`
- `path_choice_counts_proportions.csv`
- `path_choice_participant_summary.csv`
- `manifest.json`

## Run with a CSV summary

```bash
python pathChoiceWrapper.py \
  --input AN_PinDropsKnottedFiltered_noABCD_all.csv \
  --summary participantSummaryData_AN.csv \
  --out-dir path_choice_results \
  --formats png,pdf \
  --ncols 3
```

## Size controls

```bash
python pathChoiceWrapper.py \
  --input AN_PinDropsKnottedFiltered_noABCD_all.csv \
  --summary participantSummaryData_AN.csv \
  --out-dir path_choice_results \
  --panel-width 5.1 \
  --panel-height 4.2 \
  --annotation-fontsize 7.2
```

## Dependencies

```bash
pip install pandas matplotlib seaborn openpyxl xlrd
```


The proportion facets use the same participant/session title and compact PVSS, swap rate, rounds-to-criterion, and total-score annotation as the violin facets.
