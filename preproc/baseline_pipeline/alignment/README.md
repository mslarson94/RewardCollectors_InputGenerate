# Alignment Suite Flowchart

## Executive summary

- **ML↔RPi synchronization is estimated from matched mark events using an affine clock model.**
- **Matched marks define the clock relationship; the fitted model is then applied to the full cleaned ML event timeline.**
- **Raw sensor streams are not resampled or nonlinearly warped.**

---

## Pipeline Overview

``` baseRPiHandling 
baseRPiHandling
    │
    └─ batch_read_rpi_logs_to_csv   [BATCH]
         │ 		General Input: RPi log paths
    		 │ 		General Output: RPi marks csv's
    		 │		Role: Batch conversion of .log files to .csv's
         │
         └─ read_rpi_logs_to_csv.py
         			Input: Raw RPi logs 
         			Output: per-log CSVs derived from raw .log files
         			Role: conversion of .log files to .csv's with which later scripts can parse more easily
```

```rpi_preproc
rpi_preproc
    │
    └─ rpi_preproc_pipeline3.py   [BATCH]
    		 │ 		General Input: collatedData.xlsx + ML cleaned event files + RPi CSV/log paths
    		 │ 		General Output: per-session RPi_preproc folders containing simple, verbose, and unified marks
    		 │		Role: Batch orchestrator for RPi-side preprocessing
         │
         ├─ extract_rpi_marks5.py
         │    Input: per-log RPi CSVs + ML file for whole-hour clock normalization
         │    Output: simple RPi marks CSV
         │		Role: extract identifiable mark timestamps from simple logs
         │
         ├─ translate_verb_log.py
         │    Input: concatenated *_verb.log text
         │    Output: row-wise verbose event table
         │		Role: parse verbose logs into structured rows
         │
         ├─ summarize_verb_marks.py
         │    Input: verbose rows CSV
         │    Output: one-row-per-mark verbose summary CSV by ip address 
         │		Role: compress verbose micro-events into mark-level rows
         │
         ├─ verb_to_rpi_marks.py
         │    Input: verbose summary CSV
         │    Output: verbose marks in RPi-mark schema
         │		Role: normalize verbose-derived marks
         │
         └─ unify_rpi_marks.py
              Input: simple marks + verbose marks
              Output: unified RPi marks CSV
              Role: merge both detection routes into one final RPi mark table
```

```markMatchingSuite
markMatchingSuite
    │
    └─ markMatchingSuite.py   [BATCH]
         │ 		General Input: Unified RPi marks + ML cleaned event CSVs + collatedData.xlsx 
         │									 + manual review/matching files where applicable
         │ 		General Output: Per-source matched-mark tables for automatic, hybrid/manual-exclusion-
         │										assisted, and manual mark matching methods.
         │		Role: Coordinates automatic, hybrid, and manual ML↔RPi mark matching. Freezes mark matches 
         │ 					for downstream use in affine fitting suite.
         │
         ├─ auto_match_ml_rpi_marks.py
         │    Input: ML events + unified RPi marks
         │    Output: automatically aligned ML↔RPi event CSV (per source)
         │		Role: Select ML mark events, match them uniquely and monotonically to RPi marks, preserve 
         │          unmatched marks, and freeze the correspondences for downstream fitting.
         │
         ├─ hybrid_match_ml_rpi_marks.py
         │    Input: ML events + unified RPi marks + *_mark_singles.csv
         │    Output: manual-exclusion-assisted frozen ML↔RPi matched-mark table 
         │    				+ matching summary / exclusion audit
         │		Role: Validate and apply manually selected mark exclusions, then run the same automatic 
         │        	unique, monotonic matching procedure on the retained ML and RPi marks. Preserve excluded
         │        	and unmatched mark provenance and freeze the resulting correspondences for downstream 
         │        	affine fitting.
         │
         └─ manual_match_ml_rpi_marks.py
              Input: ML events + *_mark_singles.csv + *_mark_matches.csv
              Output: manual-exclusion-assisted frozen ML↔RPi matched-mark table 
             				  + matching summary / exclusion audit
         		  Role: Convert manually assigned ML↔RPi mark pairs, unmatched marks, and explicit exclusions 
         		 			  into the same canonical frozen-match format used by the automatic and hybrid methods, 
         		 			  without performing automatic rematching.
              
```

```affineFittingSuite
affineFittingSuite
    │
    └─ affine_fitting_suite.py   [BATCH]
         │ 		General Input: ML cleaned event CSVs + collatedData.xlsx + Per-source matched-mark tables 
         │									 for automatic, hybrid/manual-exclusion-assisted, and manual mark 
         │									 matching methods.
         │ 		General Output: Per-source globally aligned ML event CSVs, affine summaries / diagnostics,
         │										drift/residual plots and summaries, and optional combined BioPac/RNS event CSV
         │		Role: Applies the same global affine fitting and alignment workflow to each frozen 
         │					correspondence method, then summarizes fit quality and optionally merges independently
         │       		aligned BioPac and RNS event outputs.
         │
         ├─ fit_global_affine_from_matches.py
         │    Input: ML cleaned event CSV + frozen matched-mark CSV
         │    Output: globally aligned ML event CSV + global affine mark-diagnostics CSV
         │       		  + global affine summary CSV
         │		Role: Fit the final robust global affine ML→RPi clock model from the frozen correspondences, 
         │       	  apply that model to all ML timestamps, and generate fit/residual diagnostics
         │
         ├─ summarize_alignment3.py
         │    Input: globally aligned ML event CSV
         │    Output: clock-offset plot + alignment-residual plot + compact alignment summary CSV
         │		Role: Summarize observed clock offset, post-affine residual error, inlier behavior,
         │          and fitted clock drift
         │
         └─ merge_rpi_event_files2.py
              Input: BioPac-aligned ML CSV + RNS-aligned ML CSV
              Output: combined BioPac/RNS event CSV
              Role: Combine the independently aligned BioPac and RNS event columns into one session-level 											event file
              
```

``` alignmentQCSuite
alignmentQCSuite
    │
    ├─ alignment_qc_suite.py   [BATCH ORCHESTRATOR]
    │    │
    │    │   General Input: Global-affine summary CSVs + global-affine mark-diagnostics CSVs produced by
    │    │       					  the alignment suite
    │    │   General Output: alignment_qc_master_summary.csv + alignment_qc_category_counts.csv
    │    │       						 + five QC-category manifest CSVs + component QC summaries
    │    │       						 + alignment_qc_stage_report.csv
    │    │   Role: Runs across-session global and local alignment QC, merges the results into one session-=
    │    │       	 level table, and assigns each session to the canonical five-way QC routing category
    │    │
    │    ├─ classify_global_affine_qc.py
    │    │    Input: *_global_affine_summary.csv files
    │    │    Output: global_affine_qc_all_sessions.csv + global-affine QC classification outputs
    │    │    Role: Evaluate overall global-affine fit quality using residual RMSE, residual MAD, inlier 
    │    │        	fraction, and optionally accumulated clock drift / fit span
    │    │
    │    ├─ characterize_burst_shifts.py
    │    │    Input: *_global_affine_mark_diagnostics.csv files
    │    │    Output: burst_shift_session_summary.csv + burst-level shift diagnostics
    │    │    Role: Measure whether stabilized residual centers move from burst to burst, identifying local 
    │    │          temporal structure that may not be visible from global RMSE/MAD alone RMSE/MAD alone
    │    │
    │    └─ characterize_burst_residuals.py
    │         Input: *_global_affine_mark_diagnostics.csv files
    │         Output: burst_residual_session_summary.csv + burst-position residual diagnostics
    │         Role: Characterize residual error by position within each mark burst, especially 
    │         			the difference betweenfirst-in-burst marks and later stabilized marks
    │
    └─ characterize_fit_robustness.py
             Input: *_global_affine_mark_diagnostics.csv files + optional QC-category manifest
                    (typically global_fail_low_local_movement.csv)
             Output: fit_robustness_session_summary.csv + fit_robustness_leave_one_mark.csv
                 		 + fit_robustness_leave_one_burst.csv + fit_robustness_category_counts.csv
                 		 + robustness-category manifest CSVs
             Role: Determine whether a globally failing but locally stable session is better explained by 
                	 measurement noise, influential individual marks, burst-level leverage, first-mark  
                	 effects, or fit-method sensitivity
```

``` markMatchApps 
markMatchApps
    │
    ├─ mark_match_app.py [GUI]
    │    Input: ML cleaned event CSV(s) + unified RPi marks CSV(s) or a manifest describing event/RPi file 
    │       		pairs + optionally previously saved match state
    │    Output: *_mark_matches.csv + *_mark_singles.csv
    │    Role: Interactive manual ML↔RPi mark matching tool. Creates explicit event/RPi pair assignments 
    │        	 while also maintaining a complete single-mark table with ordinals, match IDs, exclusions,
    │          reasons, block assignment, and review metadata
    │
    └─ mark_pair_review_app.py [GUI]
         Input: ML cleaned event CSV(s) + unified RPi marks CSV(s) + *_mark_matches.csv + *_mark_singles.csv
         Output: *_pair_votes.csv
         Role: Second-pass quality review of already-created ML↔RPi mark pairs. Allows each pair to receive 
               a graded vote from hard exclusion through hard acceptance, without recreating the pair
               assignments themselves
```

``` markChunkApps
markChunkApps
    │
    ├─ mark_chunk_review_tool.py   [CLI / CLUSTER BUILDER]
    │    │
    │    ├─ propose mode
    │    │    Input: ML cleaned event CSV containing Mark events and contextual 
    │    │        	 BlockStart/RoundStart/BlockEnd events
    │    │
    │    │    Output: mark_clusters.csv, mark_cluster_context.csv, proposed_chunks.csv
    │    │       		  mark_cluster_review_template.csv, proposal_bundle.json
    │    │
    │    │    Role: Automatically group temporally adjacent Mark events into candidate clusters using a
    │    │        	mark-gap threshold, characterize nearby task/block context, assign soft start/end/pause
    │    │        	labels, and propose adjacent cluster boundaries that may form chunks
    │    │
    │    └─ apply-edits mode
    │         Input: mark_clusters.csv + manually edited review CSV + optional mark_cluster_context.csv
    │         Output: finalized_clusters.csv, finalized_chunks.csv, chunk_events.csv, chunk_anomalies.csv
    │             		finalized_bundle.json
    │         Role: Apply manual split/merge/label/ignore edits to the proposed clusters and convert the 
    │             	reviewed cluster sequence into finalized chunk boundaries/events
    │
    └─ mark_review_tk.py   [GUI]
         Input: mark_clusters.csv + mark_cluster_context.csv + original source event CSV
             		+ optional existing review CSV
         Output: edited cluster-review CSV, containing manual labels, review status, split/merge/ignore
             		 actions, and notes
         Role: Interactive GUI for reviewing and editing proposed mark clusters before final chunk creation
```



---

## What “alignment” means here

```text
RPi preprocessing
    │
    └─ produce RPi marks already placed in the intended wall-clock frame
            │
            ▼
ML cleaned events + preprocessed RPi marks
    │
    ├─ select ML rows whose event type == "Mark"
    │
    ├─ select the requested RPi timestamp column
    │
    ├─ find unique, order-preserving ML↔RPi mark correspondences
    │
    │       └─ temporary affine models may be used internally
    │           to refine correspondence
    │
    └─ freeze matched mark pairs
            │
            ▼
global affine fitting
    │
    ├─ fit t_RPi = a · t_ML + b
    │      from the frozen matched pairs
    │
    ├─ calculate offset, drift, residuals, and affine inliers
    │
    └─ apply the fitted ML→RPi clock transform
       to every timestamped row in the cleaned ML event file
```

The suite performs **mark-level temporal correspondence followed by event-timeline clock alignment**. Mark events provide the synchronization anchors; the fitted affine clock model is then applied to the full cleaned ML event table.

- it **does not** resample raw ML sensor data
- it **does not** perform nonlinear time warping of the ML stream
- it **does not** independently shift each event to its nearest RPi mark
- it **does not** require an RPi observation for every ML event
- it **does** retime every timestamped row in the cleaned ML event CSV into the estimated RPi clock frame by applying the global affine model.



---

## Older or side-path scripts

#### `alignML2Phsyio.py`
- **Input:** ML marks + raw RPi log timestamps
- **Output:** older aligned/drift output
- **Role:** likely an earlier standalone aligner superseded by `merge_ml_with_rpi_marks3.py`

#### `alignPO2AN.py`
- **Input:** PO and AN files
- **Output:** aligned PO↔AN outputs
- **Role:** separate special-purpose alignment utility, not obviously part of the main ML↔RPi flow

#### `list_rpi_files.py`

- **Input:** raw RPi directory tree
- **Output:** CSV inventory of discovered files
- **Role:** discovery / auditing

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

#### `mark_review_app.py`

- **Input:** ML cleaned event CSV(s) + unified RPi marks CSV(s)  + optional prior review decision CSVs
- **Output:** per-session mark review decision CSVs containing mark-level exclude flags, reasons, ordinals, timestamps, stream, and review metadata
- **Role:** older - manually review individual ML and RPi marks before pairing; identify false/duplicate/bad-time/wrong-block marks and record explicit exclusion decisions

``` alignmentSuite
alignmentSuite
    │
    └─ batch_split_pipeline5.py   [BATCH]
         │ 		General Input: Unified RPi marks + ML cleaned event CSVs + collatedData.xlsx
         │ 		General Output: Per-source matched-mark tables, per-source globally aligned ML event CSVs,
         │       							affine summaries / diagnostics, drift/residual plots and summaries, and
         │       							optional combined BioPac/RNS event CSV
         │		Role: Coordinates ML↔RPi mark matching, global affine fitting, alignment summarization, and          		 │ 					optional BioPac/RNS merging
         │ 	
         ├─ match_ml_rpi_marks.py
         │    Input: ML events + unified RPi marks
         │    Output: aligned ML↔RPi event CSV (per source)
         │		Role: Select ML mark events, match them uniquely and monotonically to RPi marks, preserve 
         │          unmatched marks, monotonically to RPi marks, preserve unmatched marks, and freeze the
         │          correspondences for downstream fitting
         │
         ├─ fit_global_affine_from_matches.py
         │    Input: ML cleaned event CSV + frozen matched-mark CSV
         │    Output: globally aligned ML event CSV + global affine mark-diagnostics CSV
         │       		  + global affine summary CSV
         │		Role: Fit the final robust global affine ML→RPi clock model from the frozen correspondences, 
         │       	  apply that model to all ML timestamps, and generate fit/residual diagnostics
         │
         ├─ summarize_alignment3.py
         │    Input: globally aligned ML event CSV
         │    Output: clock-offset plot + alignment-residual plot + compact alignment summary CSV
         │		Role: Summarize observed clock offset, post-affine residual error, inlier behavior,
         │          and fitted clock drift
         │
         └─ merge_rpi_event_files2.py
              Input: BioPac-aligned ML CSV + RNS-aligned ML CSV
              Output: combined BioPac/RNS event CSV
              Role: Combine the independently aligned BioPac and RNS event columns into one session-level 											event  file
              
```
