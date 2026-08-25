#!/usr/bin/env python3
# qc_first50_rounds.py

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROUND_KEY_CANDIDATES = [
    "sessionID",
    "source_file",
    "BlockInstance",
    "BlockNum",
    "RoundNum",
]

SUMMARY_METRICS = [
    "actualTestRoundsAvailable",
    "actualTestRoundsIncluded",
    "totPinDrops",
    "totCorrect",
    "totScore",
    "totSwap_all",
    "totNormal",
    "totSwap_neg",
    "totSwap_pos",
    "swapRate_tot",
    "totRounds",
    "totSwap_Rounds",
    "totNormal_Rounds",
    "swapVoteScore",
    "swapVoteRegistered_n_sum",
    "swapVoteRegistered_d_sum",
    "swapVoteRegistered_recalc",
    "totPoints",
    "avgRoundSpeed",
    "stdRoundSpeed",
    "avgPathEff",
    "stdPathEff",
    "num_HVLVNV",
    "num_LVHVNV",
    "num_HVNVLV",
    "num_NVHVLV",
    "num_LVNVHV",
    "num_NVLVHV",
]

DEFAULT_EVENT_PATTERN = "*1st50IntervalDataKnotted*_all.csv"
DEFAULT_SUMMARY_PATTERN = "*participantSummaryData_1st50*.csv"


def normalize_text(value: object) -> str:
    """Normalize identifiers used for matching."""
    if pd.isna(value):
        return ""

    text = str(value).strip()

    if text.endswith(".0"):
        candidate = text[:-2]
        if candidate.replace("-", "").isdigit():
            return candidate

    return text


def to_bool(series: pd.Series) -> pd.Series:
    """Convert common boolean encodings to bool."""
    normalized = (
        series.astype(str)
        .str.strip()
        .str.lower()
    )

    return normalized.isin(
        {
            "true",
            "1",
            "yes",
            "y",
            "t",
        }
    )


def first_non_null(series: pd.Series):
    """Return the first non-null value in a series."""
    values = series.dropna()

    if values.empty:
        return None

    value = values.iloc[0]
    return value.item() if hasattr(value, "item") else value


def unique_values_text(series: pd.Series) -> str:
    """Return unique non-null values as pipe-delimited text."""
    values = [
        str(value)
        for value in pd.unique(series.dropna())
    ]

    return "|".join(values)


def require_columns(
    df: pd.DataFrame,
    columns: list[str],
    file_label: str,
) -> None:
    """Validate required columns."""
    missing = [
        column
        for column in columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{file_label}: missing required columns: {missing}"
        )


def discover_single_csv(
    directory: Path,
    pattern: str,
    label: str,
) -> Path:
    """Find exactly one matching CSV in a directory tree."""
    matches = sorted(
        path
        for path in directory.rglob(pattern)
        if path.is_file()
    )

    if not matches:
        raise FileNotFoundError(
            f"No {label} CSV matched {pattern!r} under {directory}"
        )

    if len(matches) > 1:
        choices = "\n".join(
            f"  - {path}"
            for path in matches
        )

        raise ValueError(
            f"Multiple {label} CSV files matched {pattern!r} "
            f"under {directory}:\n{choices}\n"
            "Use an explicit file path or a more specific pattern."
        )

    return matches[0]


def resolve_event_file(
    positional_path: Path | None,
    input_dir: Path | None,
    explicit_event_file: Path | None,
    event_pattern: str,
) -> Path:
    """Resolve the final event-level CSV."""
    supplied = [
        value
        for value in [
            positional_path,
            input_dir,
            explicit_event_file,
        ]
        if value is not None
    ]

    if len(supplied) == 0:
        raise ValueError(
            "Provide event input using event_path, --input_dir, "
            "or --event_file."
        )

    if len(supplied) > 1:
        raise ValueError(
            "Use only one of event_path, --input_dir, or --event_file."
        )

    candidate = supplied[0]

    if candidate.is_file():
        return candidate

    if candidate.is_dir():
        return discover_single_csv(
            directory=candidate,
            pattern=event_pattern,
            label="event",
        )

    raise FileNotFoundError(
        f"Event input does not exist: {candidate}"
    )


def resolve_summary_file(
    positional_path: Path | None,
    explicit_summary_file: Path | None,
    summary_dir: Path | None,
    summary_pattern: str,
) -> Path:
    """Resolve the participant summary CSV."""
    supplied = [
        value
        for value in [
            positional_path,
            explicit_summary_file,
            summary_dir,
        ]
        if value is not None
    ]

    if len(supplied) == 0:
        raise ValueError(
            "Provide the participant summary using participant_summary_path, "
            "--participant_summary, --summary_file, or --summary_dir."
        )

    if len(supplied) > 1:
        raise ValueError(
            "Use only one participant-summary input method."
        )

    candidate = supplied[0]

    if candidate.is_file():
        return candidate

    if candidate.is_dir():
        return discover_single_csv(
            directory=candidate,
            pattern=summary_pattern,
            label="participant summary",
        )

    raise FileNotFoundError(
        f"Participant summary input does not exist: {candidate}"
    )


def get_round_key_columns(
    event_df: pd.DataFrame,
) -> list[str]:
    """Select available columns that identify logical rounds."""
    key_columns = [
        column
        for column in ROUND_KEY_CANDIDATES
        if column in event_df.columns
    ]

    required_identifiers = {
        "sessionID",
        "BlockInstance",
        "BlockNum",
        "RoundNum",
    }

    missing = required_identifiers - set(key_columns)

    if missing:
        raise ValueError(
            "Final event file cannot construct logical round keys. "
            f"Missing: {sorted(missing)}"
        )

    return key_columns


def add_counter_flags(
    event_df: pd.DataFrame,
    max_rounds: int,
) -> pd.DataFrame:
    """Add row-level counter eligibility and consistency flags."""
    working = event_df.copy()

    working["_mainRoundNum"] = pd.to_numeric(
        working["TotSesh_actTest_RoundNum"],
        errors="coerce",
    )

    working["_allRoundNum"] = pd.to_numeric(
        working["TotSesh_actTest_RoundNum_all"],
        errors="coerce",
    )

    working["_runTotalMain"] = pd.to_numeric(
        working["TotSesh_runTot_RoundNum"],
        errors="coerce",
    )

    working["_runTotalAll"] = pd.to_numeric(
        working["TotSesh_runTot_RoundNum_all"],
        errors="coerce",
    )

    working["_isExcludedFinalTruncated"] = to_bool(
        working["isExcludedFinalRoundOfTruncatedBlock"]
    )

    if "isExcludedTruncatedRound" in working.columns:
        working["_downstreamExcludedFlag"] = to_bool(
            working["isExcludedTruncatedRound"]
        )
    else:
        working["_downstreamExcludedFlag"] = False

    working["_mainActualTest"] = (
        working["_mainRoundNum"].notna()
    )

    working["_allActualTest"] = (
        working["_allRoundNum"].notna()
    )

    working["_mainFirstN"] = (
        working["_mainRoundNum"].between(
            1,
            max_rounds,
            inclusive="both",
        )
    )

    working["_allFirstN"] = (
        working["_allRoundNum"].between(
            1,
            max_rounds,
            inclusive="both",
        )
    )

    working["_expectedTruncatedFinalExclusion"] = (
        working["_isExcludedFinalTruncated"]
        & working["_allActualTest"]
        & ~working["_mainActualTest"]
    )

    working["_unexpectedFlaggedIncludedMain"] = (
        working["_isExcludedFinalTruncated"]
        & working["_mainActualTest"]
    )

    working["_unexpectedUnflaggedMissingMain"] = (
        ~working["_isExcludedFinalTruncated"]
        & working["_allActualTest"]
        & ~working["_mainActualTest"]
    )

    working["_unexpectedMainWithoutAll"] = (
        working["_mainActualTest"]
        & ~working["_allActualTest"]
    )

    working["_unexpectedExcludedHasRunMain"] = (
        working["_isExcludedFinalTruncated"]
        & working["_runTotalMain"].notna()
    )

    working["_excludedFlagMismatch"] = (
        working["_isExcludedFinalTruncated"]
        != working["_downstreamExcludedFlag"]
    )

    return working


def classify_round(
    group: pd.DataFrame,
    max_rounds: int,
) -> dict:
    """Classify one logical round using corrected counter semantics."""
    main_values = pd.to_numeric(
        group["TotSesh_actTest_RoundNum"],
        errors="coerce",
    ).dropna()

    all_values = pd.to_numeric(
        group["TotSesh_actTest_RoundNum_all"],
        errors="coerce",
    ).dropna()

    run_main_values = pd.to_numeric(
        group["TotSesh_runTot_RoundNum"],
        errors="coerce",
    ).dropna()

    run_all_values = pd.to_numeric(
        group["TotSesh_runTot_RoundNum_all"],
        errors="coerce",
    ).dropna()

    main_round = (
        float(main_values.iloc[0])
        if not main_values.empty
        else np.nan
    )

    all_round = (
        float(all_values.iloc[0])
        if not all_values.empty
        else np.nan
    )

    run_main = (
        float(run_main_values.iloc[0])
        if not run_main_values.empty
        else np.nan
    )

    run_all = (
        float(run_all_values.iloc[0])
        if not run_all_values.empty
        else np.nan
    )

    flagged = bool(
        group["_isExcludedFinalTruncated"].any()
    )

    downstream_flagged = bool(
        group["_downstreamExcludedFlag"].any()
    )

    main_exists = pd.notna(main_round)
    all_exists = pd.notna(all_round)

    main_first_n = bool(
        main_exists
        and 1 <= main_round <= max_rounds
    )

    all_first_n = bool(
        all_exists
        and 1 <= all_round <= max_rounds
    )

    expected_exclusion = bool(
        flagged
        and all_exists
        and not main_exists
    )

    unexpected_reasons = []

    if main_values.nunique() > 1:
        unexpected_reasons.append(
            "multiple_main_round_numbers_within_logical_round"
        )

    if all_values.nunique() > 1:
        unexpected_reasons.append(
            "multiple_all_round_numbers_within_logical_round"
        )

    if run_main_values.nunique() > 1:
        unexpected_reasons.append(
            "multiple_run_main_numbers_within_logical_round"
        )

    if run_all_values.nunique() > 1:
        unexpected_reasons.append(
            "multiple_run_all_numbers_within_logical_round"
        )

    if flagged and main_exists:
        unexpected_reasons.append(
            "truncated_final_round_has_main_actual_test_number"
        )

    if flagged and pd.notna(run_main):
        unexpected_reasons.append(
            "truncated_final_round_has_main_running_number"
        )

    if (
        not flagged
        and all_exists
        and not main_exists
    ):
        unexpected_reasons.append(
            "unflagged_all_round_missing_main_actual_test_number"
        )

    if main_exists and not all_exists:
        unexpected_reasons.append(
            "main_actual_test_number_exists_without_all_number"
        )

    if (
        "isExcludedTruncatedRound" in group.columns
        and flagged != downstream_flagged
    ):
        unexpected_reasons.append(
            "counter_exclusion_flag_disagrees_with_downstream_flag"
        )

    if expected_exclusion:
        classification = (
            "expected_truncated_final_exclusion"
        )
    elif unexpected_reasons:
        classification = "unexpected_counter_case"
    elif main_first_n:
        classification = "included_main_firstN"
    elif all_first_n:
        classification = "all_firstN_not_main"
    elif main_exists:
        classification = "main_actual_test_outside_firstN"
    elif all_exists:
        classification = "all_actual_test_outside_firstN"
    else:
        classification = "not_actual_test"

    return {
        "sessionID": first_non_null(
            group["sessionID"]
        ),
        "participantID": (
            first_non_null(group["participantID"])
            if "participantID" in group.columns
            else None
        ),
        "main_RR": (
            first_non_null(group["main_RR"])
            if "main_RR" in group.columns
            else None
        ),
        "currentRole": (
            first_non_null(group["currentRole"])
            if "currentRole" in group.columns
            else None
        ),
        "source_file": (
            first_non_null(group["source_file"])
            if "source_file" in group.columns
            else None
        ),
        "BlockInstance": first_non_null(
            group["BlockInstance"]
        ),
        "BlockNum": first_non_null(
            group["BlockNum"]
        ),
        "RoundNum": first_non_null(
            group["RoundNum"]
        ),
        "round_index_in_block": (
            first_non_null(group["round_index_in_block"])
            if "round_index_in_block" in group.columns
            else None
        ),
        "effectiveRoundNum": (
            first_non_null(group["effectiveRoundNum"])
            if "effectiveRoundNum" in group.columns
            else None
        ),
        "TotSesh_runTot_RoundNum": run_main,
        "TotSesh_runTot_RoundNum_all": run_all,
        "TotSesh_actTest_RoundNum": main_round,
        "TotSesh_actTest_RoundNum_all": all_round,
        "isExcludedFinalRoundOfTruncatedBlock": flagged,
        "isExcludedTruncatedRound": downstream_flagged,
        "mainFirstN": main_first_n,
        "allFirstN": all_first_n,
        "expectedTruncatedFinalExclusion": expected_exclusion,
        "counterClassification": classification,
        "unexpectedCounterReason": ";".join(
            unexpected_reasons
        ),
        "eventRowsInRound": int(len(group)),
        "mainRoundNumberUniqueCount": int(
            main_values.nunique()
        ),
        "allRoundNumberUniqueCount": int(
            all_values.nunique()
        ),
        "runMainNumberUniqueCount": int(
            run_main_values.nunique()
        ),
        "runAllNumberUniqueCount": int(
            run_all_values.nunique()
        ),
        "BlockStatus_values": (
            unique_values_text(group["BlockStatus"])
            if "BlockStatus" in group.columns
            else ""
        ),
        "BlockType_values": (
            unique_values_text(group["BlockType"])
            if "BlockType" in group.columns
            else ""
        ),
        "CoinSetID_values": (
            unique_values_text(group["CoinSetID"])
            if "CoinSetID" in group.columns
            else ""
        ),
        "isIncompleteRound_values": (
            unique_values_text(group["isIncompleteRound"])
            if "isIncompleteRound" in group.columns
            else ""
        ),
        "isLastRoundOfSourceFile_values": (
            unique_values_text(
                group["isLastRoundOfSourceFile"]
            )
            if "isLastRoundOfSourceFile" in group.columns
            else ""
        ),
    }


def build_round_audit(
    event_df: pd.DataFrame,
    max_rounds: int,
) -> pd.DataFrame:
    """Build one QC record per logical round."""
    round_key_columns = get_round_key_columns(
        event_df
    )

    records = []

    for _, group in event_df.groupby(
        round_key_columns,
        dropna=False,
        sort=False,
    ):
        records.append(
            classify_round(
                group,
                max_rounds=max_rounds,
            )
        )

    round_audit = pd.DataFrame(records)

    sort_columns = [
        column
        for column in [
            "sessionID",
            "TotSesh_actTest_RoundNum_all",
            "TotSesh_runTot_RoundNum_all",
            "BlockInstance",
            "RoundNum",
        ]
        if column in round_audit.columns
    ]

    return (
        round_audit.sort_values(
            sort_columns,
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )


def build_summary_session_mapping(
    summary_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build the participant-summary matching table."""
    require_columns(
        summary_df,
        [
            "inputFile",
            "sessionID",
        ],
        "Participant summary",
    )

    mapping = summary_df.copy()

    mapping["_sessionKey"] = (
        mapping["sessionID"]
        .map(normalize_text)
    )

    duplicate_sessions = mapping.loc[
        mapping["_sessionKey"].duplicated(
            keep=False
        ),
        [
            "inputFile",
            "sessionID",
        ],
    ]

    if not duplicate_sessions.empty:
        raise ValueError(
            "Participant summary has duplicate sessionID values:\n"
            f"{duplicate_sessions.to_string(index=False)}"
        )

    return mapping


def build_participant_qc(
    event_df: pd.DataFrame,
    round_audit: pd.DataFrame,
    summary_df: pd.DataFrame,
    max_rounds: int,
) -> pd.DataFrame:
    """Build participant-level reconciliation."""
    summary = build_summary_session_mapping(
        summary_df
    )

    event_working = event_df.copy()

    event_working["_sessionKey"] = (
        event_working["sessionID"]
        .map(normalize_text)
    )

    round_working = round_audit.copy()

    round_working["_sessionKey"] = (
        round_working["sessionID"]
        .map(normalize_text)
    )

    event_records = []

    for session_key, group in event_working.groupby(
        "_sessionKey",
        dropna=False,
        sort=False,
    ):
        session_rounds = round_working.loc[
            round_working["_sessionKey"]
            == session_key
        ].copy()

        unexpected_event_mask = (
            group["_unexpectedFlaggedIncludedMain"]
            | group["_unexpectedUnflaggedMissingMain"]
            | group["_unexpectedMainWithoutAll"]
            | group["_unexpectedExcludedHasRunMain"]
        )

        if "isExcludedTruncatedRound" in group.columns:
            unexpected_event_mask |= (
                group["_excludedFlagMismatch"]
            )

        event_records.append(
            {
                "_sessionKey": session_key,
                "sessionID_event": first_non_null(
                    group["sessionID"]
                ),
                "participantID_event": (
                    first_non_null(group["participantID"])
                    if "participantID" in group.columns
                    else None
                ),
                "main_RR_event": (
                    first_non_null(group["main_RR"])
                    if "main_RR" in group.columns
                    else None
                ),
                "eventRowsTotal": int(len(group)),
                "eventRowsMainFirstN": int(
                    group["_mainFirstN"].sum()
                ),
                "eventRowsAllFirstN": int(
                    group["_allFirstN"].sum()
                ),
                "eventRowsExpectedExcluded": int(
                    group[
                        "_expectedTruncatedFinalExclusion"
                    ].sum()
                ),
                "eventRowsUnexpectedCounterCases": int(
                    unexpected_event_mask.sum()
                ),
                "roundsPresentMainFirstN": int(
                    session_rounds["mainFirstN"].sum()
                ),
                "roundsPresentAllFirstN": int(
                    session_rounds["allFirstN"].sum()
                ),
                "expectedExcludedTruncatedRounds": int(
                    session_rounds[
                        "expectedTruncatedFinalExclusion"
                    ].sum()
                ),
                "unexpectedCounterRounds": int(
                    (
                        session_rounds[
                            "counterClassification"
                        ]
                        == "unexpected_counter_case"
                    ).sum()
                ),
                "maxMainActualTestRoundPresent": (
                    float(
                        session_rounds[
                            "TotSesh_actTest_RoundNum"
                        ].max()
                    )
                    if session_rounds[
                        "TotSesh_actTest_RoundNum"
                    ].notna().any()
                    else np.nan
                ),
                "maxAllActualTestRoundPresent": (
                    float(
                        session_rounds[
                            "TotSesh_actTest_RoundNum_all"
                        ].max()
                    )
                    if session_rounds[
                        "TotSesh_actTest_RoundNum_all"
                    ].notna().any()
                    else np.nan
                ),
            }
        )

    event_summary = pd.DataFrame(
        event_records
    )

    merge_columns = [
        "_sessionKey",
        "inputFile",
        "sessionID",
    ]

    for column in SUMMARY_METRICS:
        if column in summary.columns:
            merge_columns.append(column)

    merged = summary[
        merge_columns
    ].merge(
        event_summary,
        on="_sessionKey",
        how="outer",
        validate="one_to_one",
        indicator=True,
    )

    merged["check_sessionMatched"] = (
        merged["_merge"] == "both"
    )

    merged["check_noUnexpectedCounterRounds"] = (
        merged["unexpectedCounterRounds"]
        .fillna(0)
        .eq(0)
    )

    merged["check_mainRoundMaxWithinCutoff"] = (
        merged["maxMainActualTestRoundPresent"]
        .fillna(0)
        .le(max_rounds)
    )

    if "totPinDrops" in merged.columns:
        merged[
            "difference_eventRowsMain_minus_totPinDrops"
        ] = (
            merged["eventRowsMainFirstN"]
            - merged["totPinDrops"]
        )

        merged["check_eventRowsMatchTotPinDrops"] = (
            merged[
                "difference_eventRowsMain_minus_totPinDrops"
            ]
            .fillna(np.inf)
            .eq(0)
        )

    if "totRounds" in merged.columns:
        merged[
            "difference_eventMainRounds_minus_totRounds"
        ] = (
            merged["roundsPresentMainFirstN"]
            - merged["totRounds"]
        )

        merged["check_eventRoundsMatchTotRounds"] = (
            merged[
                "difference_eventMainRounds_minus_totRounds"
            ]
            .fillna(np.inf)
            .eq(0)
        )

    if "actualTestRoundsIncluded" in merged.columns:
        merged[
            "difference_eventMainRounds_minus_actualIncluded"
        ] = (
            merged["roundsPresentMainFirstN"]
            - merged["actualTestRoundsIncluded"]
        )

        merged[
            "check_eventRoundsMatchActualIncluded"
        ] = (
            merged[
                "difference_eventMainRounds_minus_actualIncluded"
            ]
            .fillna(np.inf)
            .eq(0)
        )

    if "swapVoteRegistered_d_sum" in merged.columns:
        merged[
            "difference_eventMainRounds_minus_swapVoteDenominator"
        ] = (
            merged["roundsPresentMainFirstN"]
            - merged["swapVoteRegistered_d_sum"]
        )

        merged[
            "check_eventRoundsMatchSwapVoteDenominator"
        ] = (
            merged[
                "difference_eventMainRounds_minus_swapVoteDenominator"
            ]
            .fillna(np.inf)
            .eq(0)
        )

    core_checks = [
        "check_sessionMatched",
        "check_noUnexpectedCounterRounds",
        "check_mainRoundMaxWithinCutoff",
    ]

    merged["counterQcPass"] = (
        merged[core_checks]
        .fillna(False)
        .all(axis=1)
    )

    reconciliation_checks = [
        column
        for column in [
            "check_eventRowsMatchTotPinDrops",
            "check_eventRoundsMatchTotRounds",
            "check_eventRoundsMatchActualIncluded",
            "check_eventRoundsMatchSwapVoteDenominator",
        ]
        if column in merged.columns
    ]

    if reconciliation_checks:
        merged["summaryReconciliationPass"] = (
            merged[reconciliation_checks]
            .fillna(False)
            .all(axis=1)
        )
    else:
        merged["summaryReconciliationPass"] = False

    return (
        merged.drop(
            columns=[
                "_sessionKey",
                "_merge",
            ]
        )
        .sort_values(
            [
                "counterQcPass",
                "summaryReconciliationPass",
                "inputFile",
            ],
            ascending=[
                True,
                True,
                True,
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def build_classification_summary(
    round_audit: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize counter classifications."""
    return (
        round_audit.groupby(
            "counterClassification",
            dropna=False,
        )
        .agg(
            rounds=(
                "counterClassification",
                "size",
            ),
            sessionsAffected=(
                "sessionID",
                "nunique",
            ),
            eventRows=(
                "eventRowsInRound",
                "sum",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "rounds",
                "counterClassification",
            ],
            ascending=[
                False,
                True,
            ],
            kind="stable",
        )
    )


def write_outputs(
    output_dir: Path,
    participant_qc: pd.DataFrame,
    round_audit: pd.DataFrame,
) -> dict[str, Path]:
    """Write detailed QC outputs."""
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    expected_exclusions = round_audit.loc[
        round_audit[
            "expectedTruncatedFinalExclusion"
        ]
    ].copy()

    unexpected_cases = round_audit.loc[
        round_audit[
            "counterClassification"
        ].eq("unexpected_counter_case")
    ].copy()

    summary_mismatches = participant_qc.loc[
        (
            ~participant_qc[
                "summaryReconciliationPass"
            ]
        )
        | (
            ~participant_qc[
                "counterQcPass"
            ]
        )
    ].copy()

    classification_summary = (
        build_classification_summary(
            round_audit
        )
    )

    outputs = {
        "participant_summary":
            output_dir / "qc_participant_summary.csv",
        "round_audit":
            output_dir / "qc_round_counter_audit.csv",
        "expected_exclusions":
            output_dir / "qc_expected_truncated_exclusions.csv",
        "unexpected_cases":
            output_dir / "qc_unexpected_counter_cases.csv",
        "summary_mismatches":
            output_dir / "qc_summary_reconciliation_mismatches.csv",
        "classification_summary":
            output_dir / "qc_counter_classification_summary.csv",
    }

    participant_qc.to_csv(
        outputs["participant_summary"],
        index=False,
    )

    round_audit.to_csv(
        outputs["round_audit"],
        index=False,
    )

    expected_exclusions.to_csv(
        outputs["expected_exclusions"],
        index=False,
    )

    unexpected_cases.to_csv(
        outputs["unexpected_cases"],
        index=False,
    )

    summary_mismatches.to_csv(
        outputs["summary_mismatches"],
        index=False,
    )

    classification_summary.to_csv(
        outputs["classification_summary"],
        index=False,
    )

    for output_path in outputs.values():
        print(f"Wrote: {output_path}")

    return outputs


def write_manifest(
    manifest_dir: Path,
    event_file: Path,
    summary_file: Path,
    output_paths: dict[str, Path],
    participant_qc: pd.DataFrame,
    round_audit: pd.DataFrame,
    max_rounds: int,
) -> Path:
    """Write a one-row QC processing manifest."""
    manifest_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest_path = (
        manifest_dir
        / "qc_first50_rounds_manifest.csv"
    )

    manifest = pd.DataFrame(
        [
            {
                "eventFile": str(event_file),
                "participantSummaryFile": str(summary_file),
                "maxRounds": max_rounds,
                "participantRows": int(
                    len(participant_qc)
                ),
                "logicalRounds": int(
                    len(round_audit)
                ),
                "expectedTruncatedFinalExclusions": int(
                    round_audit[
                        "expectedTruncatedFinalExclusion"
                    ].sum()
                ),
                "unexpectedCounterCases": int(
                    round_audit[
                        "counterClassification"
                    ].eq(
                        "unexpected_counter_case"
                    ).sum()
                ),
                "counterQcFailures": int(
                    (~participant_qc["counterQcPass"]).sum()
                ),
                "summaryReconciliationFailures": int(
                    (
                        ~participant_qc[
                            "summaryReconciliationPass"
                        ]
                    ).sum()
                ),
                "outputFiles": json.dumps(
                    {
                        key: str(value)
                        for key, value in output_paths.items()
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    )

    manifest.to_csv(
        manifest_path,
        index=False,
    )

    print(f"Wrote manifest: {manifest_path}")

    return manifest_path


def print_console_summary(
    participant_qc: pd.DataFrame,
    round_audit: pd.DataFrame,
) -> None:
    """Print a compact QC summary."""
    print()
    print(
        f"Sessions in participant QC: "
        f"{len(participant_qc)}"
    )
    print(
        f"Logical rounds in final event file: "
        f"{len(round_audit)}"
    )
    print(
        "Expected truncated-final exclusions: "
        f"{int(round_audit['expectedTruncatedFinalExclusion'].sum())}"
    )
    print(
        "Unexpected counter cases: "
        f"{int(round_audit['counterClassification'].eq('unexpected_counter_case').sum())}"
    )
    print(
        "Sessions failing counter QC: "
        f"{int((~participant_qc['counterQcPass']).sum())}"
    )
    print(
        "Sessions failing summary reconciliation: "
        f"{int((~participant_qc['summaryReconciliationPass']).sum())}"
    )


def main() -> None:
    """Run first-N QC using final event-level counters."""
    parser = argparse.ArgumentParser(
        description=(
            "Audit corrected TotSesh counters in the final "
            "first-N event-level file and reconcile them "
            "with the participant summary."
        )
    )

    parser.add_argument(
        "event_path",
        nargs="?",
        type=Path,
        help=(
            "Final event CSV or directory containing it. "
            "May be used instead of --input_dir or --event_file."
        ),
    )

    parser.add_argument(
        "participant_summary_path",
        nargs="?",
        type=Path,
        help=(
            "Participant summary CSV or directory containing it. "
            "May be used instead of --participant_summary."
        ),
    )

    parser.add_argument(
        "--input_dir",
        type=Path,
        default=None,
        help=(
            "Directory containing the final event-level CSV."
        ),
    )

    parser.add_argument(
        "--event_file",
        type=Path,
        default=None,
        help=(
            "Explicit final event-level CSV path."
        ),
    )

    parser.add_argument(
        "--participant_summary",
        "--summary_file",
        dest="participant_summary",
        type=Path,
        default=None,
        help=(
            "Explicit participant first-50 summary CSV."
        ),
    )

    parser.add_argument(
        "--summary_dir",
        type=Path,
        default=None,
        help=(
            "Directory containing the participant summary CSV."
        ),
    )

    parser.add_argument(
        "--output_dir",
        "--out-dir",
        dest="output_dir",
        type=Path,
        default=Path("qc_first50_final"),
        help=(
            "Directory for QC outputs "
            "(default: qc_first50_final)."
        ),
    )

    parser.add_argument(
        "--manifest_dir",
        type=Path,
        default=None,
        help=(
            "Directory where the QC manifest CSV is written."
        ),
    )

    parser.add_argument(
        "--event_pattern",
        default=DEFAULT_EVENT_PATTERN,
        help=(
            "Pattern used to locate the event CSV inside "
            f"--input_dir (default: {DEFAULT_EVENT_PATTERN!r})."
        ),
    )

    parser.add_argument(
        "--summary_pattern",
        default=DEFAULT_SUMMARY_PATTERN,
        help=(
            "Pattern used to locate the participant summary "
            f"inside --summary_dir (default: {DEFAULT_SUMMARY_PATTERN!r})."
        ),
    )

    parser.add_argument(
        "--max-rounds",
        "--max_rounds",
        dest="max_rounds",
        type=int,
        default=50,
        help=(
            "Maximum main actual-test round number "
            "(default: 50)."
        ),
    )

    args = parser.parse_args()

    if args.max_rounds <= 0:
        parser.error(
            "--max-rounds must be a positive integer."
        )

    try:
        event_file = resolve_event_file(
            positional_path=args.event_path,
            input_dir=args.input_dir,
            explicit_event_file=args.event_file,
            event_pattern=args.event_pattern,
        )

        summary_file = resolve_summary_file(
            positional_path=args.participant_summary_path,
            explicit_summary_file=args.participant_summary,
            summary_dir=args.summary_dir,
            summary_pattern=args.summary_pattern,
        )
    except (
        FileNotFoundError,
        ValueError,
    ) as exc:
        parser.error(str(exc))

    event_df = pd.read_csv(
        event_file,
        low_memory=False,
    )

    summary_df = pd.read_csv(
        summary_file,
        low_memory=False,
    )

    require_columns(
        event_df,
        [
            "sessionID",
            "BlockInstance",
            "BlockNum",
            "RoundNum",
            "TotSesh_runTot_RoundNum",
            "TotSesh_runTot_RoundNum_all",
            "TotSesh_actTest_RoundNum",
            "TotSesh_actTest_RoundNum_all",
            "isExcludedFinalRoundOfTruncatedBlock",
        ],
        event_file.name,
    )

    event_df = add_counter_flags(
        event_df,
        max_rounds=args.max_rounds,
    )

    round_audit = build_round_audit(
        event_df,
        max_rounds=args.max_rounds,
    )

    participant_qc = build_participant_qc(
        event_df=event_df,
        round_audit=round_audit,
        summary_df=summary_df,
        max_rounds=args.max_rounds,
    )

    output_paths = write_outputs(
        output_dir=args.output_dir,
        participant_qc=participant_qc,
        round_audit=round_audit,
    )

    manifest_dir = (
        args.manifest_dir
        if args.manifest_dir is not None
        else args.output_dir
    )

    write_manifest(
        manifest_dir=manifest_dir,
        event_file=event_file,
        summary_file=summary_file,
        output_paths=output_paths,
        participant_qc=participant_qc,
        round_audit=round_audit,
        max_rounds=args.max_rounds,
    )

    print_console_summary(
        participant_qc=participant_qc,
        round_audit=round_audit,
    )


if __name__ == "__main__":
    main()