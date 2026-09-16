# muscles_eventParser_PO_adapted.py
"""
PO (Observer / Observational Participant) event parser.

Aligned to AN parser conventions:
- Every event has: eMLT_orig, AppTime, mLT_raw, mLT_orig,
  plus start_* / end_* for both AppTime and eMLT_orig.
- AppTime + eMLT_orig are normalized to numeric seconds up-front.
- Cascades are sorted by time before backfill_approx_row_indices_v2.

PO chest policy:
- Trigger on chest *collection* only.
- Emit ONLY synthetic events that occur at/after the collection moment (t >= 0).
- Do NOT infer chest appearing/opening/coin appearing (those require AN↔PO alignment).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import re
import traceback

from RC_utilities.segHelpers.schwannCells_eventParserHelper import (
    build_common_event_fields_noTime,
    backfill_approx_row_indices_v2,
    generate_synthetic_events_v3,
)


def _to_seconds(x: Any) -> Optional[float]:
    """Normalize timestamps to float seconds (handles numeric, numeric strings, datetimes)."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and not x.strip()):
        return None

    if isinstance(x, (int, float)):
        return float(x)

    if isinstance(x, pd.Timestamp):
        return x.timestamp()

    if hasattr(x, "to_pydatetime"):
        try:
            return x.to_pydatetime().timestamp()
        except Exception:
            pass

    if isinstance(x, str):
        s = x.strip()
        try:
            return float(s)
        except Exception:
            ts = pd.to_datetime(s, errors="coerce")
            if pd.isna(ts):
                return None
            return ts.to_pydatetime().timestamp()

    try:
        return float(x)
    except Exception:
        return None


def _normalize_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure AppTime + eMLT_orig exist and are numeric seconds."""
    out = df.copy()

    out["AppTime"] = out["AppTime"].map(_to_seconds) if "AppTime" in out.columns else None
    out["eMLT_orig"] = out["eMLT_orig"].map(_to_seconds) if "eMLT_orig" in out.columns else None

    return out


def process_swap_votes_v4(df: pd.DataFrame, allowed_statuses: set) -> List[Dict[str, Any]]:
    """Trigger: 'Observer says it was an? <...>.'"""
    events: List[Dict[str, Any]] = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row.get("Type") == "Event" and isinstance(row.get("Message"), str):
            match = re.match(r"Observer says it was an?\s+(.*)\.", row["Message"])
            if not match:
                continue

            try:
                swapvote = match.group(1).strip().upper()

                app_time = row["AppTime"]
                start_ts = row["eMLT_orig"]
                mLT_raw = row.get("mLT_raw")
                mLT_orig = row.get("mLT_orig")

                common_info = build_common_event_fields_noTime(row, i)

                coinset = row.get("CoinSetID")
                if coinset in [1, 4]:
                    correct_answer = "OLD ROUND"
                elif coinset in [2, 3, 5]:
                    correct_answer = "NEW ROUND"
                else:
                    correct_answer = None

                score = "Unknown" if correct_answer is None else ("Correct" if swapvote == correct_answer else "Incorrect")

                events.append(
                    {
                        "eMLT_orig": start_ts,
                        "AppTime": app_time,
                        "mLT_raw": mLT_raw,
                        "mLT_orig": mLT_orig,
                        "start_AppTime": app_time,
                        "end_AppTime": app_time,
                        "start_eMLT_orig": start_ts,
                        "end_eMLT_orig": start_ts,
                        "lo_eventType": "SwapVote_Moment",
                        "med_eventType": "SwapVote",
                        "hi_eventType": "SwapVote",
                        "hiMeta_eventType": "SwapVote",
                        "details": {"SwapVote": swapvote, "SwapVoteScore": score},
                        "source": "logged",
                        **common_info,
                    }
                )

                offsets_events = [
                    ("SwapVoteText_Vis_end", 0.000, 0.000),
                    ("BlockScoreText_Vis_start", 0.000, 2.000),
                ]
                event_meta = {
                    "med_eventType": "PostSwapVoteEvents",
                    "hi_eventType": "SwapVote",
                    "hiMeta_eventType": "SwapVote",
                }
                events.extend(generate_synthetic_events_v3(start_ts, app_time, offsets_events, common_info, event_meta))

            except Exception as e:
                print(f"⚠️ Failed to process swap vote at row {i}: {e}")

    return events


def process_pin_drop_v5(df: pd.DataFrame, allowed_statuses: set) -> List[Dict[str, Any]]:
    """Trigger: 'Other participant just dropped a new pin at ...'"""
    events: List[Dict[str, Any]] = []
    i = 0

    while i < len(df):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            i += 1
            continue

        if (
            row.get("Type") == "Event"
            and isinstance(row.get("Message"), str)
            and "Other participant just dropped a new pin at" in row["Message"]
        ):
            try:
                common_info = build_common_event_fields_noTime(row, i)

                app_time = row["AppTime"]
                start_ts = row["eMLT_orig"]
                mLT_raw = row.get("mLT_raw")
                mLT_orig = row.get("mLT_orig")

                event: Dict[str, Any] = {
                    "eMLT_orig": start_ts,
                    "AppTime": app_time,
                    "mLT_raw": mLT_raw,
                    "mLT_orig": mLT_orig,
                    "start_AppTime": app_time,
                    "end_AppTime": app_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,
                    "lo_eventType": "PinDrop_Moment",
                    "med_eventType": "PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info,
                }

                j = i + 1
                while j < len(df):
                    next_row = df.iloc[j]

                    if next_row.get("Type") != "Event" or not isinstance(next_row.get("Message"), str):
                        j += 1
                        continue

                    if next_row.get("BlockNum") != row.get("BlockNum"):
                        break

                    msg = next_row["Message"]

                    if "Other participant just dropped a new pin at " in msg:
                        match = re.search(
                            r"at\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)",
                            msg,
                        )
                        if match:
                            try:
                                event["details"].update(
                                    {
                                        "pinLocal_x": float(match.group(1)),
                                        "pinLocal_y": float(match.group(2)),
                                        "pinLocal_z": float(match.group(3)),
                                    }
                                )
                            except ValueError:
                                print(f"⚠️ Float conversion failed in pin location at row {j}: {msg}")

                    elif "Dropped pin was dropped at " in msg:
                        match = re.search(
                            r"Dropped pin was dropped at (?P<dropDist>\d+\.\d{2}) from chest (?P<idvCoinID>\d+)"
                            r" originally at \((?P<coinPos_x>-?\d+\.\d{2}), (?P<coinPos_y>-?\d+\.\d{2}), (?P<coinPos_z>-?\d+\.\d{2})\)"
                            r":(?P<dropQual>CORRECT|INCORRECT)",
                            msg,
                        )
                        if match:
                            try:
                                parsed = match.groupdict()
                                event["details"].update(
                                    {
                                        "dropDist": float(parsed["dropDist"]),
                                        "coinPos_x": float(parsed["coinPos_x"]),
                                        "coinPos_y": float(parsed["coinPos_y"]),
                                        "coinPos_z": float(parsed["coinPos_z"]),
                                        "dropQual": parsed["dropQual"],
                                    }
                                )
                            except ValueError:
                                print(f"⚠️ Drop analysis parsing error at row {j}: {msg}")

                    elif "for this pindrop from the navigator" in msg and "Observer" in msg:
                        m_vote = re.search(
                            r"Observer chose (?P<pinDropVote>CORRECT|INCORRECT) for this pindrop from the navigator",
                            msg,
                        )
                        if m_vote:
                            event["details"].update(m_vote.groupdict())
                            break

                        if "Observer did not vote for this pindrop from the navigator" in msg:
                            event["details"]["pinDropVote"] = "DID_NOT_VOTE"
                            break

                    j += 1

                events.append(event)

                offsets_events = [
                    ("PinDropSound", 0.000, 0.403),
                    ("GrayPinVis", 0.000, 2.000),
                    ("VotingWindow", 0.000, 2.000),
                    ("VoteInstrText_Vis", 0.000, 2.000),
                    ("Feedback_Sound", 2.000, 0.182),
                    ("FeedbackTextVis", 2.000, 1.000),
                    ("FeedbackPinColor", 2.000, 1.000),
                    ("CoinVis_start", 3.000, 0.000),
                    ("CoinPresentSound", 3.000, 0.650),
                ]
                event_meta = {
                    "med_eventType": "PinDrop_Animation",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                }
                events.extend(generate_synthetic_events_v3(start_ts, app_time, offsets_events, common_info, event_meta))

            except Exception as e:
                print(f"⚠️ Failed to process pin drop at row {i}: {e}")

            i = j
        else:
            i += 1

    return events


def process_feedback_collect_v5(df: pd.DataFrame, allowed_statuses: set) -> List[Dict[str, Any]]:
    """Trigger: 'A.N. collected coin:<id> round reward: <value>'"""
    events: List[Dict[str, Any]] = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if (
            row.get("Type") == "Event"
            and isinstance(row.get("Message"), str)
            and row["Message"].startswith("A.N. collected coin:")
        ):
            try:
                common_info = build_common_event_fields_noTime(row, i)

                app_time = row["AppTime"]
                start_ts = row["eMLT_orig"]
                mLT_raw = row.get("mLT_raw")
                mLT_orig = row.get("mLT_orig")

                event: Dict[str, Any] = {
                    "eMLT_orig": start_ts,
                    "AppTime": app_time,
                    "mLT_raw": mLT_raw,
                    "mLT_orig": mLT_orig,
                    "start_AppTime": app_time,
                    "end_AppTime": app_time,
                    "start_eMLT_orig": start_ts,
                    "end_eMLT_orig": start_ts,
                    "lo_eventType": "CoinCollect_Moment_PinDrop",
                    "med_eventType": "CoinCollect_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                    "details": {},
                    "source": "logged",
                    **common_info,
                }

                match = re.search(
                    r"A\.N\. collected coin:(?P<idvCoinID>\d+)\s+round reward:\s+(?P<currRoundTotal>-?\d+\.\d{2})",
                    row["Message"],
                )
                if match:
                    event["details"].update(match.groupdict())

                events.append(event)

                offsets_events = [
                    ("CoinVis_end", 0.000, 0.000),
                    ("CoinValueTextVis", 0.000, 2.000),
                    ("CoinCollectSound_start", 0.000, 0.654),
                ]
                event_meta = {
                    "med_eventType": "CoinCollect_Animation_PinDrop",
                    "hi_eventType": "PinDrop",
                    "hiMeta_eventType": "BlockActivity",
                }
                events.extend(generate_synthetic_events_v3(start_ts, app_time, offsets_events, common_info, event_meta))

            except Exception as e:
                print(f"⚠️ Failed to parse feedback values at row {i}: {e}")

    return events


def process_chest_collect_v3(df: pd.DataFrame, allowed_statuses: set) -> List[Dict[str, Any]]:
    """
    Trigger: 'Other participant just collected coin: <id>'

    Emits:
    - Logged coin collection moment
    - Forward-only synthetic events (t >= 0), e.g.:
        - CoinVis_end at t=0
        - CurrChestVis_end / NextChestVis_start at t=0
        - CoinValueTextVis duration
        - CoinCollectSound duration
    """
    events: List[Dict[str, Any]] = []

    for i in range(len(df)):
        row = df.iloc[i]
        block_status = row.get("BlockStatus", "unknown")
        if pd.notna(row.get("BlockNum")) and block_status not in allowed_statuses:
            continue

        if row.get("Type") != "Event" or not isinstance(row.get("Message"), str):
            continue

        if not row["Message"].startswith("Other participant just collected coin: "):
            continue

        try:
            coin_id = int(row["Message"].replace("Other participant just collected coin: ", "").strip())
            common_info = build_common_event_fields_noTime(row, i)

            app_time = row["AppTime"]
            start_ts = row["eMLT_orig"]
            mLT_raw = row.get("mLT_raw")
            mLT_orig = row.get("mLT_orig")

            # Logged event FIRST (avoids out-of-order cascades)
            event = {
                "eMLT_orig": start_ts,
                "AppTime": app_time,
                "mLT_raw": mLT_raw,
                "mLT_orig": mLT_orig,
                "start_AppTime": app_time,
                "end_AppTime": app_time,
                "start_eMLT_orig": start_ts,
                "end_eMLT_orig": start_ts,
                "lo_eventType": "CoinCollect_Moment_Chest",
                "med_eventType": "CoinCollect_Chest",
                "hi_eventType": "ChestOpen",
                "hiMeta_eventType": "BlockActivity",
                "details": {"idvCoinID": coin_id},
                "source": "logged",
                **common_info,
            }
            events.append(event)

            # Forward-only synthetic followups (NO back-population)
            offsets_events = [
                ("CoinVis_end", 0.000, 0.000),
                ("CurrChestVis_end", 0.000, 0.000),
                ("NextChestVis_start", 0.000, 0.000),
                ("CoinValueTextVis", 0.000, 2.000),
                ("CoinCollectSound", 0.000, 0.654),
            ]
            event_meta = {
                "med_eventType": "CoinCollect_Animation_Chest",
                "hi_eventType": "ChestOpen",
                "hiMeta_eventType": "BlockActivity",
            }
            events.extend(generate_synthetic_events_v3(start_ts, app_time, offsets_events, common_info, event_meta))

        except Exception as e:
            print(f"⚠️ Failed to process chest coin collect at row {i}: {e}")

    return events


def buildEvents_PO(df: pd.DataFrame, allowed_statuses: set) -> List[Dict[str, Any]]:
    """Main entrypoint."""
    try:
        df = _normalize_time_columns(df)

        cascades = (
            process_pin_drop_v5(df, allowed_statuses)
            + process_feedback_collect_v5(df, allowed_statuses)
            + process_chest_collect_v3(df, allowed_statuses)
            + process_swap_votes_v4(df, allowed_statuses)
        )

        cascades_sorted = sorted(
            cascades,
            key=lambda e: (
                float("inf") if e.get("eMLT_orig") is None else e.get("eMLT_orig"),
                float("inf") if e.get("AppTime") is None else e.get("AppTime"),
            ),
        )

        return backfill_approx_row_indices_v2(cascades_sorted, df)

    except KeyError as e:
        print(f"🔥 KEY ERROR: {e}")
        traceback.print_exc()
        raise