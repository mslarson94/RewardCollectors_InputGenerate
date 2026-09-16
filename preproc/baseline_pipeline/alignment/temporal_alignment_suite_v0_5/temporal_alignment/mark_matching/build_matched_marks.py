from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from temporal_alignment.common.io import read_csv, write_csv, write_json
from temporal_alignment.common.timestamps import parse_times


def automatic_match(ml, rpi, max_gap_s):
    ml = ml.sort_values("ml_ordinal").reset_index(drop=True)
    rpi = rpi.sort_values("rpi_ordinal").reset_index(drop=True)
    rt = pd.to_datetime(rpi["rpi_time"]).to_numpy(dtype="datetime64[ns]")
    last_j = -1
    rows = []
    for _, m in ml.iterrows():
        mt = np.datetime64(pd.Timestamp(m["corrected_ml_time"]), "ns")
        if last_j + 1 >= len(rpi):
            rows.append((m, None, "exhausted_rpi"))
            continue
        diffs = np.abs((rt[last_j+1:] - mt).astype("timedelta64[ns]").astype(np.int64)) / 1e9
        jrel = int(np.argmin(diffs))
        j = last_j + 1 + jrel
        if max_gap_s is not None and diffs[jrel] > max_gap_s:
            rows.append((m, None, f"no_rpi_within_{max_gap_s:g}s"))
            continue
        rows.append((m, rpi.iloc[j], "matched"))
        last_j = j
    return rows


def manual_match(ml, rpi, path):
    mm = read_csv(path)
    required = ["events_mark_id", "rpi_mark_id"]
    missing = [c for c in required if c not in mm]
    if missing:
        raise KeyError(f"Manual match file missing {missing}")
    ml_idx = ml.set_index("ml_mark_id", drop=False)
    rpi_idx = rpi.set_index("rpi_mark_id", drop=False)
    rows = []
    for _, x in mm.iterrows():
        mid, rid = str(x["events_mark_id"]), str(x["rpi_mark_id"])
        if mid not in ml_idx.index:
            raise KeyError(f"Manual match references unknown ML mark: {mid}")
        if rid not in rpi_idx.index:
            raise KeyError(f"Manual match references unknown RPi mark: {rid}")
        rows.append((ml_idx.loc[mid], rpi_idx.loc[rid], "matched"))
    return rows, mm


def make_output(rows, mode, manual_df=None):
    out = []
    manual_lookup = {}
    if manual_df is not None:
        for _, r in manual_df.iterrows():
            manual_lookup[(str(r["events_mark_id"]), str(r["rpi_mark_id"]))] = r

    for k, (m, r, reason) in enumerate(rows, start=1):
        base = m.to_dict()
        rid = "" if r is None else str(r["rpi_mark_id"])
        rtime = pd.NaT if r is None else pd.Timestamp(r["rpi_time"])
        matched = r is not None
        raw = (
            (rtime - pd.Timestamp(m["corrected_ml_time"])).total_seconds()
            if matched else np.nan
        )
        md = manual_lookup.get((str(m["ml_mark_id"]), rid))
        pair_ex = bool(md.get("exclude", False)) if md is not None else False
        pair_reason = str(md.get("reason", "") or "") if md is not None else ""
        rpi_ex = bool(r.get("rpi_excluded", False)) if r is not None else False
        rpi_reason = str(r.get("rpi_exclusion_reason", "") or "") if r is not None else ""
        ml_ex = bool(m.get("ml_excluded", False))
        ml_reason = str(m.get("ml_exclusion_reason", "") or "")
        effective = ml_ex or rpi_ex or pair_ex
        reasons = [x for x in [ml_reason, rpi_reason, pair_reason] if x and x != "nan"]

        out.append({
            **base,
            "match_mode": mode,
            "match_id": f"{mode}_{k:04d}",
            "rpi_mark_id": rid,
            "rpi_time": rtime,
            "rpi_ordinal": np.nan if r is None else r.get("rpi_ordinal", np.nan),
            "rpi_timestamp_source": "" if r is None else r.get("rpi_timestamp_source", ""),
            "raw_offset_s": raw,
            "matched": matched,
            "match_reason": reason,
            "pair_excluded": pair_ex,
            "pair_exclusion_reason": pair_reason,
            "effective_excluded": effective,
            "effective_exclusion_reason": " | ".join(reasons),
        })
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser(description="Build standardized manual or automatic mark matches.")
    ap.add_argument("--master-dir", required=True)
    ap.add_argument("--mode", required=True, choices=["manual", "automatic"])
    ap.add_argument("--manual-matches", default="")
    ap.add_argument("--max-match-gap-s", type=float, default=1.0)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    master = Path(args.master_dir)
    ml = read_csv(master / "canonical_ml_marks.csv")
    rpi = read_csv(master / "canonical_rpi_marks.csv")
    ml["corrected_ml_time"] = parse_times(ml["corrected_ml_time"], "corrected_ml_time")
    rpi["rpi_time"] = parse_times(rpi["rpi_time"], "rpi_time")

    if args.mode == "manual":
        if not args.manual_matches:
            raise SystemExit("--manual-matches required for manual mode")
        rows, mm = manual_match(ml, rpi, args.manual_matches)
        out = make_output(rows, "manual", mm)
    else:
        rows = automatic_match(ml, rpi, args.max_match_gap_s)
        out = make_output(rows, "automatic")

    od = Path(args.out_dir)
    write_csv(out, od / f"matched_marks_{args.mode}.csv")
    write_json({
        "match_mode": args.mode,
        "n_ml_marks": len(ml),
        "n_rpi_marks": len(rpi),
        "n_matched": int(out["matched"].sum()),
        "n_effectively_excluded": int(out["effective_excluded"].sum()),
        "max_match_gap_s": args.max_match_gap_s if args.mode == "automatic" else None,
    }, od / f"matching_summary_{args.mode}.json")
    print(f"[ok] {args.mode} matches -> {od}")


if __name__ == "__main__":
    main()
