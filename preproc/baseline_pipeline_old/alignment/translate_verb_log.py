# translate_verb_log.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Translate a *_verb.log into a flat CSV with columns:
[ipAddress, markNumber, ML_Time, RPi_Time, Mono_Time, Mono_Time_Adj]

Grouping:
- Each "[IP]" line starts a new Mark for that IP.
- Following triplets (ML_Time, RPi_Time, Mono_Time) belong to that Mark until a blank line or next header.

Adjustment (timezone alignment to RPi):
- Default: If the FIRST row in a mark has hour(ML_Time) == hour(RPi_Time), compute dh = hour(RPi_Time) - hour(Mono_Time)
  (wrapped to [-12,+11]) and shift ALL Mono times in that mark by dh hours (mod 24).
- With --force-if-ml-rpi-mismatch: always compute/shift using RPi vs Mono (when hours parse), even if ML/​RPi hours differ.
- If hours cannot be parsed for the first row, Mono_Time_Adj == Mono_Time for that mark.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

HEADER_RE = re.compile(r"^\[(\d{1,3}(?:\.\d{1,3}){3})\]\s*$")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Translate *_verb.log into CSV rows grouped by Mark/IP blocks.")
    p.add_argument("--verb-log", required=True, help="Path to input *_verb.log")
    p.add_argument("--out-csv", default="", help="Optional output CSV path; defaults to <verb-log>_verb_full.csv")
    p.add_argument("--debug", action="store_true", help="Print parsing diagnostics")
    p.add_argument(
        "--force-if-ml-rpi-mismatch",
        action="store_true",
        help="Always align Mono to RPi hour offset even when ML and RPi hours differ",
    )
    return p.parse_args()


def tokenize_triplet(line: str) -> Tuple[str, str, str] | None:
    """Tolerate commas/tabs/spaces."""
    toks = [t for t in re.split(r"[,\s]+", line.strip()) if t]
    if len(toks) < 3:
        return None
    return toks[0], toks[1], toks[2]


def _hour(t: str) -> int | None:
    """Parse hour from 'H[H]:MM:SS(.ffffff)'."""
    m = re.match(r"^(\d{1,2}):\d{2}:\d{2}(?:\.\d+)?$", t.strip())
    return int(m.group(1)) if m else None


def _shift_hours(t: str, dh: int) -> str:
    """Shift time by whole hours, modulo 24. Keep minutes/seconds/fraction."""
    m = re.match(r"^(\d{1,2}):(\d{2}:\d{2}(?:\.\d+)?)$", t.strip())
    if not m:
        return t  # keep original if malformed
    hh = int(m.group(1))
    new_h = (hh + dh) % 24
    return f"{new_h:02d}:{m.group(2)}"


def _wrap_hour_diff(diff: int) -> int:
    """Normalize raw diff into [-12, +11] for minimal wrap."""
    return ((diff + 12) % 24) - 12


def translate_verb_log(path: Path, debug: bool = False) -> pd.DataFrame:
    rows: List[Tuple[str, int, str, str, str]] = []
    mark_counters: Dict[str, int] = {}
    current_ip: str | None = None

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for ln, raw in enumerate(f, start=1):
            line = raw.rstrip("\r\n")

            # Header: [IP]
            m = HEADER_RE.match(line.strip())
            if m:
                ip_literal = f"[{m.group(1)}]"  # keep brackets
                current_ip = ip_literal
                mark_counters[ip_literal] = mark_counters.get(ip_literal, 0) + 1
                if debug:
                    print(f"[{ln}] NEW MARK ip={ip_literal} markNumber={mark_counters[ip_literal]}")
                continue

            # Blank line ends current bundle
            if not line.strip():
                if debug and current_ip is not None:
                    print(f"[{ln}] END MARK ip={current_ip}")
                current_ip = None
                continue

            # Data only valid inside a mark
            if current_ip is None:
                if debug:
                    print(f"[{ln}] WARN: data line outside a mark; skipping: {line!r}")
                continue

            triplet = tokenize_triplet(line)
            if not triplet:
                if debug:
                    print(f"[{ln}] WARN: malformed triplet; skipping: {line!r}")
                continue

            ml, rpi, mono = triplet
            rows.append((current_ip, mark_counters[current_ip], ml, rpi, mono))
            if debug:
                print(f"[{ln}] row -> ip={current_ip} mark={mark_counters[current_ip]} ML={ml} RPi={rpi} Mono={mono}")

    df = pd.DataFrame(
        rows,
        columns=["ipAddress", "markNumber", "ML_Time_verb", "RPi_Time_verb", "Mono_Time_Raw_verb"],
    )

    # Adjust Mono_Time_verb per (ipAddress, markNumber) block:
    def _adjust_group(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        first = g.iloc[0]
        ml_h = _hour(first["ML_Time_verb"])
        rpi_h = _hour(first["RPi_Time_verb"])
        mono_h = _hour(first["Mono_Time_Raw_verb"])

        apply = False
        if rpi_h is not None and mono_h is not None:
            if debug:
                print(f"[adjust] IP={first['ipAddress']} mark={first['markNumber']} "
                      f"ML_h={ml_h} RPi_h={rpi_h} Mono_h={mono_h}")
            # Only adjust when ML and RPi hours match (both parsed)
            apply = ml_h is not None and ml_h == rpi_h

        if apply:
            dh = _wrap_hour_diff(rpi_h - mono_h)  # type: ignore[arg-type]
            if debug:
                print(f"[adjust] applying dh={dh} to Mono times for IP={first['ipAddress']} mark={first['markNumber']}")
            g["Mono_Time_verb"] = g["Mono_Time_Raw_verb"].map(lambda t: _shift_hours(t, dh))
        else:
            g["Mono_Time_verb"] = g["Mono_Time_Raw_verb"]

        return g

    # df = df.groupby(["ipAddress", "markNumber"], as_index=False, sort=False).apply(
    #     _adjust_group, include_groups=False
    # )
    df = _adjust_group(df)

    return df


def main() -> None:
    args = parse_args()
    in_path = Path(args.verb_log)
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    out_path = Path(args.out_csv) if args.out_csv else in_path.with_suffix(in_path.suffix + "_verb_full.csv")
    df = translate_verb_log(in_path, debug=args.debug)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[ok] wrote {len(df)} rows -> {out_path}")


if __name__ == "__main__":
    main()
