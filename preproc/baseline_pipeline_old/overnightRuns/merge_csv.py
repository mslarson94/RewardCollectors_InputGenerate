#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys


def merge_csv(out_path: Path, inputs: list[Path]) -> None:
    if len(inputs) < 2:
        raise SystemExit("Need at least two input CSV files.")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    header: list[str] | None = None
    wrote_header = False

    with out_path.open("w", newline="", encoding="utf-8") as fout:
        writer = None

        for i, p in enumerate(inputs):
            with p.open("r", newline="", encoding="utf-8") as fin:
                reader = csv.reader(fin)
                file_header = next(reader, None)
                if file_header is None:
                    continue

                if header is None:
                    header = file_header
                    writer = csv.writer(fout)
                    writer.writerow(header)
                    wrote_header = True
                else:
                    if file_header != header:
                        raise SystemExit(
                            f"Header mismatch in {p}.\n"
                            f"Expected: {header}\n"
                            f"Got     : {file_header}"
                        )

                # write remaining rows
                assert writer is not None
                writer.writerows(reader)

    if not wrote_header:
        raise SystemExit("No data written (all files empty?).")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("inputs", nargs="+")
    args = ap.parse_args(argv)

    out = Path(args.output)
    ins = [Path(x) for x in args.inputs]
    for p in ins:
        if not p.exists():
            print(f"ERROR: missing input: {p}", file=sys.stderr)
            return 2

    merge_csv(out, ins)
    print(f"Wrote merged CSV: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
