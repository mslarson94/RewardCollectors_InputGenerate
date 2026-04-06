import pandas as pd
import numpy as np
import json
import re
import ast
from pathlib import Path
import os
import argparse


# ---- details parsing ---------------------------------------------------------
def parse_details(details):
    """
    Parse the 'details' column which may be:
      - a python-literal string dict like "{'k': 1, 'coinPos': (x y z)}"
      - a 'key: val | key2: (x y z)' pipe-delimited string
    Produces flat columns; 3D tuples become *_x, *_y, *_z.
    """
    # Fast path: python-literal dict as string
    if isinstance(details, str) and details.startswith("{") and "'" in details:
        try:
            val = ast.literal_eval(details)
            # normalize any 3D tuple-ish items into _x/_y/_z keys
            out = {}
            for k, v in val.items():
                if isinstance(v, (tuple, list)) and len(v) == 3:
                    out[f"{k}_x"], out[f"{k}_y"], out[f"{k}_z"] = float(v[0]), float(v[1]), float(v[2])
                else:
                    out[k] = v
            return out
        except Exception:
            # fall through to pipe-delimited parsing
            pass

    if isinstance(details, str):
        result = {}
        pairs = [seg.strip() for seg in details.split('|') if ':' in seg]
        for pair in pairs:
            key, val = pair.split(':', 1)
            key = key.strip()
            val = val.strip()
            if re.match(r"\(.*\)", val):  # "(x y z)" form
                nums = re.findall(r"-?\d+\.?\d*", val)
                if len(nums) == 3:
                    result[f"{key}_x"] = float(nums[0])
                    result[f"{key}_y"] = float(nums[1])
                    result[f"{key}_z"] = float(nums[2])
            else:
                try:
                    result[key] = float(val) if '.' in val else int(val)
                except ValueError:
                    result[key] = val
        return result

    return {}


# ---- core flatten function ---------------------------------------------------
def flatten_events(events_path, meta_path, out_path, distance_threshold=None):
    """
    - Expands 'details' into columns.
    - Labels coins via nearest-neighbor in CoinRegistry per CoinSetID.
    - Writes a single flattened CSV.
    """
    with open(meta_path, 'r') as f:
        meta = json.load(f)

    df = pd.read_csv(events_path)

    # Expand details
    details_expanded = df['details'].apply(parse_details).apply(pd.Series)
    df = pd.concat([df.drop(columns='details'), details_expanded], axis=1)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"✅ Flattened and annotated events written to {out_path}")


# ---- batch driver ------------------------------------------------------------

def batch_flatten_events(base_dir, distance_threshold=None, events_dir='Events_Flat_csv', meta_dir = 'MetaData_Flat',out_dir='Events_Flattened', eventEnding='processed_events', metaEnding = 'processed_meta', outEnding='eventsFlat'):
    base_dir   = Path(base_dir)
    events_dir = base_dir / "full" / events_dir
    meta_dir   = base_dir / "full" / meta_dir
    output_dir = base_dir / "full" / out_dir

    # make sure directories exist
    for d in (events_dir, meta_dir, output_dir):
        d.mkdir(parents=True, exist_ok=True)

    print(events_dir)

    meta_files  = {p.stem.replace(f"_{metaEnding}", ""): p
                   for p in meta_dir.glob(f"*_{metaEnding}.json")}
    print(meta_files)
    event_files = {p.stem.replace(f"_{eventEnding}", ""): p
                   for p in events_dir.glob(f"*_{eventEnding}.csv")}
    print(event_files)

    matched_keys = sorted(set(meta_files) & set(event_files))
    print(f"🔍 Found {len(matched_keys)} matched file pairs to process.")

    for key in matched_keys:
        meta_path   = meta_files[key]
        events_path = event_files[key]
        out_path    = output_dir / f"{key}_{outEnding}.csv"
        print(f"➡️ Processing pair: {events_path.name} & {meta_path.name}")
        flatten_events(events_path, meta_path, out_path, distance_threshold=distance_threshold)


# if __name__ == "__main__":
#     trueRootDir = '/Users/mairahmac/Desktop/RC_TestingNotes'
#     #eventsDir = 'full/Events_Flat_csv'

#     procDir = 'FreshStart'
    
#     base_dir = os.path.join(trueRootDir, procDir)
#     print(base_dir)


#     #events_dir = os.path.join(base_dir, eventsDir)  # aligned, augmented events
#     #print(events_dir)
#     #meta_dir = os.path.join(base_dir, "MetaData_Flat")
#     #output_dir = os.path.join(base_dir, "Events_AugPart1")

#     print("🚀 Starting batch flatten...")
#     # Optionally set a tolerance (in meters) to drop dubious matches:
#     # e.g., distance_threshold=0.75
#     batch_flatten_events(base_dir, distance_threshold=None)

def cli() -> None:
    parser = argparse.ArgumentParser(
        prog="justFlatten",
        description="Flatten the 'details' dict column in *_events.csv files."
    )
    parser.add_argument(
        "--root-dir", required=True, type=Path,
        help="Base project directory (e.g., '/Users/you/RC_TestingNotes')."
    )
    parser.add_argument(
        "--proc-dir", required=True, type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). If absolute, --root-dir is ignored."
    )
    parser.add_argument(
        "--distance-threshold", type=float, default=None,
        help="Optional tolerance (meters) to drop dubious matches, e.g., 0.75."
    )

    parser.add_argument(
        "--events-dir", default='Events_Flat_csv', type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). If absolute, --root-dir is ignored."
    )

    parser.add_argument(
        "--meta-dir", default='MetaData_Flat', type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). If absolute, --root-dir is ignored."
    )

    parser.add_argument(
        "--out-dir", default='Events_Flattened', type=Path,
        help="Dataset subdirectory under --root-dir (e.g., 'FreshStart'). If absolute, --root-dir is ignored."
    )

    parser.add_argument(
        "--eventEnding", default='processed_events', type=str,
        help="file name ending pattern for source event files - excluding the leading '_' and trailing file extension"
    )

    parser.add_argument(
        "--metaEnding", default='processed_meta', type=str,
        help="file name ending pattern for meta.json files - excluding the leading '_' and trailing file extension"
    )

    parser.add_argument(
        "--outEnding", default='eventsFlat', type=str,
        help="file name ending pattern for the output flattened event files - excluding the leading '_' and trailing file extension"
    )

    args = parser.parse_args()

    root = args.root_dir.expanduser()
    proc = args.proc_dir
    base_dir = proc if proc.is_absolute() else (root / proc)

    if not base_dir.exists():
        parser.error(f"Data root not found: {base_dir}")

    print("🚀 Starting batch flatten...")
    
    batch_flatten_events(
        base_dir=str(base_dir),
        distance_threshold=args.distance_threshold,
        events_dir=args.events_dir,
        meta_dir=args.meta_dir,
        out_dir=args.out_dir,
        eventEnding=args.eventEnding,
        metaEnding=args.metaEnding,
        outEnding=args.outEnding
    )

if __name__ == "__main__":
    cli()


# Use it like:
# python justFlatten.py \
#   --root-dir "$TRUE_BASE_DIR" \
#   --proc-dir "$PROC_DIR" \
#   --distance-threshold 0.75
