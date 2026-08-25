# wrapper_postprocessing.py

import subprocess
import argparse

def run(cmd):
    print(f"🧼 Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run post-muscle event augmentation and flattening.")
    parser.add_argument("--dataDir", required=True, help="Root directory for your data files.")
    args = parser.parse_args()

    run(f"python computeWalks.py --dataDir \"{args.dataDir}\"")
    run(f"python postCascadeAugment.py --dataDir \"{args.dataDir}\"")
    run(f"python batch_fill_logged_event_positions.py --dataDir \"{args.dataDir}\"")
    run(f"python flattenAndLabel.py --dataDir \"{args.dataDir}\"")
    run(f"python mergeWalks.py --dataDir \"{args.dataDir}\"")
