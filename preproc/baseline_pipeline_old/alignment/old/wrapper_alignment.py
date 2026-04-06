# wrapper_alignment.py

import subprocess
import argparse

def run(cmd):
    print(f"🚀 Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run alignment phase of the pipeline.")
    parser.add_argument("--dataDir", required=True, help="Root directory for your data files.")
    parser.add_argument("--metadata", required=True, help="Path to the collated metadata Excel file.")
    args = parser.parse_args()

    run(f"python preprocess_events_for_alignment.py --dataDir \"{args.dataDir}\"")
    run(f"python alignPO2AN_part1.py --dataDir \"{args.dataDir}\" --metadata \"{args.metadata}\"")
    run(f"python alignPO2AN_part2.py --dataDir \"{args.dataDir}\" --metadata \"{args.metadata}\"")
