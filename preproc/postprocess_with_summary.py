
import os
import pandas as pd
from resolve_coin_ids_from_positions import resolve_coin_ids_from_positions
from dataConfigs_3Coins import CoinSet
from collections import Counter

def postprocess_events(input_dir, output_dir, summary_path="postprocess_summary.txt"):
    os.makedirs(output_dir, exist_ok=True)
    all_positions = set()
    match_counter = Counter()

    summary_lines = []

    for file in os.listdir(input_dir):
        if file.endswith("_events.json") or file.endswith("_events.csv"):
            input_path = os.path.join(input_dir, file)
            output_path = os.path.join(output_dir, file)

            try:
                if file.endswith(".json"):
                    df = pd.read_json(input_path, lines=True)
                else:
                    df = pd.read_csv(input_path)

                events = df.to_dict("records")
                enriched = resolve_coin_ids_from_positions(events, CoinSet)

                matched = sum(1 for e in enriched
                              if e.get("event_type") == "PinDrop" and "matchedCoinLabel" in e.get("details", {}))
                total = sum(1 for e in enriched if e.get("event_type") == "PinDrop")

                for e in enriched:
                    if e.get("event_type") == "PinDrop":
                        d = e.get("details", {})
                        x, z = d.get("pin_local_x"), d.get("pin_local_z")
                        if x is not None and z is not None:
                            all_positions.add((round(x, 1), round(z, 1)))

                match_counter[file] = (matched, total)

                out_df = pd.DataFrame(enriched)
                if file.endswith(".json"):
                    out_df.to_json(output_path, orient="records", lines=True)
                else:
                    out_df.to_csv(output_path, index=False)

                print(f"✓ Post-processed: {file}")

            except Exception as e:
                print(f"✗ Failed to process {file}: {e}")
                summary_lines.append(f"✗ Failed to process {file}: {e}")

    # Create summary
    summary_lines.append("Matched Coin Summary:")
    for file, (matched, total) in match_counter.items():
        summary_lines.append(f"{file}: {matched}/{total} PinDrops matched")

    summary_lines.append("Unique (x, z) Coin Positions Encountered:")
    for xz in sorted(all_positions):
        summary_lines.append(f"{xz}")

    summary_output = "\n".join(summary_lines)
    print(summary_output)

    with open(os.path.join(output_dir, summary_path), "w") as f:
        f.write(summary_output)

if __name__ == "__main__":
    # Example usage
    INPUT_DIR = "/path/to/existing/events"
    OUTPUT_DIR = "/path/to/postprocessed/events"
    postprocess_events(INPUT_DIR, OUTPUT_DIR)
