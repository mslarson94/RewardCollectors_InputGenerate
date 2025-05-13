
import os
import pandas as pd
from resolve_coin_ids_from_positions import resolve_coin_ids_from_positions
from dataConfigs_3Coins import CoinSet

def postprocess_events(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    for file in os.listdir(input_dir):
        if file.endswith("_events.json") or file.endswith("_events.csv"):
            input_path = os.path.join(input_dir, file)
            output_path = os.path.join(output_dir, file)

            try:
                if file.endswith(".json"):
                    df = pd.read_json(input_path, lines=True)
                else:
                    df = pd.read_csv(input_path)

                enriched = resolve_coin_ids_from_positions(df.to_dict("records"), CoinSet)
                out_df = pd.DataFrame(enriched)

                if file.endswith(".json"):
                    out_df.to_json(output_path, orient="records", lines=True)
                else:
                    out_df.to_csv(output_path, index=False)

                print(f"✓ Post-processed: {file}")
            except Exception as e:
                print(f"✗ Failed to process {file}: {e}")

if __name__ == "__main__":
    # Example usage
    INPUT_DIR = "/path/to/existing/events"
    OUTPUT_DIR = "/path/to/postprocessed/events"
    postprocess_events(INPUT_DIR, OUTPUT_DIR)
