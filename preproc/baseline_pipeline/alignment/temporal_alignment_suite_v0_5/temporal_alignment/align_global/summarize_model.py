from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from temporal_alignment.common.io import read_csv, write_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-json", required=True)
    ap.add_argument("--diagnostics-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    model = json.loads(Path(args.model_json).read_text())
    d = read_csv(args.diagnostics_csv)
    od = Path(args.out_dir)
    od.mkdir(parents=True, exist_ok=True)

    pd.DataFrame([{
        "model_type": model["model_type"],
        "slope": model["slope"],
        "intercept_s": model["intercept_s"],
        "clock_skew_ppm": model["clock_skew_ppm"],
        "n_used": model["n_used"],
        **model["fit_metrics"],
    }]).to_csv(od/"global_affine_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(10,4))
    t = pd.to_datetime(d["corrected_ml_time"])
    x = (t - t.min()).dt.total_seconds()/60
    ax.scatter(x, d["raw_offset_s"].astype(float), label="raw offset")
    ax.plot(x, d["predicted_offset_s"].astype(float), label="fitted offset")
    ax.set_xlabel("Elapsed corrected ML time (min)")
    ax.set_ylabel("Offset (s)")
    ax.set_title("Global affine alignment")
    ax.grid(True, alpha=.3)
    ax.legend()
    fig.savefig(od/"global_affine_fit.png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] summarized global affine -> {od}")


if __name__ == "__main__":
    main()
