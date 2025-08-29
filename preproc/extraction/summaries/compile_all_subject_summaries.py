import pandas as pd
import json
from pathlib import Path

# ===== USER INPUTS =====
root_dir = "/Users/mairahmac/Desktop/RC_TestingNotes"
base_dir = Path(root_dir) / "ResurrectedData"
events_dir = base_dir / "Events_AugmentedPart4"
meta_dir = base_dir / "MetaData_Flat"
output_dir = base_dir / "Summary"
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / "all_subjects_summary.csv"

# ===== MATCH FILE PAIRS =====
meta_files = {f.stem.replace("_processed_meta", ""): f for f in meta_dir.glob("*_processed_meta.json")}
event_files = {f.stem.replace("_events_with_walks", ""): f for f in events_dir.glob("*_events_with_walks.csv")}
matched_keys = set(meta_files) & set(event_files)

print(f"🔍 Found {len(matched_keys)} matched file pairs to process.")

summaries = []

# ===== PROCESS EACH MATCHED FILE PAIR =====
for key in sorted(matched_keys):
    events_file = event_files[key]
    meta_file = meta_files[key]

    try:
        events_df = pd.read_csv(events_file)
        with open(meta_file) as f:
            meta_data = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load files for {key}: {e}")
        continue

    try:
        # === Extract currentRole from events file ===
        if "currentRole" in events_df.columns:
            current_role = events_df["currentRole"].dropna().iloc[0]
        else:
            current_role = "unknown"

        # === Identifiers (use current_role from events) ===
        identifiers = {
            "participantID": meta_data.get("participantID"),
            "pairID": meta_data.get("pairID"),
            "testingDate": meta_data.get("testingDate"),
            "sessionType": meta_data.get("sessionType"),
            "ptIsAorB": meta_data.get("main_RR"),
            "coinSet": meta_data.get("coinSet"),
            "device": meta_data.get("device"),
            "currentRole": current_role,
            "source_file": key
        }

        num_blocks_completed = len([
            b for b in meta_data['BlockStructureSummary']
            if b.get('BlockStatus') == 'complete'
        ])

        # === Total Points Earned (only if currentRole ≠ PO) ===
        if current_role == "PO":
            total_points_earned = "not available"
        else:
            total_points_earned = events_df["currGrandTotal"].dropna().iloc[-1]

        # === Total Session Time ===
        total_session_time = events_df["SessionElapsedTime"].dropna().iloc[-1]

        # === Average round time ===
        events_filtered = events_df[
            ((events_df["lo_eventType"] == "RoundEnd") & (events_df["BlockNum"].isin([1, 3]))) |
            ((events_df["lo_eventType"] == "BlockEnd") & (~events_df["BlockNum"].isin([1, 3])))
        ]
        round_times = events_filtered.apply(
            lambda row: row["RoundElapsedTime"] if row["lo_eventType"] == "RoundEnd" else row["BlockElapsedTime"],
            axis=1
        )
        average_round_time = round_times.mean()

        # === Swap votes (if present) ===
        if "SwapVote" in events_df.columns and "SwapVoteScore" in events_df.columns:
            swap_votes = events_df[events_df["SwapVote"].notna()]
            num_swap_votes = len(swap_votes)
            num_correct_swaps = (swap_votes["SwapVoteScore"] == "Correct").sum()
            pct_correct_swap_votes = (num_correct_swaps / num_swap_votes * 100) if num_swap_votes > 0 else None
        else:
            num_swap_votes = "no TP2 detected"
            num_correct_swaps = "no TP2 detected"
            pct_correct_swap_votes = "no TP2 detected"

        if "pinDropVote" in events_df.columns:
            pinDropVote = events_df[events_df["pinDropVote"].notna()]
            num_pinDropVote = len(pinDropVote)
            num_correct_pinDropVote = (pinDropVote["pinDropVote"] == "CORRECT").sum()
            pct_correct_pinDropVote = (num_correct_pinDropVote / num_pinDropVote * 100) if num_pinDropVote > 0 else None
        else:
            num_pinDropVote = "not AN"
            num_correct_pinDropVote = "not AN"
            pct_correct_pinDropVote = "not AN"

        # === Rounds to criterion (safe detection of TP1) ===
        criterion_df = events_df[(events_df["totalRounds"] > 1) & (events_df["CoinSetID"] == 1)]
        if not criterion_df.empty:
            rounds_to_criterion_count = criterion_df["totalRounds"].iloc[0]
        else:
            rounds_to_criterion_count = "no TP1 detected"

        # === Final summary ===
        summary = {
            **identifiers,
            "Total Blocks Completed": num_blocks_completed,
            "Files Generated": 1,
            "Total Points Earned": total_points_earned,
            "Total Session Time (sec)": total_session_time,
            "Average Round Time (sec)": average_round_time,
            "Number of Total Swap Votes": num_swap_votes,
            "Number of Correct Swap Votes": num_correct_swaps,
            "% Correct Swap Votes": pct_correct_swap_votes,
            'Number of PinDropVotes': num_pinDropVote,
            "Number of Correct PinDropVotes": num_correct_pinDropVote,
            "% Correct PinDropVotes": pct_correct_pinDropVote,
            "Rounds to Criterion": rounds_to_criterion_count
        }

        summaries.append(summary)
    except Exception as e:
        print(f"⚠️ Error processing {key}: {e}")
        continue


# ===== SAVE OUTPUT =====
summary_df = pd.DataFrame(summaries)
summary_df.to_csv(output_path, index=False)
print(f"✅ Summary saved to {output_path}")
