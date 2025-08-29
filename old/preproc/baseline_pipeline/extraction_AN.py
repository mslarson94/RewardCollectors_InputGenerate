'''
extraction_AN.py

Myra Saraí Larson 
08/06/2025
used to extract data from metadata json files, events .csv files, and raw behavioral files 
'''
import pandas as pd
import json
from pathlib import Path

def extract_summary_stats(meta_path, events_path):
    # Load metadata
    with open(meta_path, 'r') as f:
        meta = json.load(f)

    # Load events
    df_events = pd.read_csv(events_path)

    participant_id = meta.get('participantID')
    blocks = meta['BlockStructureSummary']

    summary = {
        'participant_id': participant_id,
        'total_blocks': len(blocks),
        'complete_blocks': sum(1 for b in blocks if b['BlockStatus'] == 'complete'),
        'truncated_blocks': sum(1 for b in blocks if b['BlockStatus'] == 'truncated'),
        'total_duration_sec': sum(b['BlockDuration_sec'] for b in blocks),
        'total_true_rounds': sum(b['NumTrueRounds'] for b in blocks),
        'avg_block_duration_sec': sum(b['BlockDuration_sec'] for b in blocks) / len(blocks),
    }

    # Behavior event stats
    event_counts = df_events['hi_eventType'].value_counts().to_dict()
    event_durations = df_events.groupby('hi_eventType')['duration_sec'].sum().to_dict() if 'duration_sec' in df_events.columns else {}

    summary.update({
        'event_counts': event_counts,
        'event_durations_sec': event_durations
    })

    # Optional: time from pin_drop to coin_collect
    pin_to_coin_times = []
    pins = df_events[df_events['hi_eventType'] == 'pin_drop']
    coins = df_events[df_events['hi_eventType'] == 'coin_collect']
    
    if not pins.empty and not coins.empty:
        # Align by nearest next coin_collect after pin_drop
        pin_times = pd.to_datetime(pins['timestamp']).reset_index(drop=True)
        coin_times = pd.to_datetime(coins['timestamp']).reset_index(drop=True)
        
        min_len = min(len(pin_times), len(coin_times))
        for i in range(min_len):
            delta = coin_times[i] - pin_times[i]
            pin_to_coin_times.append(delta.total_seconds())

    if pin_to_coin_times:
        summary['avg_pin_to_coin_sec'] = sum(pin_to_coin_times) / len(pin_to_coin_times)
        summary['num_valid_pin_to_coin'] = len(pin_to_coin_times)


    return summary


# Example usage
if __name__ == "__main__":
    participant = 'A'  # or 'B'
    meta_file = f'ObsReward_{participant}_02_17_2025_15_11_processed_meta.json'
    event_file = f'ObsReward_{participant}_02_17_2025_15_11_processed_events.csv'
    
    stats = extract_summary_stats(meta_file, event_file)
    for k, v in stats.items():
        print(f"{k}: {v}")
