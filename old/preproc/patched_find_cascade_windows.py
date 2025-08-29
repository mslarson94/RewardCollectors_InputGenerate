
def find_cascade_windows_from_events(events):
    """
    Identifies the time windows of pinDrop or chestOpen cascades based on event sequences.
    Each window includes cascade_id, hi_eventType, hiMeta_eventType, and BlockType.
    """
    windows = []
    cascade_id = 0
    start_time, hi_event_type, hi_meta_type, block_type = None, None, None, None

    for event in sorted(events, key=lambda e: e["AppTime"]):
        lo_type = event.get("lo_eventType")

        if lo_type in {"PinDrop_Moment", "ChestOpen_Moment"}:
            cascade_id += 1
            start_time = event["AppTime"]
            hi_event_type = "PinDrop" if lo_type == "PinDrop_Moment" else "ChestOpen"
            hi_meta_type = "BlockActivity"
            block_type = event.get("BlockType", "Unknown")

        elif lo_type in {"FeedbackCoinCollectMoment_PinDrop", "CoinCollectMoment_IE"} and start_time is not None:
            windows.append({
                "cascade_id": cascade_id,
                "hi_eventType": hi_event_type,
                "hiMeta_eventType": hi_meta_type,
                "BlockType": block_type,
                "start": start_time,
                "end": event["AppTime"]
            })
            start_time, hi_event_type, hi_meta_type, block_type = None, None, None, None

    # Debugging summary
    from collections import Counter
    summary = Counter(w["hi_eventType"] for w in windows)
    print("🧪 Cascade Summary by hi_eventType:")
    for etype, count in summary.items():
        print(f"  - {etype}: {count} cascade(s)")

    return windows
