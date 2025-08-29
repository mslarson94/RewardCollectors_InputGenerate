
from collections import defaultdict

# Define expected lo_eventType sequences for each hi_eventType cascade
CASCADE_SCHEMAS = {
    "PinDrop": {
        "required": {
            "PinDrop_Moment",
            "FeedbackTextVis_start",
            "CoinCollect_Moment_PinDrop"
        }
    },
    "ChestOpen": {
        "required": {
            "ChestOpen_Moment",
            "CoinCollect_Moment_Chest"
        }
    },
    "SwapVote": {
        "required": {
            "SwapVoteMoment",
            "SwapVoteText_end",
            "BlockScoreText_start"
        }
    }
}

def validate_cascades(events):
    """
    Validates each cascade_id group against the expected schema.
    Returns a report of errors and compliance.
    """
    from collections import defaultdict

    cascades = defaultdict(list)
    for e in events:
        if e.get("cascade_id") is not None:
            cascades[e["cascade_id"]].append(e)

    results = []
    for cid, group in cascades.items():
        hi_type = group[0].get("hi_eventType", "Unknown")
        expected = CASCADE_SCHEMAS.get(hi_type, {}).get("required", set())
        actual = set(e["lo_eventType"] for e in group if "lo_eventType" in e)

        missing = expected - actual
        extra = actual - expected

        results.append({
            "cascade_id": cid,
            "hi_eventType": hi_type,
            "missing": list(missing),
            "unexpected": list(extra)
        })

    return results
