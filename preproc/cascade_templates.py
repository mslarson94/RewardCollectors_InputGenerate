# Canonical cascade structures for PinDrop and ChestOpen workflows

CANONICAL_CASCADE_TEMPLATES = {
    "pinDrop": {
        "sequence": [
            {"type": "WalkingPeriod", "tag": "walk2pinDrop"},
            {"type": "PinDrop", "tag": "pinDrop"},
            {"type": "GrayPin_Visible_start", "tag": "pinStartVisual"},
            {"type": "Feedback_textNcolor_Visible_start", "tag": "wait4feedback"},
            {"type": "Feedback_textNcolor_Visible_end", "tag": "feedbackComplete"},
            {"type": "Coin_Visible_start", "tag": "coinVisible", "optional": True},
            {"type": "Coin_Released", "tag": "coinReleased", "optional": True}
        ],
        "typical_durations": {
            "Feedback_textNcolor_Visible_end": 3.0,
            "Coin_Released": 4.0
        }
    },
    "chestOpen": {
        "sequence": [
            {"type": "WalkingPeriod", "tag": "walk2chest"},
            {"type": "ChestOpen", "tag": "chestOpen"},
            {"type": "Coin_Visible_start", "tag": "wait4coin"},
            {"type": "Coin_Collected", "tag": "coinCollect"}
        ],
        "typical_durations": {
            "Coin_Visible_start": 2.0,
            "Coin_Collected": 4.0
        }
    }
}

'''
📦 What’s cascade_templates.py For?
The primary purpose of canonical_templates.py is to codify what a complete, well-formed cascade looks like — both for developers and automated validation tools.

🔧 Utility Purposes:
Documentation for future devs: “What does a PinDrop cascade really contain?”

Validation: You can compare extracted cascades against this structure to:

Detect missing or misordered events

Test log integrity

Inference Templates: If a field is missing, the typical durations can help synthesize timing.

Consistency Checking across legacy → modern formats.
'''