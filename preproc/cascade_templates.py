# Canonical cascade structures for PinDrop and ChestOpen workflows

"hi_eventType" = {

    "SwapVote": [
        {"mid_eventType": "SwapVote", 
            "lo_eventType": "SwapVoteMoment",
            "optional": False},

        {"mid_eventType": "FullFeedbackAnimation", 
            "lo_eventType": ("SwapVoteText_end", "BlockScoreText_start", "BlockScoreText_start")
            "optional": False},

        {"mid_eventType": "NonRewardDrivenNavigation", 
            "lo_eventType": ("InterRound_PostCylinderWalk", "InterBlock_Idle"),
            "optional": False},

        {"mid_eventType": "Infrastructure", 
            "lo_eventType": "Mark",
            "optional": True},

        ], 

    "PinDrop": [
        {"mid_eventType": "PinDrop", 
            "lo_eventType": "PinDropMoment",
            "optional": False},

        {"mid_eventType": "FullPinDropAnimation", 
            "lo_eventType": ("PinDropSound_start", "GrayPinVis_start", "PinDropSound_end","GrayPinVis_end", 
                                "Feedback_Sound_start", "FeedbackTextVis_start", "FeedbackPinColor_start",
                                "FeedbackTextVis_end", "FeedbackPinColor_end", "CoinVis_start",
                                "CoinPresentSound_start", "CoinPresentSound_end", "Coin_Released")
            "optional": False},

        {"mid_eventType": "FeedbackCoinCollect", 
            "lo_eventType": "FeedbackCoinCollectMoment",
            "optional": False},

        {"mid_eventType": "FullFeedbackCoinCollect_Animation", 
            "lo_eventType": ("CoinVis_end", "CoinValueTextVis_start", "CoinCollectSound_start",
                                "CoinCollectSound_end", "CoinValueTextVis_end"),
            "optional": False},

        {"mid_eventType": "NonRewardDrivenNavigation", 
            "lo_eventType": ("PreBlock_CylinderWalk", "InterRound_CylinderWalk",
                                "InterRound_PostCylinderWalk", "InterBlock_Idle"),
            "optional": False},

        {"mid_eventType": "RewardDrivenNavigation", 
            "lo_eventType": ("Walk2PinDrop", "Wait4Feedback"),
            "optional": False},

        {"mid_eventType": "Infrastructure", 
            "lo_eventType": "Mark",
            "optional": True},

        ], 

    "ChestOpen": [
        {"mid_eventType": "ChestOpen", 
            "lo_eventType": "ChestOpenMoment",
            "optional": False},
        {"mid_eventType": "FullChestOpenAnimation", 
            "lo_eventType": ("ChestOpenAnimation_start", "ChestOpenSound_start", 
                                "ChestOpenAnimation_end", "ChestOpenSound_end", 
                                "ChestOpenEmpty_start", "ChestOpenEmpty_end", "CoinVis_start", 
                                "CoinPresentSound_start", "CoinPresentSound_end","Coin_Released"),
            "optional": False},
        {"mid_eventType": "CoinCollect_IE", 
            "lo_eventType": "CoinCollectMoment_IE",
            "optional": False},

        {"mid_eventType": "FullCoinCollectIE_Animation", 
            "lo_eventType": ("CoinVis_end", "ChestVis_end", "CoinCollectSound_start", 
                                "CoinValueTextVis_start", "NextChestVisible", "CoinCollectSound_end", 
                                "CoinValueTextVis_end"),
            "optional": False},

        {"mid_eventType": "NonRewardDrivenNavigation", 
            "lo_eventType": ("PreBlock_CylinderWalk", "InterRound_CylinderWalk",
                                "InterRound_PostCylinderWalk", "InterBlock_Idle"),
            "optional": False},

        {"mid_eventType": "RewardDrivenNavigation", 
            "lo_eventType": ("Walk2PinDrop", "Wait4Feedback"),
            "optional": False},

        {"mid_eventType": "Infrastructure", 
            "lo_eventType": "Mark",
            "optional": True}
        ]
}

'''
📦 What’s cascade_templates.py For?
The primary purpose of canonical_templates.py is to codify what a complete, well-formed cascade looks like — both for developers and automated validation tools.

🔧 Utility Purposes:
Documentation for future devs: “What does a PinDrop cascade really contain?”

Validation: You can compare extracted cascades against this structure to:

Detect missing or misordered events

Test log integrity

Consistency Checking across legacy → modern formats.
'''