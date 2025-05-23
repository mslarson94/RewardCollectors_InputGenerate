
"Task" = [list of "Blocks" that occured within a log file]
	"Blocks" can be either of the values in "BlockType", denoted by column "BlockNum" 
	"BlockType" - denoted by column "BlockType", consists of 2 values: "pinDrop" and "coinCollect"
	if "BlockType" == "PinDrop":
		Block = [PreBlockActivity, BlockActivity, SwapVote, PostBlockActivity, Marks]
		{"hiMeta_eventType": [PreBlockActivity, 
								BlockActivity, 
								SwapVote, 
								PostBlockActivity,
								Marks]}

		if "hiMeta_eventType" == PreBlockActivity:
			PreBlockActivity consists of a single "PreBlock_CylinderWalk" event (where "RoundNum" == 0)
			events = {"hiMeta_eventType": "PreBlockActivity",
					  "medMeta_eventType": "PreBlockActivity",
					  "hi_eventType": "PreBlockActivity",
					  "med_eventType": "PreBlockActivity",
					  "lo_eventType": "PreBlock_CylinderWalk",
					  "optional": False}
		
		elif "hiMeta_eventType" == BlockActivity:
			"BlockActivity" = [list of "Rounds"], Number of Rounds can range between 1 & n+3 
			"Round": [list of "PinDropCascade" medMeta_eventType events]
			if "medMeta_eventType" == "PinDropCascade":
				{"hi_eventType": "ActualPinDropProcess"
					{"med_eventType": "PinDrop", 
			            "lo_eventType": "PinDrop_Moment",
			            "optional": False}

			        {"med_eventType": "PinDrop_Animation", 
			            "lo_eventType": ("PinDropSound_start", "GrayPinVis_start", "PinDropSound_end","GrayPinVis_end", 
			                                "Feedback_Sound_start", "FeedbackTextVis_start", "FeedbackPinColor_start",
			                                "FeedbackTextVis_end", "FeedbackPinColor_end", "CoinVis_start",
			                                "CoinPresentSound_start", "CoinPresentSound_end", "Coin_Released")
			            "optional": False}

			        {"med_eventType": "RewardDriven_Navigation", 
		            	"lo_eventType": ("Walk2PinDrop"),
		            	"optional": False}
			     	},

			    {"hi_eventType": "FeedbackProcess" 
			    	{"med_eventType": "CoinCollect_PinDrop", 
			            "lo_eventType": "CoinCollect_Moment_PinDrop",
			            "optional": False}

			        {"med_eventType": "CoinCollect_Animation_PinDrop", 
			            "lo_eventType": ("CoinVis_end", "CoinValueTextVis_start", "CoinCollectSound_start",
			                                "CoinCollectSound_end", "CoinValueTextVis_end"),
			            "optional": False}

			        {"med_eventType": "RewardDriven_Navigation", 
			            "lo_eventType": ("Wait4Feedback"),
			            "optional": False}
			    	}

			    {"hi_eventType": "InterRound_Walk"
			        {"med_eventType": "NonRewardDriven_Navigation", 
			            "lo_eventType": ("InterRound_CylinderWalk", "InterRound_PostCylinderWalk"),
			            "optional": False}
			    	}
				},



		{"hiMeta_eventType": "SwapVote"
				"SwapVote" is an optional hiMeta_eventType
				"SwapVote" = [
					{"med_eventType": "SwapVote", 
			            "lo_eventType": "SwapVoteMoment"},

			        {"med_eventType": "FullSwapVoteAnimation", 
			            "lo_eventType": ("SwapVoteText_end", "BlockScoreText_start", "BlockScoreText_start")}
							
					]
				},

		{"hiMeta_eventType": "PostBlockActivity"
				"PostBlockActivity" = ["InterBlock_Idle", [list of "Marks", if any]] 
				}

			]
		
			

			"CoinCollect": [
				{"hi_eventType": "PreBlockActivity"
					"preBlockActivity"= ["PreBlock_CylinderWalk", [list of "Marks", if any]]
					},
				
				{"hiMeta_eventType": "BlockActivity"
					"BlockActivity" = [list of "ChestOpen"]
						"ChestOpen" = [
					        {"med_eventType": "ChestOpen", 
					            "lo_eventType": "ChestOpen_Moment",
					            "optional": False},
					        {"med_eventType": "ChestOpen_Animation", 
					            "lo_eventType": ("ChestOpenAnimation_start", "ChestOpenSound_start", 
					                                "ChestOpenAnimation_end", "ChestOpenSound_end", 
					                                "ChestOpenEmpty_start", "ChestOpenEmpty_end", "CoinVis_start", 
					                                "CoinPresentSound_start", "CoinPresentSound_end","Coin_Released"),
					            "optional": False},
					        {"med_eventType": "CoinCollect_Chest", 
					            "lo_eventType": "CoinCollect_Moment_Chest",
					            "optional": False},

					        {"med_eventType": "CoinCollect_Animation_Chest", 
					            "lo_eventType": ("CoinVis_end", "ChestVis_end", "CoinCollectSound_start", 
					                                "CoinValueTextVis_start", "NextChestVisible", "CoinCollectSound_end", 
					                                "CoinValueTextVis_end"),
					            "optional": False},

					        {"med_eventType": "NonRewardDriven_Navigation", 
					            "lo_eventType": ("InterRound_CylinderWalk", "InterRound_PostCylinderWalk"),
					            "optional": False},

					        {"med_eventType": "RewardDriven_Navigation", 
					            "lo_eventType": ("Walk2PinDrop", "Wait4Feedback"),
					            "optional": False}
					        ]
					},

				{"hi_eventType": "PostBlockActivity"
					"PostBlockActivity" = ["InterBlock_Idle", [list of "Marks", if any]] 
					}

				]
		
			}
