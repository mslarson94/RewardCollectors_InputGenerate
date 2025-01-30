#loggerTranslations.py

## Specific Round
markTime = "Mark should happen if checked on terminal." 
# Is logged at the end of the previous block, but it is actually the start of the next block
# Always followed by whichCoinSet

whichCoinSet_start = "coinsetID"
# For example: "coinsetID:1 absolute and delta(local position)"
# 0: Never used, is always formatted as HV_1, HV_2, LV_1, LV_2, NV_1, NV_2
# 1: Initial Path
# 2: HV1_NV1 Swap
# 3: HV1_NV2 Swap
# 4: HV2_NV1 Swap
# 5: HV2_NV2 Swap
# 6: Tutorial Coins

readyStart = "Repositioned and ready to start block or round"

blockEnd = "finished current task"

## Initial Encoding 
chestOpened_IE_start = "Chest opened:" 
# value is 0-2 (tutorial) or 0-5 (actual) for the current coin collected in the round e.g. 3 / 5 = 4th coin collected out of 6

coinCollect_IE_start = "coin collected:" 
# value is 0-2 (tutorial) or 0-5 (actual) for the current coin collected in the round e.g. 3 / 5 = 4th coin collected out of 6

## Pin Drop
pinDrop = "Just dropped a pin." 
# Always followed by whereActualPin, closestCoinLoc, and pinScore

whereActualPin_start = "Dropped a new pin at "
# Gives the anchored position & self generated local pos
# For example: "Dropped a new pin at -3.354 -1.444 -1.506 localpos: -0.162 0.000 -5.915"

closestCoinLoc_start = "Closest location was "
# Gives the closest coin loc, actual distance, & verdict separated by | 
# Bad pindrop example: "Closest location was: (-3.49, -1.44, -1.39) | actual distance: 0.1827938 | good drop"
# Good pindrop example: "Closest location was: (-2.82, -1.44, -9.65) | actual distance: 1.360124 | bad drop"

pinScore_bad = "Dropped a bad pin"
pinScore_good = "Dropped a good pin"
# Gives the current round, current # of perfect rounds, value of coin, running round total, separated by | 
# For example: "Dropped a good pin|0|0|20.00|0.00"


skipCoinPoints_start = "coinpoint" # No clue what to do with this 
# For example: "coinpoint0:  x: -1.5437 y: -1.443987 z: -1.834341 deltax:-2 deltay:0 deltaz:-6"

collectFeedBack = "Collected feedback coin" 
# Gives the value of the coin & running total reward for this round
# For example: "Collected feedback coin:0.00 total reward: 0.00" 

whichFeedBackCollected = "Collected pin feedback coin: "
# value is 0-2 (tutorial) or 0-5 (actual) for the current coin collected in the round e.g. 3 / 5 = 4th coin collected out of 6

pinDrop_RoundEnd_start = "Finished pindrop round:" 
# Current round number within block, important for Test Phase 1 blocks 
# always preceeded by collectFeedBack; next 2 lines: pinDrop_RoundTotal & collectFeedBack

pinDrop_RoundTotal_start = "Finished a perfect round with:"
# Bad Round Example: "Finished a perfect round with:0.00 total reward: 340.00"
# Good Round Example: "Finished a perfect round with:45.00 total reward: 215.00"

roundVote_Swapped = "Active Navigator says it was a swaped round."
roundVote_NotSwapped = "Active Navigator says it was NOT a swaped round."


'''
   public List<block> taskBlocks;
    int block_i;
taskBlock_i

BlockNum
int BlockNumber
blocknumb

SetTaskBlock_i
'''