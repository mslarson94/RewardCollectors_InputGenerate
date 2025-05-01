hardCodedTimeline.py

## Display Times (in seconds)


#### Text & 2D images #### 

uiText = 2.0 
## uiText: Appears within task only to provide instructions (ex: 'Please Standby') or give feedback on individual pin drops/votes on pin drops. Gives instructions & feedback with respect to each participant (i.e. is not identical between AN & PO). Does *not* temporally overlap with coin presentation. 

rewardTotalText = 2.0 
## rewardTotalText: Appears only at the end of the task to show AN's current points Grand Total. Is identical between AN & PO 

rewardText = 2.0 
## rewardText: Appears only at the end of the task to show AN's total points earned for this block. Is identical between AN & PO 

rewardTempText = 2.0 
## rewardTempText: Appears upon pindrop feedback, temporally overlaps between pin feedback & coin presentaiton. Is identical between AN & PO. 

pinIcon = unset
## the pin icon is present on screen in the upper lefthand corner when AN is between pin drops. Its to ensure the participants know that the round is still not over & that AN still needs to drop another pin. Is identical between AN & PO


#### AR tokens ####

openingChest = 0.4 
## the time it takes for the treasure chest to open

delayedRevealTime = 2.0
## the time in which the treasure chest is open & empty 

time4pinfeedback = 2.0
## the time in which PO can submit their vote on AN's pin drop and when AN/PO are waiting for feedback on their pin drop/vote

feedbackCoinDelay = 2.0 
## time that the feedback items are visible (pin changed color, correct, expected value) 

coinLockedTime = 1.0
## time that coin is visible but not yet collectable



#### Sounds ####

pinDropSound = 0.403

chestOpenSound = 0.4 

coinPresSound_orig = 0.650

coinPresSound_swap = 0.650

correct = 0.201

error = 0.201

highValueCoin = 0.654

lowValueCoin = 0.654

nullValueCoin = 0.654
