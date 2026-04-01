'''
prelimConditLogit_learningAll.py

'''

import pandas as pd
from statsmodels.discrete.conditional_models import ConditionalLogit
import statsmodels.api as sm
import numpy as np

dataFile = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/decisionExpandedKnotted_L1.csv"
df = pd.read_csv(dataFile).copy()

dat = df[[
    "roundID", "chosen", "points", "idealDistance",
    "t_early_20", "t_late_20"
]].dropna().copy()

# make sure types are right
dat["roundID"] = dat["roundID"].astype(str)
dat["chosen"] = dat["chosen"].astype(int)

dat["points_x_early"] = dat["points"] * dat["t_early_20"]
dat["dist_x_early"]   = dat["idealDistance"] * dat["t_early_20"]
dat["points_x_late"]  = dat["points"] * dat["t_late_20"]
dat["dist_x_late"]    = dat["idealDistance"] * dat["t_late_20"]

X_learning = dat[[
    "points",
    "idealDistance",
    "points_x_early",
    "dist_x_early",
    "points_x_late",
    "dist_x_late"
]].astype(float)

res_learning = ConditionalLogit(
    dat["chosen"],
    X_learning,
    groups=dat["roundID"]
).fit()

print('\n'*3)
print('*'*25)
print('')
print("\nL3: Value + Distance * Learning")
print("ChosenPath ~ PathValue + Distance + PathValue*EarlyLearning + PathValue*LateLearning + Distance*EarlyLearning + Distance*LateLearning")
print(res_learning.summary())
print("\nL3 params")
print(res_learning.params)
print('')
print('*'*25)
print('\n'*3)