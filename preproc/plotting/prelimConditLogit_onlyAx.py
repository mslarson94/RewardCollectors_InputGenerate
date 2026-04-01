'''
prelimConditLogit_onlyAx.py

'''
import pandas as pd
import statsmodels.api as sm
from statsmodels.discrete.conditional_models import ConditionalLogit
import numpy as np

# load
dataFile = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/decisionExpandedKnotted_L1.csv"
df = pd.read_csv(dataFile).copy()

# keep only rows needed for the basic models
keep = ["roundID", "participantID", "alt", "chosen", "points", "idealDistance", "utility", "coinSet"]
dat = df[keep].dropna().copy()
dat = dat[dat["coinSet"].astype(str).str.strip().str.lower() == "ax"].copy()

# make sure types are right
dat["roundID"] = dat["roundID"].astype(str)
dat["alt"] = dat["alt"].astype(int)
dat["chosen"] = dat["chosen"].astype(int)
dat["coinSet"] = dat["coinSet"].astype("category")

print('*'*25)
print('*'*25)
print('')
print('Data in All Participants for CoinSet Ax')
print('')
print('*'*25)
print('*'*25)

# alt dummies for M0
alt_dummies = pd.get_dummies(dat["alt"], prefix="alt", drop_first=True).astype(float)

# M0: path identity only
X0 = alt_dummies
m0 = ConditionalLogit(dat["chosen"], X0, groups=dat["roundID"])
r0 = m0.fit()

# M1: value only
X1 = dat[["points"]].astype(float)
m1 = ConditionalLogit(dat["chosen"], X1, groups=dat["roundID"])
r1 = m1.fit()

#M2: distance only
X2 = dat[["idealDistance"]].astype(float)
m2 = ConditionalLogit(dat["chosen"], X2, groups=dat["roundID"])
r2 = m2.fit()

# M3: value + distance
X3 = dat[["points", "idealDistance"]].astype(float)
m3 = ConditionalLogit(dat["chosen"], X3, groups=dat["roundID"])
r3 = m3.fit()

# M4: fixed utility only
X4 = dat[["utility"]].astype(float)
m4 = ConditionalLogit(dat["chosen"], X4, groups=dat["roundID"])
r4 = m4.fit()

print('\n'*3)
print('*'*25)
print('')
print("\nM0: alt only")
print("ChosenPath ~ PathType")
print(r0.summary())
print("\nM0 params")
print(r0.params)
print('')
print('*'*25)
print('\n'*3)

print('\n'*3)
print('*'*25)
print('')
print("\nM1: value only")
print('ChosenPath ~ PathValue')
print(r1.summary())
print("\nM1 params")
print(r1.params)
print('')
print('*'*25)
print('\n'*3)


print('\n'*3)
print('*'*25)
print('')
print("\nM2: distance only")
print('ChosenPath ~  Distance')
print(r2.summary())
print("\nM2 params")
print(r2.params)
print('')
print('*'*25)
print('\n'*3)

print('\n'*3)
print('*'*25)
print('')
print("\nM3: value + distance")
print('ChosenPath ~ PathValue * Distance')
print(r3.summary())
print("\nM3 params")
print(r3.params)
print('')
print('*'*25)
print('\n'*3)


print('\n'*3)
print('*'*25)
print('')
print("\nM4: utility only")
print('ChosenPath ~ Utility (lambda = 1)')
print(r4.summary())
print("\nM4 params")
print(r4.params)
print('')
print('*'*25)
print('\n'*3)

# # M5: alt + value
# X5 = pd.concat([
#     pd.get_dummies(dat["alt"], prefix="alt", drop_first=True).astype(float),
#     dat[["points"]].astype(float)
# ], axis=1)
# r5 = ConditionalLogit(dat["chosen"], X5, groups=dat["roundID"]).fit()

# # M6: alt + value + distance
# X6 = pd.concat([
#     pd.get_dummies(dat["alt"], prefix="alt", drop_first=True).astype(float),
#     dat[["points", "idealDistance"]].astype(float)
# ], axis=1)
# r6 = ConditionalLogit(dat["chosen"], X6, groups=dat["roundID"]).fit()

# # M7: alt + utility
# X7 = pd.concat([
#     pd.get_dummies(dat["alt"], prefix="alt", drop_first=True).astype(float),
#     dat[["utility"]].astype(float)
# ], axis=1)
# r7 = ConditionalLogit(dat["chosen"], X7, groups=dat["roundID"]).fit()

def get_ic(result):
    llf = result.llf
    k = len(result.params)
    n = result.nobs
    aic = 2 * k - 2 * llf
    bic = np.log(n) * k - 2 * llf
    return llf, aic, bic, k, n

rows = []
for name, res in [
    ("M0_alt", r0),
    ("M1_points", r1),
    ("M2_distance", r2),
    ("M3_value_distance", r3),
    ("M4_utility", r4),
]:
    llf, aic, bic, k, n = get_ic(res)
    rows.append({
        "model": name,
        "nobs": n,
        "k": k,
        "logLik": llf,
        "AIC": aic,
        "BIC": bic,
    })

model_compare = pd.DataFrame(rows).sort_values("AIC")
print('\n'*3)
print('*'*25)
print('')
print('\nModel Comparisions')
print(model_compare.to_string(index=False))
print('')
print('*'*25)
print('\n'*3)
'''
For these models:

    points positive in M1 means higher-value options are more likely to be chosen.

    idealDistance negative in M2 means longer paths are less likely to be chosen.

    If both are meaningful in M2, that supports integration.

    utility positive in M3 means the fixed utility (1 meter for 1 point exchange) tracks choices.

    M1 beats M0 → value explains choice beyond route identity bias

    M2 beats M0 → distance explains choice beyond route identity bias

    M3 beats M1 → distance x value adds explanatory power beyond value alone

    M3 beats M2 → distance x value adds explanatory power beyond distance alone

    M4 vs M3 → does the fixed utility score summarize behavior about as well as separate value and distance?
'''