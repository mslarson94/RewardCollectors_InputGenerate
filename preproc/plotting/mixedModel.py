import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
import numpy as np

## Within Participant Fatigue 

# # Compute within-participant residuals: TotalElapsedTime ~ BlockNum for each participant
# df["fatigue_resid_within"] = pd.NA

# for pid, sub in df.groupby("participant"):
#     # Need at least 2 non-NaN rows to fit a line
#     sub_valid = sub[["BlockNum", "TotalElapsedTime"]].dropna()
#     if len(sub_valid) < 2:
#         # Can't fit a regression: set residuals to NaN for this participant
#         df.loc[sub.index, "fatigue_resid_within"] = pd.NA
#         continue

#     X = sm.add_constant(sub_valid["BlockNum"])
#     y = sub_valid["TotalElapsedTime"]
#     ols_sub = sm.OLS(y, X).fit()

#     # Map residuals back to the original indices
#     df.loc[sub_valid.index, "fatigue_resid_within"] = ols_sub.resid
                                                                                                                                                                                                           
# # Convert to float (will keep NaNs)
# df["fatigue_resid_within"] = df["fatigue_resid_within"].astype(float)

dataFile = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/allIntervalData_L1.csv"

df = pd.read_csv(dataFile)


df = df[df["BlockType"].astype(str).str.strip().str.lower() != "collecting"]
coinset = pd.to_numeric(df["CoinSetID"], errors="coerce")  # float dtype with NaN for bad values
df = df[coinset.lt(4)] ## no coin sets that are greater than or equal to 4 (tutorial data)
# Make sure categorical predictors are categorical
df["participantID"] = df["participantID"].astype("category")
df["coinLabel"] = df["coinLabel"].astype("category")
df["coinSet"] = df["coinSet"].astype("category")
df["isSwap"] = df["isSwap"].astype("category")
df["main_RR"] = df["main_RR"].astype("category")
 

## Mixed Model (all 22 participants, no PVSS)

# ## Verbal Translation
# model_all = smf.mixedlm(
#     "pinDropLatency ~ coinLabel*BlockNum*Volatity + main_RR + AverageSpeedApproachToPin + coinLayout + isSwapRound",
#     data=df,
#     groups=df["participantID"],
#     re_formula="1 + main_RR + AverageSpeedApproachToPin + coinLayout + isSwapRound"
# )
needed = ["roundElapsed_s", "coinLabel", "TotSesh_runTot_RoundNum", "recentSwapRate_all", "main_RR", "WalkAvgSpeed", "coinSet", "isSwap", "participantID"]
offending_rows = df[df[needed].isna().any(axis=1)].copy()
offending_rows["missing_cols"] = offending_rows[needed].isna().apply(lambda row: ",".join(row.index[row]), axis=1)
offending_rows.to_csv("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/offending_rows_model_all.csv", index=False)

model_df = df.dropna(subset=needed).copy()
model_df.to_csv("/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/model_all_rows_used.csv", index=False)
# Visually 

print(model_df["coinSet"].value_counts(dropna=False))
print(model_df["coinLabel"].value_counts(dropna=False))
print(model_df["isSwap"].value_counts(dropna=False))
print(model_df["main_RR"].value_counts(dropna=False))
print(len(model_df))

coinset_str = model_df["coinSet"].astype(str).str.strip().str.lower()
model_df_A = model_df.loc[coinset_str == "a"].copy()
model_df = model_df.loc[coinset_str != "a"].copy()

model_all = smf.mixedlm(
    "roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet + isSwap",
    data=model_df,
    groups=model_df["participantID"],
    re_formula="1"
)
result_all = model_all.fit(reml=False, method="lbfgs")
print('\n'*5)
print('mixedlm: roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet + isSwap')
print('\n'*2)
print(result_all.summary())
# ## Volatity = swapRate for t-10 to t-1 trials   ==  recentSwapRate_all  ## 9 trials = 3 rounds

ols_all = smf.ols(
    "roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet + isSwap",
    data=model_df
).fit(cov_type="cluster", cov_kwds={"groups": model_df["participantID"]})
print('\n'*5)
print('ols: roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all.summary())




model_df2 = model_df[model_df["roundElapsed_s"] > 0]
model_df2["log_roundElapsed_s"] = np.log(model_df2["roundElapsed_s"])

ols_all2 = smf.ols(
    "log_roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet + isSwap",
    data=model_df2
).fit(cov_type="cluster", cov_kwds={"groups": model_df2["participantID"]})
print('\n'*5)
print('ols log transform round elapsed time: log_roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all2.summary())




ols_all3 = smf.ols(
    "roundFrac ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet + isSwap",
    data=model_df
).fit(cov_type="cluster", cov_kwds={"groups": model_df["participantID"]})
print('\n'*5)
print('roundFrac: roundFrac ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all3.summary())


## collinearity checks
print('\n'*5)
print('collinearity checks')
print('running total round num vs. recentSwapRate_all', model_df[["TotSesh_runTot_RoundNum", "recentSwapRate_all"]].corr())
print()


ols_all4 = smf.ols(
    "roundFrac ~ coinLabel*recentSwapRate_all + main_RR + coinSet + isSwap",
    data=model_df
).fit(cov_type="cluster", cov_kwds={"groups": model_df["participantID"]})
print('\n'*5)
print('roundFrac: roundFrac ~ coinLabel*recentSwapRate_all + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all4.summary())

ols_all4_A = smf.ols(
    "roundFrac ~ coinLabel*recentSwapRate_all + main_RR +  isSwap",
    data=model_df_A
).fit(cov_type="cluster", cov_kwds={"groups": model_df_A["participantID"]})
print('\n'*5)
print('Coin Set A: roundFrac: roundFrac ~ coinLabel*recentSwapRate_all + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all4_A.summary())

model_df['runningSwapRate'] = model_df['swapRate_t-1_all']
model_df_A['runningSwapRate'] = model_df_A['swapRate_t-1_all']
ols_all5 = smf.ols(
    "roundFrac ~ coinLabel*runningSwapRate + main_RR + coinSet + isSwap",
    data=model_df
).fit(cov_type="cluster", cov_kwds={"groups": model_df["participantID"]})
print('\n'*5)
print('roundFrac: roundFrac ~ coinLabel*runningSwapRate + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all5.summary())

ols_all5_A = smf.ols(
    "roundFrac ~ coinLabel*runningSwapRate + main_RR +  isSwap",
    data=model_df_A
).fit(cov_type="cluster", cov_kwds={"groups": model_df_A["participantID"]})
print('\n'*5)
print('Coin Set A: roundFrac: roundFrac ~ coinLabel*runningSwapRate + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all5_A.summary())

ols_all6 = smf.ols(
    "roundFrac ~ coinLabel*TotSesh_runTot_RoundNum + main_RR + coinSet + isSwap",
    data=model_df
).fit(cov_type="cluster", cov_kwds={"groups": model_df["participantID"]})
print('\n'*5)
print('roundFrac: roundFrac ~ coinLabel*TotSesh_runTot_RoundNum + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all6.summary())

ols_all6_A = smf.ols(
    "roundFrac ~ coinLabel*TotSesh_runTot_RoundNum + main_RR + isSwap",
    data=model_df_A
).fit(cov_type="cluster", cov_kwds={"groups": model_df_A["participantID"]})
print('\n'*5)
print('Coin Set A: roundFrac: roundFrac ~ coinLabel*TotSesh_runTot_RoundNum + main_RR +  coinSet + isSwap')
print('\n'*2)
print(ols_all6_A.summary())

ols_all7 = smf.ols(
    "roundFrac ~ coinLabel + main_RR + coinSet",
    data=model_df
).fit(cov_type="cluster", cov_kwds={"groups": model_df["participantID"]})
print('\n'*5)
print('roundFrac: roundFrac ~ coinLabel + main_RR +  coinSet')
print('\n'*2)
print(ols_all7.summary())

ols_all7_A = smf.ols(
    "roundFrac ~ coinLabel + main_RR ",
    data=model_df_A
).fit(cov_type="cluster", cov_kwds={"groups": model_df_A["participantID"]})
print('\n'*5)
print('roundFrac: roundFrac ~ coinLabel + main_RR +  coinSet')
print('\n'*2)
print(ols_all7_A.summary())
# ## orig
# model_all = smf.mixedlm(
#     "roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + WalkAvgSpeed + coinSet + isSwap",
#     data=model_df,
#     groups=model_df["participantID"],
#     re_formula="1"
# )



## Mixed Model (PVSS, 18 participants who have it)
df_pvss = model_df.dropna(subset="PVSS_TotalScore").copy()

model_pvss = smf.mixedlm(
    "roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + WalkAvgSpeed + coinSet + isSwap + PVSS_TotalScore",
    data=df_pvss,
    groups=df_pvss["participantID"],
    re_formula="1"
)

result_pvss = model_pvss.fit(reml=False, method="lbfgs")
print('\n'*5)
print('roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet + isSwap + PVSS_TotalScore')
print('\n'*2)
print(result_pvss.summary())


# model_groupChoiceUtility = smf.mixedlm(
#     "path_order_round ~ pathValue + ideal_distance + pathValue:TotSesh_runTot_RoundNum + ideal_distance:TotSesh_runTot_RoundNum + main_RR + coinSet",
#     data = group_df,
#     groups=data["participant"],
#     re_formula="1 + main_RR + coinSet"


'''

generating the path utility stuff 

/Desktop/myra_code/Python/RewardCollectors_InputGenerate/walkDataAnalysis/theoPaths_Classifiers/greedy_v2.py
    - generates the path utility with the ideal distances that were already calculated with the previous script assuming a lambda (distance weight) of 1 pt / 1 m
    - (distance minus points) util = distance_weight * dist - pts 
    - (points minus distance) util = pts - distance_weight * dist
        - how much points is this route worth given that it produces x points, has a distance weight of 1 point per 1 meter, and has a distance of y? 

'''

#     )

# model_idvChoiceUtility = smf.mixedlm(
#     "path_order_round ~ pathValue + ideal_distance + TotSesh_runTot_RoundNum + pathValue*TotSesh_runTot_RoundNum + ideal_distance*TotSesh_runTot_RoundNum + main_RR + coinSet",
#     data = group_df,
#     groups=df["participant"],
#     re_formula="1 + main_RR + coinSet"

#     )

## pts without PVSS : ["8000", "9000", "8888", "9999"]