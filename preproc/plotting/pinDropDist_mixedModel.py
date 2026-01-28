import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf


def pinDropModeling(df: pd.DataFrame, voi: str, randomEffects: List[str]):
    re_str = " + ".join(randomEffects)
    modelStr = f"{voi} ~ CoinType * RoundNum_tot + {re_str}"

    model = smf.mixedlm(
        modelStr,
        data=df,
        groups=df["participantID"],
        re_formula=f"1 + {re_str}",
    )
    return model

# Make sure categorical predictors are categorical
df["CoinType"] = df["CoinType"].astype("category")
df["coinSet"] = df["coinSet"].astype("category")
df["participantID"] = df["participantID"].astype("category")


## Within Speed Deviation
# Compute within-participantID residuals: SessionElapsedTime ~ RoundNum_tot for each participantID
df["speedDev"] = pd.NA

for pid, sub in df.groupby("participantID"):
    # Need at least 2 non-NaN rows to fit a line
    sub_valid = sub[["RoundNum_tot", "SessionElapsedTime"]].dropna()
    if len(sub_valid) < 2:
        # Can't fit a regression: set residuals to NaN for this participantID
        df.loc[sub.index, "speedDev"] = pd.NA
        continue

    X = sm.add_constant(sub_valid["RoundNum_tot"])
    y = sub_valid["SessionElapsedTime"]
    ols_sub = sm.OLS(y, X).fit()

    # Map residuals back to the original indices
    df.loc[sub_valid.index, "speedDev"] = ols_sub.resid

# Convert to float (will keep NaNs)
df["speedDev"] = df["speedDev"].astype(float)


## PVSS, 18 participantIDs who have it
df_pvss = df[~df["PVSS"].isna()].copy()

randomEffects_all  = ['main_RR','speedDev','TestPhase','coinSet','taskNaive']
randomEffects_PVSS = ['main_RR','speedDev','TestPhase','coinSet','taskNaive','PVSS_Avg']


## All 22 participantIDs, no PVSS

# pinDropDist Mixed Model
pDD_model_all = pinDropModeling(df, "pinDropDist", randomEffects_all)
pDD_result_all = pDD_model_all.fit(reml=False)

# pinDropLatency Mixed Model
pDL_model_all = pinDropModeling(df, "pinDropLatency", randomEffects_all)
pDL_result_all = pDL_model_all.fit(reml=False)


## PVSS Mixed Models: 18 participantIDs who have it

#pinDropDist PVSS Mixed Model
pDD_model_PVSS = pinDropModeling(df_pvss, "pinDropDist", randomEffects_PVSS)
pDD_result_PVSS = pDD_model_PVSS.fit(reml=False)

#pinDropLatency PVSS Mixed Model
pDL_model_PVSS = pinDropModeling(df_pvss, "pinDropLatency", randomEffects_PVSS)
pDL_result_PVSS = pDL_model_PVSS.fit(reml=False)



