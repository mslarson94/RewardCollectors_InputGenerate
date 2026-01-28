import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf

## Within Participant Fatigue 
# Ensure participant is treated as categorical if needed
df["participant"] = df["participant"].astype("category")

# Compute within-participant residuals: TotalElapsedTime ~ BlockNum for each participant
df["fatigue_resid_within"] = pd.NA

for pid, sub in df.groupby("participant"):
    # Need at least 2 non-NaN rows to fit a line
    sub_valid = sub[["BlockNum", "TotalElapsedTime"]].dropna()
    if len(sub_valid) < 2:
        # Can't fit a regression: set residuals to NaN for this participant
        df.loc[sub.index, "fatigue_resid_within"] = pd.NA
        continue

    X = sm.add_constant(sub_valid["BlockNum"])
    y = sub_valid["TotalElapsedTime"]
    ols_sub = sm.OLS(y, X).fit()

    # Map residuals back to the original indices
    df.loc[sub_valid.index, "fatigue_resid_within"] = ols_sub.resid

# Convert to float (will keep NaNs)
df["fatigue_resid_within"] = df["fatigue_resid_within"].astype(float)


# Make sure categorical predictors are categorical
df["CoinType"] = df["CoinType"].astype("category")
df["coinLayout"] = df["coinLayout"].astype("category")
 

## Mixed Model (all 22 participants, no PVSS)

model_all = smf.mixedlm(
    "pinDropDist ~ CoinType*BlockNum + mainRR + fatigue_resid_within + coinLayout",
    data=df,
    groups=df["participant"],
    re_formula="1 + mainRR + fatigue_resid_within + coinLayout"
)

result_all = model_all.fit(reml=False)
print(result_all.summary())

## Mixed Model (PVSS, 18 participants who have it)
df_pvss = df[~df["PVSS"].isna()].copy()

model_pvss = smf.mixedlm(
    "pinDropDist ~ CoinType*BlockNum + mainRR + fatigue_resid_within + coinLayout + PVSS",
    data=df_pvss,
    groups=df_pvss["participant"],
    re_formula="1 + mainRR + fatigue_resid_within + coinLayout"
)

result_pvss = model_pvss.fit(reml=False)
print(result_pvss.summary())
