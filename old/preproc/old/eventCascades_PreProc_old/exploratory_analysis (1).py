
# ---
# 🧠 RC Cascade Event Analysis
# ---

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- Load Data ---
event_df = pd.read_csv("event_summary.csv")

# --- Filter to Dropped Pins with Cascade Metadata ---
pins_df = event_df[event_df["event_type"] == "Just dropped a pin"].copy()

# --- Basic Distribution Analysis ---
print("Drop Quality Counts:")
print(pins_df["drop_quality"].value_counts())

print("\nDrop Score Stats by Quality:")
print(pins_df.groupby("drop_quality")["result_score"].describe())

# --- Score vs Distance ---
plt.figure()
sns.scatterplot(data=pins_df, x="drop_distance", y="result_score", hue="drop_quality")
plt.title("Drop Score vs Distance")
plt.xlabel("Drop Distance")
plt.ylabel("Result Score")
plt.tight_layout()
plt.show()

# --- Bonus & Coin Value Analysis ---
plt.figure()
sns.boxplot(data=pins_df, x="drop_quality", y="result_bonus")
plt.title("Bonus Distribution by Drop Quality")
plt.tight_layout()
plt.show()

# --- Load and Join with Walking Periods ---
walking_df = event_df[event_df["event_type"] == "Walking Period"].copy()

# Merge with drop outcome
walk_merge = walking_df.merge(
    pins_df[["cascade_id", "drop_quality", "result_score"]],
    on="cascade_id", how="left"
)

# --- Walking Duration by Drop Quality ---
plt.figure()
sns.boxplot(data=walk_merge, x="drop_quality", y="duration")
plt.title("Walking Duration by Drop Quality")
plt.tight_layout()
plt.show()

# --- Summary Stats ---
print("\nWalking Duration Summary:")
print(walk_merge.groupby("drop_quality")["duration"].describe())
