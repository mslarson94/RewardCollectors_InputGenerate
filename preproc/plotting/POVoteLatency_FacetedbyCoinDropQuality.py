import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set up styles and categories
n_trials = 50
coin_types = ['HV', 'LV', 'NV']
route_categories = ['Type 1', 'Type 3', 'Type 4', 'Other']
category_palette = {
    'Type 1': '#66c2a5',
    'Type 3': '#fc8d62',
    'Type 4': '#8da0cb',
    'Other': '#e78ac3'
}
color_map = {'good': '#a6d854', 'bad': '#fc8d62'}

# Simulate data
multi_vote_data = []
for trial in range(1, n_trials + 1):
    an_quality = np.random.choice(['good', 'bad'], p=[0.7, 0.3])
    for coin in coin_types:
        if np.random.rand() > 0.1:
            latency = np.clip(np.random.normal(loc=1.0, scale=0.4), 0, 2)
            quality = np.random.choice(['good', 'bad'], p=[0.8, 0.2])
        else:
            latency = np.nan
            quality = np.nan
        multi_vote_data.append({
            'Trial': trial,
            'AN_PinDrop_Quality': an_quality,
            'PO_Vote_Latency': latency,
            'PO_Vote_Quality': quality,
            'CoinType': coin
        })

df_multi_vote = pd.DataFrame(multi_vote_data)

# Plot function
def plot_facet(data, **kwargs):
    ax = plt.gca()
    for trial in sorted(data['Trial'].unique()):
        quality = data[data['Trial'] == trial]['AN_PinDrop_Quality'].iloc[0]
        ax.axvspan(trial - 0.4, trial + 0.4, color=color_map[quality], alpha=0.4, zorder=0)

    for _, row in data.iterrows():
        if not np.isnan(row['PO_Vote_Latency']):
            jittered_x = row['Trial'] + np.random.normal(0, 0.07)
            color = 'black' if row['PO_Vote_Quality'] == 'good' else 'red'
            marker = 'o' if row['PO_Vote_Quality'] == 'good' else 'X'
            size = 60 if row['PO_Vote_Quality'] == 'good' else 80
            ax.scatter(jittered_x, row['PO_Vote_Latency'], color=color, marker=marker, s=size, zorder=2)

# Facet and render
g = sns.FacetGrid(df_multi_vote, row="CoinType", height=3.5, aspect=3, sharex=True, sharey=True)
g.map_dataframe(plot_facet)
g.set_axis_labels("Trial", "PO Vote Latency (s)")
g.set_titles(row_template="{row_name}")
g.set(ylim=(0, 2))
plt.suptitle("PO Vote Latency per Trial by Coin Type\nWith AN Pin Drop Quality Background", y=1.02)
plt.tight_layout()
plt.show()
