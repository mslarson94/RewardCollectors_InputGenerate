# Simulate pin drop latency data
pin_drop_data = []
trial_route_map = {trial: np.random.choice(route_categories) for trial in range(1, n_trials + 1)}

for trial in range(1, n_trials + 1):
    for coin in coin_types:
        latency = np.clip(np.random.normal(loc=1.2 if coin == 'HV' else 0.8, scale=0.3), 0, 2)
        quality = 'good' if latency < 1 else 'bad'
        pin_drop_data.append({
            'Trial': trial,
            'PinDropLatency': latency,
            'PinDropQuality': quality,
            'CoinType': coin,
            'RouteType': trial_route_map[trial]
        })

df_pin_drop = pd.DataFrame(pin_drop_data)

# Plot function
def plot_pin_latency_with_route(data, **kwargs):
    ax = plt.gca()
    for trial in sorted(data['Trial'].unique()):
        route = data[data['Trial'] == trial]['RouteType'].iloc[0]
        ax.axvspan(trial - 0.4, trial + 0.4, color=category_palette[route], alpha=0.3, zorder=0)

    for _, row in data.iterrows():
        jittered_x = row['Trial'] + np.random.normal(0, 0.07)
        color = 'black' if row['PinDropQuality'] == 'good' else 'red'
        marker = 'o' if row['PinDropQuality'] == 'good' else 'X'
        size = 60 if row['PinDropQuality'] == 'good' else 80
        ax.scatter(jittered_x, row['PinDropLatency'], color=color, marker=marker, s=size, zorder=2)

# Facet and render
g = sns.FacetGrid(df_pin_drop, row="CoinType", height=3.5, aspect=3, sharex=True, sharey=True)
g.map_dataframe(plot_pin_latency_with_route)
g.set_axis_labels("Trial", "Pin Drop Latency (s)")
g.set_titles(row_template="{row_name}")
g.set(ylim=(0, 2))
plt.suptitle("Pin Drop Latency per Trial by Coin Type\nRoute Type as Background", y=1.02)
plt.tight_layout()
plt.show()
