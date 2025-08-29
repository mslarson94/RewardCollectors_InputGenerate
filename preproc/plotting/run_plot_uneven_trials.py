
import pandas as pd
import matplotlib.pyplot as plt
import random
from itertools import permutations
from matplotlib.colors import ListedColormap

# Simulate participants with uneven trial counts
participants_uneven = {
    'P01': 103,
    'P02': 88,
    'P03': 76,
    'P04': 95,
    'P05': 65
}

# Define route categories and mappings
order_map = {
    ('HV', 'LV', 'NV'): 'Type 1',
    ('LV', 'HV', 'NV'): 'Type 3',
    ('LV', 'NV', 'HV'): 'Type 4',
}
all_orders = list(permutations(['HV', 'LV', 'NV']))
category_palette = {
    'Type 1': '#66c2a5',
    'Type 3': '#fc8d62',
    'Type 4': '#8da0cb',
    'Other': '#999999'
}

# Simulate data
uneven_dummy_data = []
for pid, n_trials in participants_uneven.items():
    for trial in range(1, n_trials + 1):
        order = random.choice(all_orders)
        label = order_map.get(order, 'Other')
        uneven_dummy_data.append({
            'Participant': pid,
            'Trial': trial,
            'Category': label
        })

df_uneven = pd.DataFrame(uneven_dummy_data)

# Pivot and fill missing values
df_runplot_un = df_uneven.pivot(index='Participant', columns='Trial', values='Category')
df_runplot_un_filled = df_runplot_un.fillna('No Trial')

# Update colormap
category_palette_extended = {
    **category_palette,
    'No Trial': '#d3d3d3'
}
all_categories_extended = list(category_palette_extended.keys())
category_to_int_extended = {cat: i for i, cat in enumerate(all_categories_extended)}
df_runplot_un_int = df_runplot_un_filled.replace(category_to_int_extended)
cmap_extended = ListedColormap([category_palette_extended[cat] for cat in all_categories_extended])

# Plot
plt.figure(figsize=(16, 4))
plt.imshow(df_runplot_un_int, aspect='auto', cmap=cmap_extended)
plt.yticks(ticks=range(len(df_runplot_un.index)), labels=df_runplot_un.index)
max_trials = max(list(participants_uneven.values()))
plt.xticks(ticks=list(range(0, max_trials + 1, 10)), labels=list(range(1, max_trials + 1, 10)))
plt.xlabel('Trial')
plt.ylabel('Participant')
plt.title('Categorical Run Plot with Uneven Trial Counts')

# Legend
handles = [plt.Line2D([0], [0], marker='s', color=color, label=cat, linestyle='') 
           for cat, color in category_palette_extended.items()]
plt.legend(handles=handles, title='Route Type', bbox_to_anchor=(1.01, 1), loc='upper left')
plt.tight_layout()
plt.show()
