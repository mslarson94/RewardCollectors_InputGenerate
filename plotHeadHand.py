import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the dataset
file_path = 'df_Global_block.csv'
df = pd.read_csv(file_path)


# Define colors based on the 'verdict' column
df['color'] = df['verdict'].apply(lambda x: 'red' if x == ' bad drop' else 'black')

# Plotting head_Hand_dist vs. AppTime with a regression line and confidence interval
plt.figure(figsize=(10, 5))

# Plot the regression line with confidence interval
sns.regplot(x='AppTime', y='head_Hand_dist', data=df, ci=95, scatter=False, line_kws={'color': 'blue'})

# Overlay the individual points with colors based on 'verdict'
plt.scatter(df['AppTime'], df['head_Hand_dist'], s=25, alpha=0.5, edgecolor='black')
# Calculate Q1 (25th percentile) and Q3 (75th percentile) for each GlobalBlock
Q1 = df.groupby('GlobalBlock')['head_Hand_dist'].quantile(0.25).reset_index()
Q3 = df.groupby('GlobalBlock')['head_Hand_dist'].quantile(0.75).reset_index()

# Calculate the IQR for each GlobalBlock
IQR = Q3['head_Hand_dist'] - Q1['head_Hand_dist']

# Define outlier bounds
lower_bound = Q1['head_Hand_dist'] - 1.5 * IQR
upper_bound = Q3['head_Hand_dist'] + 1.5 * IQR

# Merge the bounds with the original data
df_outliers = df.merge(Q1[['GlobalBlock', 'head_Hand_dist']], on='GlobalBlock', how='left', suffixes=('', '_Q1'))
df_outliers = df_outliers.merge(Q3[['GlobalBlock', 'head_Hand_dist']], on='GlobalBlock', how='left', suffixes=('', '_Q3'))
df_outliers['IQR'] = IQR
df_outliers['lower_bound'] = lower_bound
df_outliers['upper_bound'] = upper_bound

# Identify outliers
df_outliers['is_outlier'] = ((df_outliers['head_Hand_dist'] < df_outliers['lower_bound']) | 
                             (df_outliers['head_Hand_dist'] > df_outliers['upper_bound']))

# Overlay the outliers with a different color
sns.scatterplot(x='AppTime', y='head_Hand_dist', data=df_outliers[df_outliers['is_outlier']], 
                color='red', edgecolor='black', s=80, label='Outliers')


# Adding labels and title
plt.title('Mean Head-Hand Distance Over AppTime with Standard Deviation')
plt.xlabel('AppTime')
plt.ylabel('Head-Hand Distance in meters')
plt.legend()
plt.grid(True)
plt.savefig('headHand.png')

# Step 1: Group by GlobalBlock and calculate the average head_Hand_dist
average_head_hand_dist = df.groupby('GlobalBlock')['head_Hand_dist'].mean().reset_index()

# Step 2: Extract the time (AppTimeCorr) from the first waypoint in each block
first_waypoint_time = df[df['WaypointNumber'] == 1].groupby('GlobalBlock')['AppTimeCorr'].first().reset_index()

# Step 3: Merge the average_head_hand_dist with the first_waypoint_time on GlobalBlock
result_df = pd.merge(average_head_hand_dist, first_waypoint_time, on='GlobalBlock')

# Step 4: Rename the columns for clarity
result_df.columns = ['GlobalBlock', 'Average_head_Hand_dist', 'First_Waypoint_Time']

# Step 5: Display the result
print(result_df)

# Optionally, visualize the results
plt.figure(figsize=(10, 6))
#sns.violinplot(x='GlobalBlock', y='head_Hand_dist', data=df, inner=None, palette='viridis')
sns.violinplot(x='GlobalBlock', y='head_Hand_dist', data=df, palette='Pastel1',  inner_kws=dict(color=".8"))
sns.stripplot(x='GlobalBlock', y='head_Hand_dist', data=df, color='k', alpha=0.5, jitter=True)

plt.xlabel('GlobalBlock')
plt.ylabel('Average Head-Hand Distance')
plt.title('Average Head-Hand Distance per Block with Time as First Waypoint')
plt.savefig('avg_headHand.png')
