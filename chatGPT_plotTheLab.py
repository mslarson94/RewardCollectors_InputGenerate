# Load the newly uploaded CSV file
file_path_new = '/mnt/data/ObsReward_B_07_16_2024_13_46_cleaned_data.csv'
data_new = pd.read_csv(file_path_new)

# Extracting rows where "Message" contains "Observer chose INCORRECT" or "Observer chose CORRECT"
vote_pattern = r"Observer chose (INCORRECT|CORRECT) for this pindrop from the navigator"
vote_rows_new = data_new['Message'].str.contains(vote_pattern, na=False)

# Finding the rows that directly precede these voting events
preceding_rows_new = data_new[vote_rows_new].index - 1

# Extract the 'HeadPosAnchored' values from the preceding rows
positions_new = data_new.loc[preceding_rows_new, 'HeadPosAnchored'].dropna()

# Ensure valid format for 'HeadPosAnchored' (i.e., exactly three space-separated values)
positions_valid_new = positions_new[positions_new.str.count(' ') == 2]

# Splitting the 'HeadPosAnchored' values into x, y, and z
positions_split_new = positions_valid_new.str.split(' ', expand=True)
positions_split_new.columns = ['x', 'y', 'z']

# Convert x and z to floats for plotting
positions_split_new['x'] = positions_split_new['x'].astype(float)
positions_split_new['z'] = positions_split_new['z'].astype(float)

# Now we will plot these x and z coordinates, labeling each point with its coordinates

plt.figure(figsize=(10, 6))
plt.scatter(positions_split_new['x'], positions_split_new['z'], color='purple')

# Adding labels to each point
for i, row in positions_split_new.iterrows():
    plt.text(row['x'], row['z'], f"({row['x']:.2f}, {row['z']:.2f})", fontsize=9, ha='right')

# Set labels and title
plt.title("Observer X and Z Coordinates when Submitting a Vote")
plt.xlabel("X Coordinate")
plt.ylabel("Z Coordinate")

# Show the plot
plt.grid(True)
plt.show()
