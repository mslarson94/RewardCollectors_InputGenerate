import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
#import matplotlib.cm as cm

#fileName = '/Users/mairahmac/Desktop/RC_Data_Analysis/Data/myraMovementsRawIsh.csv'
fileName = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08112024/ObsReward_A_08_09_2024_13_35.csv'
#library(tidyverse)
############ Set A Coins ###############
# [x, z, orig value]

#High Value
HV_1 = [-2.5,1.9, 10.0]
HV_2=  [1.8,-3.1, 10.0]

#Low Value
LV_1 = [2.8,1.2, 5.0]
LV_2 = [-2.1,-2.2, 5.0]

#Null Value
NV_1 = [0.75, 3.0, 0.0]
NV_2 = [0.2, -1.3, 0.0]

pos1 = [[0.0, 5.0],     [1.75, 4.25]] 
pos2 = [[3.5, 3.5],     [4.25, 1.75]]
pos3 = [[5.0, 0.0],     [4.25, -1.75]]
pos4 = [[3.5, -3.5],    [1.75, -4.25]]
pos5 = [[0.0, -5.0],    [-1.75, -4.25]]
pos6 = [[-3.5, -3.5],   [-4.25, -1.75]]
pos7 = [[-5.0, 0.0],    [-4.25, 1.75]]
pos8 = [[-3.5, 3.5],    [-1.75, 4.25]]



# collectionOrder_List_str = ['LV_1', 'NV_2', 'HV_1', 'NV_1', 'HV_2', 'LV_2']
# actual_startPos = tuple(pos6[0])
# firstpos = 6

annotations = {
	'startPos' : (tuple(pos6[0])),
    'HV_1': (HV_1[0], HV_1[1]),
    'HV_2': (HV_2[0], HV_2[1]),
    'LV_1': (LV_1[0], LV_1[1]),
    'LV_2': (LV_2[0], LV_2[1]),
    'NV_1': (NV_1[0], NV_1[1]),
    'NV_2': (NV_2[0], NV_2[1])
}


# Desired order of keys
collectionOrder_List_str = ['startPos','LV_1', 'HV_1', 'NV_2', 'LV_2', 'HV_2', 'NV_1']

# Reordering the annotations dictionary
ordered_annotations = {key: annotations[key] for key in collectionOrder_List_str}

# def plot_head_positions_myra(filepath):
#     """
#     Reads a CSV file, processes time data, drops missing values, and plots head positions.
#     !!!Note: The csv must have column name "Time" "Headposx" "Headposy" "Headposz"
#     Parameters:
#     - filepath: str, the path to the CSV file containing the data.
#     """
#     with open('filepath') as f:
#       lines_after_19 = f.readlines()[19:]

#   for idx, r in df.iterrows():
#     if r.is.na == True:
#       print(idx, df.iloc())
# df %>%
#   group_by(id) %>%          # for each id
#   mutate(flag = cumsum(ifelse(v < 5, 1, NA))) %>%  # check if v < 5 and fill with NA all rows when condition is  FALSE and after that
#   filter(!is.na(flag)) %>%  # keep only rows with no NA flags
#   ungroup() %>%             # forget the grouping
#   select(-flag)

# def plot_head_positions(filepath):
# 	"""
# 	Reads a CSV file, processes time data, drops missing values, and plots head positions.
# 	!!!Note: The csv must have column name "Time" "Headposx" "Headposy" "Headposz"
# 	Parameters:
# 	- filepath: str, the path to the CSV file containing the data.
# 	"""
# 	# with open('filepath') as f:
# 	#   lines_after_19 = f.readlines()[19:]

# 	# # Read the CSV file
# 	# df_raw = pd.read_csv(filepath)
	
# 	# df_raw.loc[df_raw['HeadPosAnchored'] == '', 'HeadPosAnchored'] = np.nan

# 	# # Drop rows with any missing values
# 	# df = df_raw.dropna(subset=['HeadPosAnchored'])
# 	# # Convert time strings to datetime objects including milliseconds
# 	# df.loc[:, 'Timestamp'] = pd.to_datetime(df['Timestamp'], format='%H:%M:%S:%f')

# 	# # Convert datetime objects to seconds since the first timestamp
# 	# df.loc[:, 'Time_seconds'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()
# 	# df.loc[:, ['HeadPosX', 'HeadPoxY', 'HeadPosZ']] = df['HeadPosAnchored'].str.split(' ', expand=True)

# 	# #df[['HeadPosX', 'HeadPoxY', 'HeadPosZ']] = df['HeadPosAnchored'].str.split(' ', expand=True)

# 	# # Create a scatter plot where color varies with time
# 	# plt.figure(figsize=(10, 6))
# 	# scatter = plt.scatter(df['HeadPosX'], df['HeadPosZ'], c=df['Time_seconds'], cmap='viridis', alpha=0.5, s=2)
# 	# plt.title('2D Scatter Plot of Head Positions with Time Color Encoding')
# 	# plt.xlabel('HeadPosX')
# 	# plt.ylabel('HeadPosZ')
# 	# plt.colorbar(scatter, label='Time_seconds')
# 	# plt.grid(True)
# 	# #plt.savefig(f"frames/frame_{frame_id:03d}.png")
# 	# plt.savefig(f"headPos_OverTime.png")

# 	df_raw = pd.read_csv(filepath)

# 	# Replace empty strings with NaN in the 'HeadPosAnchored' column
# 	df_raw.loc[df_raw['HeadPosAnchored'] == '', 'HeadPosAnchored'] = np.nan

# 	# Drop rows with any missing values in the 'HeadPosAnchored' column
# 	df = df_raw.dropna(subset=['HeadPosAnchored'])

# 	# Convert time strings to datetime objects, including milliseconds
# 	df.loc[:, 'Timestamp'] = pd.to_datetime(df['Timestamp'], format='%H:%M:%S:%f')

# 	# Convert datetime objects to seconds since the first timestamp
# 	df.loc[:, 'Time_seconds'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()

# 	# Split 'HeadPosAnchored' into three new columns 'HeadPosX', 'HeadPosY', and 'HeadPosZ'
# 	df.loc[:, ['HeadPosX', 'HeadPosY', 'HeadPosZ']] = df['HeadPosAnchored'].str.split(' ', expand=True)

# 	# Convert 'HeadPosX', 'HeadPosY', and 'HeadPosZ' to float for plotting
# 	df[['HeadPosX', 'HeadPosY', 'HeadPosZ']] = df[['HeadPosX', 'HeadPosY', 'HeadPosZ']].astype(float)



def plot_head_positions(filepath):

    # Read the CSV file
    df_raw = pd.read_csv(filepath, error_bad_lines=False, warn_bad_lines=True)
    
    # Replace empty strings with NaN in the 'HeadPosAnchored' column
    #df_raw.loc[df_raw['HeadPosAnchored'] == '', 'HeadPosAnchored'] = np.nan

    # Drop rows with any missing values in the 'HeadPosAnchored' column
    #df = df_raw.dropna(subset=['HeadPosAnchored']).copy()  # Ensure df is a fresh copy
    df = df_raw[df_raw['GlobalBlock'] == 5].copy()
    # Convert time strings to datetime objects, including milliseconds
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%H:%M:%S:%f')

    # Convert datetime objects to seconds since the first timestamp
    df['Time_seconds'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()

    # Split 'HeadPosAnchored' into three new columns 'HeadPosX', 'HeadPosY', and 'HeadPosZ'
    df[['HeadPosX', 'HeadPosY', 'HeadPosZ']] = df['HeadPosAnchored'].str.split(' ', expand=True)

    # Convert 'HeadPosX', 'HeadPosY', and 'HeadPosZ' to float for plotting
    df[['HeadPosX', 'HeadPosY', 'HeadPosZ']] = df[['HeadPosX', 'HeadPosY', 'HeadPosZ']].astype(float)

    # Create a scatter plot where color varies with time
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(df['HeadPosX'], df['HeadPosZ'], c=df['Time_seconds'], cmap='hsv', alpha=0.5, s=2)
    plt.title('2D Scatter Plot of Head Positions with Time Color Encoding')
    plt.xlabel('HeadPosX')
    plt.ylabel('HeadPosZ')
    plt.colorbar(scatter, label='Time_seconds')
    plt.grid(True)
    
    # Use viridis colormap
    colormap = plt.get_cmap('hsv', len(ordered_annotations))

    for i, (label, (x, y)) in enumerate(ordered_annotations.items()):
        color = colormap(i / len(ordered_annotations))
        #plt.text(x, y, '*', color=color, fontsize=20, ha='center', va='center', weight='bold')  # Place the '*' symbol
        plt.scatter(x, y, color=color, marker='o', s=100)
        plt.text(x, y - 0.3, label, color='black', fontsize=8, ha='center', weight='bold')  # Label above the '*'

    start_x, start_y = ordered_annotations['startPos']
    plt.scatter(start_x, start_y, color='black', marker='o', s=100)  # 's' controls the size of the marker

    plt.savefig("headPos_OverTime.png")

plot_head_positions(fileName)
#plot_head_positions(fileName)