import pandas as pd
import re

# Assuming 'df' is your log data DataFrame with a 'Message' column

# Example data (replace this with your actual DataFrame)
filePath = '/Users/mairahmac/Desktop/RewardCollectorsCentral/TestingNotes_Misc/08312024/magicLeap'
fileName = 'ObsReward_A_08_31_2024_allCleaned'
file = filePath + '/' + fileName + '.csv'
df = pd.read_csv(file)


# Initialize the 'RoundNum' column with NaN values
df['RoundNum'] = None

# Function to extract X and Z values from 'HeadPosAnchored'
def extract_xz(headpos):
    if isinstance(headpos, str):
        try:
            x, y, z = map(float, headpos.split())
            return x, z  # Return X and Z, ignoring Y
        except ValueError:
            # If there's a problem parsing, return default values
            return 0.0, 0.0
    else:
        # If the value is not a string, return default values
        return 0.0, 0.0

# Apply the function to extract X and Z coordinates from 'HeadPosAnchored'
df[['X', 'Z']] = df['HeadPosAnchored'].apply(lambda pos: pd.Series(extract_xz(pos)))

# Check if the X and Z columns were created successfully
print(df.head())  # Check if 'X' and 'Z' are there

# Regular expression to detect the end of a round with the round number
end_round_regex = r"Finished pindrop round:(\d+)"

# Variables to track round numbers
current_round = None
last_block = None

# Iterate over the rows of the DataFrame to assign round numbers
for idx, row in df.iterrows():
    message = str(row['Message'])  # Ensure 'Message' is treated as a string
    current_block = row['BlockNumber']  # Get the current block from the BlockNum column

    # Check if we are in a new block and reset the round number
    if current_block != last_block:
        current_round = 0  # Reset round number for the new block
        last_block = current_block
    
    # Detect the start of a new round
    if message == "Repositioned and ready to start block or round":
        current_round += 1  # Increment the round number within the current block
    
    # Detect the end of the current round using regex
    match = re.search(end_round_regex, message)
    if match:
        round_num = int(match.group(1))
        current_round = round_num  # Ensure the round number is correctly set

    # Assign the current round number to the 'RoundNum' column
    df.at[idx, 'RoundNum'] = current_round

# Print the updated DataFrame with RoundNum, X, and Z columns
print(df[['BlockNumber', 'Message', 'HeadPosAnchored', 'X', 'Z', 'RoundNum']])


# Print the updated DataFrame with RoundNum column
df_outFile = filePath + '/' + fileName + '_RoundNum.csv'
df.to_csv(df_outFile, index=False)