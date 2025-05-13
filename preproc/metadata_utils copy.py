import pandas as pd

def load_metadata(metadata_file, role="PO"):
    df = pd.read_excel(metadata_file, sheet_name="MagicLeapFiles")
    df = df.dropna(subset=["cleanedFile"])
    df["cleanedFile"] = df["cleanedFile"].str.strip().str.lower()
    
    valid_df = df[
        (df["participantID"] != "none") &
        (df["pairID"] != "none") &
        (df["Role"].str.lower() == role.lower())
    ]
    return df, valid_df, set(df["cleanedFile"]), set(valid_df["cleanedFile"])
