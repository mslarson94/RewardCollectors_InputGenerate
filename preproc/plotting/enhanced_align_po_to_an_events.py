
def align_po_to_an_events(an_events_file, po_events_file, po_processed_file, output_file):
    import pandas as pd

    print("📂 Loading AN events file:", an_events_file)
    df_an_events = pd.read_csv(an_events_file)

    # Confirm required columns exist
    print("🧪 Columns in AN events file:", df_an_events.columns.tolist())

    # Filter and sort AN block starts
    df_an_starts = df_an_events[df_an_events['lo_eventType'] == 'BlockStart']
    df_an_starts = df_an_starts.sort_values(['BlockNum', 'BlockInstance', 'origRow_start'])
    df_an_starts = df_an_starts.groupby(['BlockNum', 'BlockInstance'], as_index=False).first()

    # Fallback parsing for missing mLTimestamp
    if 'mLTimestamp' not in df_an_starts.columns or df_an_starts['mLTimestamp'].isna().all():
        print("⚠️ mLTimestamp missing or empty. Attempting to parse from backup columns...")
        if 'ParsedTimestamp' in df_an_starts.columns:
            df_an_starts['mLTimestamp'] = pd.to_datetime(df_an_starts['ParsedTimestamp'], errors='coerce')
        elif 'Timestamp' in df_an_starts.columns:
            df_an_starts['mLTimestamp'] = pd.to_datetime(df_an_starts['Timestamp'], errors='coerce')
        else:
            raise ValueError("No usable timestamp column found in AN events.")

    print("✅ Sample aligned AN starts:")
    print(df_an_starts[['BlockNum', 'BlockInstance', 'mLTimestamp']].head())

    df_po = pd.read_csv(po_processed_file)
    print("📂 Loaded PO processed file:", po_processed_file)
    print("🧪 PO columns:", df_po.columns.tolist())

    # Create empty mLTimestamp column
    df_po['mLTimestamp'] = pd.NaT

    # Align by BlockNum and BlockInstance
    for _, an_row in df_an_starts.iterrows():
        bn = an_row['BlockNum']
        bi = an_row['BlockInstance']
        an_start_ts = an_row['mLTimestamp']
        if isinstance(an_start_ts, str):
            an_start_ts = pd.to_datetime(an_start_ts, errors='coerce')
        mask = (df_po['BlockNum'] == bn) & (df_po['BlockInstance'] == bi)
        if mask.sum() > 0:
            df_po.loc[mask, 'mLTimestamp'] = an_start_ts
        else:
            print(f"⚠️ BlockNum={bn}, BlockInstance={bi} found in AN but not in PO.")

    df_po.to_csv(output_file, index=False)
    print(f"✅ Aligned processed file saved to {output_file}")
    return df_po
