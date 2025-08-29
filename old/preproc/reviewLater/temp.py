
def aligned_output_path(metadata_path, baseDir, output_path):
	# Filter valid entries
	metadata = pd.read_excel(metadata_path, sheet_name="MagicLeapFiles")
	valid_metadata = metadata[
	    metadata["currentRole"].isin(["AN", "PO"]) &
	    metadata["cleanedFile"].notna() &
	    metadata["testingDate"].notna() &
	    metadata["sessionType"].notna() &
	    metadata["device"].notna() &
	    metadata["pairID"].apply(lambda x: str(x).isdigit())
	].copy()

	# Format pairID
	valid_metadata["pairID"] = valid_metadata["pairID"].apply(lambda x: f"pair_{int(x):03}")

	valid_metadata["suffix"] = valid_metadata["cleanedFile"].apply(extract_suffix)

	# Split into AN and PO
	an_files = valid_metadata[valid_metadata["currentRole"] == "AN"]
	po_files = valid_metadata[valid_metadata["currentRole"] == "PO"]

	# Merge on identifying fields
	merged = pd.merge(
	    an_files,
	    po_files,
	    on=["pairID", "testingDate", "sessionType", "suffix"],
	    suffixes=("_an", "_po")
	)

	# Construct aligned output paths
	base_dir = Path(baseDir)
	merged["aligned_output_path"] = merged.apply(
	    lambda row: str(
	        base_dir
	        / row["pairID"]
	        / row["testingDate"]
	        / row["sessionType"]
	        / "MagicLeaps"
	        / row["device_po"]
	        / row["cleanedFile_po"].replace("_processed_unaligned.csv", "_processed.csv")
	    ),
	    axis=1
	)
	os.makedirs(output_path, exist_ok=True)
	timestr= datetime.now().strftime("%Y_%m_%d-%H_%M")
	outFile = os.path.join(output_path, ('matched_ANPO' + timestr + '.csv'))
	merged.to_csv(outFile, index=False)


# def robust_parse_timestamp_to_time(ts):
#     try:
#         hh, mm, ss, ms = ts.split(":")
#         return time(int(hh), int(mm), int(ss), int(ms) * 1000)
#     except:
#         return pd.NaT

# def infer_7777_from_an(an_path, po_df):
#     an_df = pd.read_csv(an_path, dtype={"Timestamp": str}, low_memory=False)
#     an_df["Timestamp_dt"] = an_df["Timestamp"].apply(robust_parse_timestamp_to_time)
#     po_df["Timestamp_dt"] = po_df["Timestamp"].apply(robust_parse_timestamp_to_time)

#     ranges_7777 = []
#     in_range = False
#     start_time = None

#     for _, row in an_df.iterrows():
#         if row["RoundNum"] == 7777 and not in_range:
#             start_time = row["Timestamp_dt"]
#             in_range = True
#         elif row["RoundNum"] != 7777 and in_range:
#             end_time = row["Timestamp_dt"]
#             ranges_7777.append((start_time, end_time))
#             in_range = False

#     if in_range and start_time is not None:
#         ranges_7777.append((start_time, an_df["Timestamp_dt"].iloc[-1]))

#     po_df["RoundNum"] = po_df.apply(
#         lambda row: 7777 if any(start <= row["Timestamp_dt"] <= end for start, end in ranges_7777)
#         else row["RoundNum"],
#         axis=1
#     )
#     return po_df

# def extract_suffix(filename):
#     # Keeps date and full hour-minute stamp for uniqueness
#     return "_".join(filename.split("_")[2:6])

# def find_an_po_pairs(metadata, base_dir):
#     pairs = []

#     grouped = metadata.groupby(["pairID", "testingDate", "sessionType"])

#     for (pair_id, test_date, session_type), group in grouped:
#         # Get AN and PO files with their correct devices
#         an_group = group[group["cleanedFile"].str.startswith("ObsReward_A")]
#         po_group = group[group["cleanedFile"].str.startswith("ObsReward_B")]

#         # Build suffix maps with correct device names
#         an_suffix_map = {
#             extract_suffix(row["cleanedFile"]): (row["cleanedFile"], row["device"])
#             for _, row in an_group.iterrows()
#         }

#         po_suffix_map = {
#             extract_suffix(row["unalignedFile"]): (row["unalignedFile"], row["device"])
#             for _, row in po_group.iterrows()
#         }

#         # Match files by suffix
#         for suffix in an_suffix_map.keys() & po_suffix_map.keys():
#             an_file, an_device = an_suffix_map[suffix]
#             po_file, po_device = po_suffix_map[suffix]

#             an_path = Path(base_dir) / pair_id / test_date / session_type / "MagicLeaps" / an_device / an_file
#             po_path = Path(base_dir) / pair_id / test_date / session_type / "MagicLeaps" / po_device / po_file

#             pairs.append((str(an_path), str(po_path)))

#     return pairs

# def batch_enrich_po_with_7777(metadata, base_dir):
#     enriched_paths = []

#     for an_path, po_unaligned_path in find_an_po_pairs(metadata, base_dir):
#         # Debug print: show matching AN and unaligned PO
#         print(f"🔍 Matching AN: {an_path}")
#         print(f"🔍 Unaligned PO: {po_unaligned_path}")

#         if not os.path.exists(po_unaligned_path):
#             print(f"⚠️ Missing input file: {po_unaligned_path}")
#             continue

#         # Load and enrich PO file with inferred RoundNum = 7777
#         po_df = pd.read_csv(po_unaligned_path, dtype={"Timestamp": str}, low_memory=False)
#         enriched_df = infer_7777_from_an(an_path, po_df)

#         # Determine aligned output path (remove 'unaligned/', rename suffix)
#         aligned_output_path = str(
#             Path(po_unaligned_path).parent.parent / os.path.basename(po_unaligned_path).replace("_processed_unaligned.csv", "_processed.csv")
#         )

#         # Debug print: show final output save path
#         print(f"💾 Saving to: {aligned_output_path}")

#         # Save enriched PO file
#         enriched_df.to_csv(aligned_output_path, index=False)
#         enriched_paths.append(aligned_output_path)

#         print(f"✅ Processed: {aligned_output_path}")

#     return enriched_paths



