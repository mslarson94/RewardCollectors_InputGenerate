import os
import shutil

# Define the root directory to search and the destination directory

source_root = '/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/Events'
destination = "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart/full/MetaData_Flat"

# Create the destination directory if it doesn't exist
os.makedirs(destination, exist_ok=True)
print("starting!")
# Walk through the directory tree
for dirpath, dirnames, filenames in os.walk(source_root):
    for filename in filenames:
        if filename.endswith("_meta.json"):
            full_path = os.path.join(dirpath, filename)

            # Ensure uniqueness by including part of the path
            relative_path = os.path.relpath(dirpath, source_root)
            safe_name = relative_path.replace(os.sep, "_") + "_" + filename
            dest_path = os.path.join(destination, safe_name)

            # Copy the file
            shutil.copy2(full_path, dest_path)
            print(f"Copied: {full_path} -> {dest_path}")
