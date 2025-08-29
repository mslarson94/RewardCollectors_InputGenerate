
import argparse
import os
import pandas as pd
from eventCascadeBuilder_AN import (process_all_obsreward_files, process_file_list)

def main():
    parser = argparse.ArgumentParser(description="Cascade builder for events")

    parser.add_argument("--dataDir", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--allowed_statuses", nargs="+", default=["complete", "truncated"])
    parser.add_argument("--subDirs", nargs="*")
    parser.add_argument("--file_list", nargs="*")

    args = parser.parse_args()
    import sys
    print(f"🧾 sys.argv: {sys.argv}")
    print(f"📁 subDirs: {args.subDirs}")
    print(f"📥 allowed_statuses: {args.allowed_statuses}")
    print(f"📁 subDirs: {args.subDirs}")

    if args.file_list:
        if len(args.file_list) == 0:
            raise ValueError("Mode is 'filelist' but no files were provided with --file_list.")
        process_file_list(args.file_list, args.metadata, args.allowed_statuses)
    else:
        print(f"📂 dataDir: {args.dataDir}")
        print(f"📂 ProcessedData inside: {os.listdir(os.path.join(args.dataDir, 'ProcessedData'))}")

        subDirs = args.subDirs if args.subDirs else None
        process_all_obsreward_files(
                                    dataDir=args.dataDir,
                                    metadata=args.metadata,
                                    subDirs=subDirs,
                                    allowed_statuses=args.allowed_statuses
                                )

        #process_all_obsreward_files(args.dataDir, args.metadata, args.allowed_statuses, subDirs)


if __name__ == "__main__":
    main()

"""
cli_script_path = Path("/mnt/data/integration_with_logger_cli.py")
cli_script_path.write_text(cli_script_content)
cli_script_path.name
if __name__ == "__main__":
    main()
"""



