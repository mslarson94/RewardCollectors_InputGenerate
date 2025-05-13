import argparse
from eventCascadeBuilder_proprosed import process_all_obsreward_files
import eventParser_legacy_AN as parser_legacy
import eventParser_modern_AN as parser_modern

def main():
    parser = argparse.ArgumentParser(description="AN Cascading Script with Versioned Parser")
    parser.add_argument("--dataDir", required=True, help="Path to the root data directory")
    parser.add_argument("--metadata", required=True, help="Path to the metadata Excel file")
    parser.add_argument("--allowed_statuses", nargs="+", default=["complete", "truncated"])
    parser.add_argument("--subDirs", nargs="*", help="List of subdirectories to process")
    parser.add_argument("--version", choices=["legacy", "modern"], default="legacy", help="Which version of parser to use")
    parser.add_argument("--role", choices=["AN", "PO"], required=True, help="Role of the participant")

    args = parser.parse_args()

    parser_map = {
        "legacy": parser_legacy,
        "modern": parser_modern
    }

    parser_module = parser_map[args.version]

    process_all_obsreward_files(
        dataDir=args.dataDir,
        metadata=args.metadata,
        allowed_statuses=args.allowed_statuses,
        subDirs=args.subDirs,
        role=args.role,
        parser_module=parser_module
    )

if __name__ == "__main__":
    main()
