#!/usr/bin/env python3
"""
Combine many JSON files into one compressed JSONL file.

Result: a gzip-compressed file where each line is one JSON object
(easier to stream/process than one gigantic JSON array).

Usage:
    python combine_jsons.py \
        --input-dir /path/to/jsons \
        --output combined.jsonl.gz
"""

import argparse
import gzip
import json
import os
from pathlib import Path
from typing import Iterable


def iter_json_files(input_dir: Path, pattern: str = "*.json") -> Iterable[Path]:
    """
    Recursively yield all JSON files under input_dir matching pattern.
    """
    for path in input_dir.rglob(pattern):
        if path.is_file():
            yield path


def read_json(path: Path):
    """
    Read a JSON file and return the parsed object.
    Raises json.JSONDecodeError if invalid.
    """
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def combine_to_gzip_jsonl(input_dir: Path, output_path: Path, pattern: str = "*.json") -> None:
    """
    Read all JSON files under input_dir and write them as JSON Lines
    into a gzip-compressed output file.
    """
    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with gzip.open(output_path, "wt", encoding="utf-8") as out_f:
        for json_path in iter_json_files(input_dir, pattern):
            try:
                obj = read_json(json_path)
            except json.JSONDecodeError as e:
                # You can change this to "continue" if you want to skip bad files
                raise RuntimeError(f"Failed to parse JSON in {json_path}: {e}") from e

            out_f.write(json.dumps(obj, separators=(",", ":")))
            out_f.write("\n")
            count += 1

    print(f"Written {count} JSON objects to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine many JSON files into one gzip-compressed JSONL file."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        type=Path,
        help="Directory containing JSON files (searched recursively).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output .gz file (e.g. combined.jsonl.gz).",
    )
    parser.add_argument(
        "--pattern",
        default="*.json",
        help="Glob pattern for JSON files (default: *.json).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.input_dir.is_dir():
        raise SystemExit(f"Input dir does not exist or is not a directory: {args.input_dir}")
    combine_to_gzip_jsonl(args.input_dir, args.output, args.pattern)


if __name__ == "__main__":
    main()
