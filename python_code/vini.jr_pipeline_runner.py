#!/usr/bin/env python3
"""
Orchestrates the full usage pipeline:
  1. capture tables
  2. score query complexity
  3. analyse usage
Usage:
  python vini.jr_pipeline_runner.py NFL_query_test_Jan_June.csv
"""
import subprocess
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
RAW_DIR = ROOT.parent / "raw_data"
OUT_DIR = ROOT.parent / "curated_output"
OUT_DIR.mkdir(exist_ok=True)

def run(cmd):
    start = time.time()
    print(f"→ {cmd}")
    result = subprocess.run(cmd, shell=True, check=True)
    print(f"   finished in {time.time()-start:.1f}s")
    return result

def main(raw_file):
    raw_path = RAW_DIR / raw_file
    captured = OUT_DIR / "query_table_captured.csv"
    scored   = OUT_DIR / "categorized_queries.csv"
    final    = OUT_DIR / "usage_analysis.csv"
    
    run(f"python query_table_capture.py {raw_file}")
    run(f"python query_categorization.py query_table_captured.csv")
    run(f"python query_usage_quicklook.py categorized_queries.csv")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python vini.jr_pipeline_runner.py <raw_csv>")
    main(sys.argv[1])