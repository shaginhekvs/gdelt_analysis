import os
import glob
import gzip
import json

# Configuration
DATA_DIR = os.getenv("DATA_DIR", "data_saved")
CACHE_DIR = os.path.join(DATA_DIR, "cache")

def cat_cache_files():
    """Read all .json.gz files in CACHE_DIR and print 10 lines from each."""
    cache_files = glob.glob(os.path.join(CACHE_DIR, "*.json.gz"))
    cache_files.sort(reverse=True)  # Most recent first

    if not cache_files:
        print("No .json.gz files found in CACHE_DIR.")
        return

    for i, file in enumerate(cache_files):
        print(f"\n--- Cache File {i+1}: {os.path.basename(file)} ---")
        try:
            with gzip.open(file, 'rt', encoding='utf-8') as gz:
                lines = gz.readlines()
                for j, line in enumerate(lines[:10]):  # Print first 10 lines
                    try:
                        data = json.loads(line.strip())
                        print(f"Line {j+1}: {data}")
                    except json.JSONDecodeError:
                        print(f"Line {j+1}: {line.strip()}")
        except Exception as e:
            print(f"Error reading {file}: {e}")

    print(f"\nProcessed {len(cache_files)} cache files.")

if __name__ == "__main__":
    cat_cache_files()
