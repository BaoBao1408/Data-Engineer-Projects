import gzip
import json

file_path = "data/export/glamira_raw.jsonl.gz"

with gzip.open(file_path, "rt", encoding="utf-8") as f:
    for i in range(5):
        line = next(f)
        record = json.loads(line)
        print(f"\n--- RECORD {i+1} ---")
        print(json.dumps(record, indent=2, ensure_ascii=False))