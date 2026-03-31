import gzip
import json
import os
from tqdm import tqdm
from datetime import datetime

# ========================
# CONFIG
# ========================
INPUT_FILE = "data/export/glamira_raw.jsonl.gz"
OUTPUT_DIR = "data/glamira_raw"
PROGRESS_FILE = "data/glamira_raw/progress.txt"

CHUNK_SIZE = 1_000_000
FLUSH_INTERVAL = 10000

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ========================
# LOAD PROGRESS
# ========================
progress = 0

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        try:
            progress = int(f.read().strip())
        except:
            progress = 0

print(f"Resume from line: {progress}")


# ========================
# DETECT CURRENT FILE INDEX
# ========================
existing_parts = [
    int(f.split("_")[1].split(".")[0])
    for f in os.listdir(OUTPUT_DIR)
    if f.startswith("part_") and f.endswith(".jsonl")
]

file_index = max(existing_parts) if existing_parts else 1

output_path = f"{OUTPUT_DIR}/part_{file_index}.jsonl"

# count existing rows in last file
row_count = 0
if os.path.exists(output_path):
    with open(output_path, "r") as f:
        row_count = sum(1 for _ in f)

out_file = open(output_path, "a")

print(f"Writing to: {output_path} (current rows: {row_count})")


# ========================
# TRANSFORM
# ========================
def transform(record):
    try:
        ts = record.get("time_stamp")
        if not ts:
            return None

        return {
            "event_id": str(record.get("_id")),
            "event_time": datetime.fromtimestamp(ts).isoformat(),
            "event_type": record.get("collection"),
            "user_id": record.get("user_id_db"),
            "session_id": record.get("device_id"),
            "product_id": record.get("product_id"),
            "ip": record.get("ip"),
            "user_agent": record.get("user_agent"),
            "device": "mobile" if "Mobile" in (record.get("user_agent") or "") else "desktop",
            "resolution": record.get("resolution"),
            "store_id": record.get("store_id"),
            "url": record.get("current_url"),
            "referrer": record.get("referrer_url"),
        }
    except Exception:
        return None


# ========================
# MAIN PROCESS
# ========================
total_processed = 0

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f:
    for i, line in enumerate(tqdm(f, desc="Processing", unit="lines")):

        # skip processed
        if i < progress:
            continue

        try:
            record = json.loads(line)
        except Exception:
            continue

        transformed = transform(record)
        if not transformed:
            continue

        # write
        out_file.write(json.dumps(transformed) + "\n")

        row_count += 1
        total_processed += 1

        # flush + save progress
        if total_processed % FLUSH_INTERVAL == 0:
            out_file.flush()

            with open(PROGRESS_FILE, "w") as pf:
                pf.write(str(i))

        # split file
        if row_count >= CHUNK_SIZE:
            out_file.close()

            file_index += 1
            row_count = 0

            output_path = f"{OUTPUT_DIR}/part_{file_index}.jsonl"
            out_file = open(output_path, "a")

            print(f"Switch to new file: {output_path}")

        # log
        if total_processed % 1_000_000 == 0:
            print(f"Processed {total_processed:,} records")


# ========================
# FINALIZE
# ========================
out_file.close()

with open(PROGRESS_FILE, "w") as pf:
    pf.write(str(i))

print(f"Done. Total processed: {total_processed}")