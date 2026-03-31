import gzip
import json
import os
from tqdm import tqdm
from datetime import datetime

# ========================
# CONFIG
# ========================
INPUT_FILE = "data/export/glamira_raw.jsonl.gz"
OUTPUT_DIR = "data/glamira_option"
PROGRESS_FILE = "data/glamira_option/option_progress.txt"

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

print(f"[OPTION] Resume from line: {progress}")


# ========================
# INIT OUTPUT
# ========================
file_index = 1
row_count = 0

output_path = f"{OUTPUT_DIR}/part_{file_index}.jsonl"
out_file = open(output_path, "a")

print(f"[OPTION] Writing to: {output_path}")


# ========================
# TRANSFORM OPTION
# ========================
def extract_options(record):
    try:
        ts = record.get("time_stamp")
        if not ts:
            return []

        event_id = str(record.get("_id"))
        event_time = datetime.fromtimestamp(ts).isoformat()
        session_id = record.get("device_id")
        product_id = record.get("product_id")
        event_type = record.get("collection")

        options = record.get("option", [])

        results = []

        for opt in options:

            # skip option rỗng
            if not opt:
                continue

            base = {
                "event_id": event_id,
                "event_time": event_time,
                "event_type": event_type,
                "session_id": session_id,
                "product_id": product_id,
                "option_label": opt.get("option_label"),
            }

            # 🔥 dynamic fields
            for k, v in opt.items():
                base[k] = v

            results.append(base)

        return results

    except Exception:
        return []

# ========================
# MAIN
# ========================
total_processed = 0

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f:
    for i, line in enumerate(tqdm(f, desc="Option Processing", unit="lines")):

        if i < progress:
            continue

        try:
            record = json.loads(line)
        except:
            continue

        options = extract_options(record)

        for opt in options:
            out_file.write(json.dumps(opt) + "\n")

            row_count += 1
            total_processed += 1

            # flush
            if total_processed % FLUSH_INTERVAL == 0:
                out_file.flush()

                with open(PROGRESS_FILE, "w") as pf:
                    pf.write(str(i))

            # split
            if row_count >= CHUNK_SIZE:
                out_file.close()

                file_index += 1
                row_count = 0

                output_path = f"{OUTPUT_DIR}/part_{file_index}.jsonl"
                out_file = open(output_path, "a")

                print(f"[OPTION] Switch file: {output_path}")


# ========================
# FINALIZE
# ========================
out_file.close()

with open(PROGRESS_FILE, "w") as pf:
    pf.write(str(i))

print(f"[OPTION] Done. Total option rows: {total_processed}")