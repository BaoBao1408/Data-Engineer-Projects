import gzip
import json
from tqdm import tqdm
from datetime import datetime
from io import BytesIO
import gzip as gz

from google.cloud import storage

# ========================
# CONFIG
# ========================
INPUT_FILE = "data/export/glamira_raw.jsonl.gz"

BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/option/"

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB

# ========================
# GCS CLIENT
# ========================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# ========================
# GET LAST PART (RESUME)
# ========================
def get_last_part_index():
    blobs = client.list_blobs(BUCKET_NAME, prefix=PREFIX)

    max_index = 0

    for blob in blobs:
        name = blob.name.split("/")[-1]

        if name.startswith("part_") and name.endswith(".jsonl"):
            try:
                idx = int(name.replace("part_", "").replace(".jsonl", ""))
                max_index = max(max_index, idx)
            except:
                pass

    return max_index


# ========================
# TRANSFORM OPTION
# ========================
def extract_options(record):
    try:
        ts = record.get("time_stamp")
        if not ts:
            return []

        base = {
            "event_id": str(record.get("_id")),
            "event_time": datetime.fromtimestamp(ts).isoformat(),
            "event_type": record.get("collection"),
            "session_id": record.get("device_id"),
            "product_id": record.get("product_id"),
        }

        options = record.get("option", [])
        results = []

        for opt in options:
            if not opt:
                continue

            row = base.copy()

            # core fields
            row["option_label"] = opt.get("option_label")
            row["option_value"] = opt.get("value")
            row["option_id"] = opt.get("option_id")

            # 🔥 flatten dynamic + nested
            for k, v in opt.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        row[f"{k}_{sub_k}"] = sub_v
                else:
                    if k not in row:
                        row[k] = v

            results.append(row)

        return results

    except Exception:
        return []


# ========================
# UPLOAD FUNCTION
# ========================
def upload_chunk(buffer_list, file_index):

    blob_name = f"{PREFIX}part_{file_index}.jsonl"   
    blob = bucket.blob(blob_name)

    if blob.exists():
        print(f"⚠️ Skip existing: {blob_name}")
        return False

    bio = BytesIO()

    bio.write("".join(buffer_list).encode("utf-8"))
    bio.seek(0)

    blob.upload_from_file(bio, content_type="application/json")

    print(f"✅ Uploaded: {blob_name}")
    return True


# ========================
# MAIN
# ========================
last_part = get_last_part_index()
file_index = last_part + 1

print(f"🔁 Resume from part_{file_index}")

buffer = []
buffer_size = 0
total_processed = 0

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f:

    for line in tqdm(f, desc="Option Processing"):

        try:
            record = json.loads(line)
        except:
            continue

        option_rows = extract_options(record)

        for row in option_rows:
            json_line = json.dumps(row, ensure_ascii=False) + "\n"

            buffer.append(json_line)
            buffer_size += len(json_line.encode("utf-8"))

            total_processed += 1

            # 🔥 split theo size
            if buffer_size >= MAX_FILE_SIZE:

                success = upload_chunk(buffer, file_index)

                buffer = []
                buffer_size = 0

                file_index += 1


# ========================
# FINAL
# ========================
if buffer:
    upload_chunk(buffer, file_index)

print(f"🎉 DONE OPTION PIPELINE: {total_processed} rows")