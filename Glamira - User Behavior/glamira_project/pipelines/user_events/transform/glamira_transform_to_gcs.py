import gzip
import json
from tqdm import tqdm
from datetime import datetime
from google.cloud import storage

# ========================
# CONFIG
# ========================
INPUT_FILE = "data/export/glamira_raw.jsonl.gz"

BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/glamira/"

CHUNK_SIZE = 1_000_000  
FLUSH_INTERVAL = 10000

# ========================
# GCS
# ========================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# ========================
# TRANSFORM
# ========================
def transform(record):
    ts = record.get("time_stamp")
    if not ts:
        return None

    return {
        "event_id": str(record.get("_id")),
        "event_time": datetime.fromtimestamp(ts).isoformat(),
        "event_type": record.get("collection"),

        # user
        "user_id": record.get("user_id_db"),
        "session_id": record.get("device_id"),

        # product
        "product_id": record.get("product_id"),

        # device
        "ip": record.get("ip"),
        "user_agent": record.get("user_agent"),
        "device": "mobile" if "Mobile" in (record.get("user_agent") or "") else "desktop",
        "resolution": record.get("resolution"),

        # navigation
        "current_url": record.get("current_url"),
        "referrer_url": record.get("referrer_url"),

        # business
        "store_id": record.get("store_id"),

        # tracking
        "utm_source": record.get("utm_source"),
        "utm_medium": record.get("utm_medium"),
        "recommendation": record.get("recommendation"),

        # time extra
        "local_time": record.get("local_time"),
    }

# ========================
# MAIN STREAM WRITE
# ========================
file_index = 1
row_count = 0

blob = bucket.blob(f"{PREFIX}part_{file_index}.jsonl")

gcs_file = blob.open("w")

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f:
    for i, line in enumerate(tqdm(f, desc="Processing")):

        try:
            record = json.loads(line)
        except:
            continue

        transformed = transform(record)
        if not transformed:
            continue

        # 👇 write trực tiếp (NO RAM buffer)
        gcs_file.write(json.dumps(transformed) + "\n")

        row_count += 1

        # flush
        if row_count % FLUSH_INTERVAL == 0:
            gcs_file.flush()

        # split file
        if row_count >= CHUNK_SIZE:
            gcs_file.close()
            print(f"Uploaded part_{file_index}")

            file_index += 1
            row_count = 0

            blob = bucket.blob(f"{PREFIX}part_{file_index}.jsonl")
            gcs_file = blob.open("w")

gcs_file.close()

print("Done")