from config.mongo_connection import connect_mongo
from urllib.parse import urlparse
import csv
import time
import os
from tqdm import tqdm


db = connect_mongo()

event_types = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed"
]

OUTPUT_FILE = "data/raw/product_urls.csv"
CHECKPOINT_FILE = "data/raw/processing_id.txt"

os.makedirs("data/raw", exist_ok=True)

seen_products = set()

processed = 0
saved = 0
start = time.time()

# ----------------------------
# load checkpoint
# ----------------------------

last_product_id = None
resume_processed = 0

if os.path.exists(CHECKPOINT_FILE):

    with open(CHECKPOINT_FILE) as f:

        line = f.read().strip()

        if line:

            parts = line.split(",")

            if len(parts) == 2:
                last_product_id = int(parts[0])
                resume_processed = int(parts[1])

print("Resume product_id:", last_product_id)
print("Resume processed:", resume_processed)

# ----------------------------
# total records
# ----------------------------

total_records = db.glamira_raw.count_documents(
    {"collection": {"$in": event_types}}
)

print("Total events:", total_records)

# ----------------------------
# cursor
# ----------------------------

cursor = db.glamira_raw.find(
    {
        "collection": {"$in": event_types},
        "current_url": {"$regex": "glamira-.*\\.html"}
    },
    {
        "current_url": 1,
        "product_id": 1,
        "viewing_product_id": 1
    },
    batch_size=10000,
    no_cursor_timeout=True
)

# ----------------------------
# progress bar
# ----------------------------

pbar = tqdm(total=total_records, initial=resume_processed)

# ----------------------------
# open files
# ----------------------------

with open(OUTPUT_FILE, "a", newline="", encoding="utf8") as csv_file, \
     open(CHECKPOINT_FILE, "w") as checkpoint:

    writer = csv.writer(csv_file)

    for doc in cursor:

        processed += 1
        pbar.update(1)

        url = doc.get("current_url")

        product_id = doc.get("product_id") or doc.get("viewing_product_id")

        if not url or not product_id:
            continue

        if last_product_id and product_id <= last_product_id:
            continue

        if product_id in seen_products:
            continue

        try:
            parsed = urlparse(url)
        except:
            continue

        host = parsed.netloc.lower()

        if "glamira." not in host:
            continue

        if host.startswith(("stage.", "dev.", "test.")):
            continue

        path = parsed.path.lower()

        if not path.endswith(".html"):
            continue

        base_url = f"{parsed.scheme}://{host}{path}"

        seen_products.add(product_id)

        writer.writerow([product_id, base_url])

        saved += 1

        # flush csv
        if saved % 100 == 0:
            csv_file.flush()

        # update checkpoint
        if processed % 1000 == 0:

            checkpoint.seek(0)
            checkpoint.write(f"{product_id},{processed}")
            checkpoint.truncate()
            checkpoint.flush()

        # log speed
        if processed % 50000 == 0:

            elapsed = round(time.time() - start, 1)

            print(
                f"\nProcessed: {processed:,} | Products: {saved:,} | Time: {elapsed}s"
            )

pbar.close()

print("\nFinished")
print("Total processed:", processed)
print("Unique products:", saved)