import logging
import json
import os
import time
from urllib.parse import urlparse

from config.mongo_connection import connect_mongo


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


EVENTS = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed"
]

SPECIAL_EVENT = "product_view_all_recommend_clicked"

OUTPUT_FILE = "data/raw/product_urls.jsonl"
CHECKPOINT_FILE = "data/raw/checkpoint.txt"

os.makedirs("data/raw", exist_ok=True)


# ------------------------------------------------
# checkpoint
# ------------------------------------------------

def load_checkpoint():

    if os.path.exists(CHECKPOINT_FILE):

        with open(CHECKPOINT_FILE) as f:
            return f.read().strip()

    return None


def save_checkpoint(_id):

    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(_id))


# ------------------------------------------------
# url normalize
# ------------------------------------------------

def normalize_url(url):

    try:

        parsed = urlparse(url)

        if "glamira." not in parsed.netloc:
            return None

        path = parsed.path.lower()

        if not path.endswith(".html"):
            # return None
            print(url)

        return f"{parsed.scheme}://{parsed.netloc}{path}"

    except Exception:

        return None


# ------------------------------------------------
# main pipeline
# ------------------------------------------------

def run():

    db = connect_mongo()

    collection = db["glamira_raw"]

    resume_id = load_checkpoint()

    logging.info(f"Resume from _id: {resume_id}")

    query = {
        "collection": {"$in": EVENTS + [SPECIAL_EVENT]}
    }

    if resume_id:

        query["_id"] = {"$gt": resume_id}

    cursor = collection.find(
        query,
        {
            "_id": 1,
            "product_id": 1,
            "viewing_product_id": 1,
            "current_url": 1,
            "referrer_url": 1
        },
        batch_size=10000,
        no_cursor_timeout=True
    )

    seen_products = set()

    processed = 0
    saved = 0

    start = time.time()

    with open(OUTPUT_FILE, "a", encoding="utf8") as f:

        for doc in cursor:

            processed += 1

            product_id = doc.get("product_id") or doc.get("viewing_product_id")

            url = doc.get("current_url") or doc.get("referrer_url")

            if not product_id or not url:
                continue

            # dedupe product_id
            if product_id in seen_products:
                continue

            normalized = normalize_url(url)

            if not normalized:
                continue

            seen_products.add(product_id)

            record = {
                "product_id": product_id,
                "url": normalized
            }

            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            saved += 1

            save_checkpoint(doc["_id"])

            if processed % 50000 == 0:

                elapsed = round(time.time() - start, 1)

                logging.info(
                    f"Processed {processed:,} | "
                    f"Saved {saved:,} | "
                    f"Time {elapsed}s"
                )

    logging.info("Finished")
    logging.info(f"Total processed: {processed:,}")
    logging.info(f"Unique products saved: {saved:,}")


if __name__ == "__main__":
    run()