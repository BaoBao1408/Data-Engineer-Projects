import logging
import json

logging.basicConfig(level=logging.INFO)

EVENTS = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed"
]

SPECIAL_EVENTS = "product_view_all_recommend_clicked"

def product_map(db):
    logging.info("Start extracting product urls")
    collection = db["summary"]
    pipeline = [
        {
            "$match": {
                "collection": {
                    "$in": EVENTS + [SPECIAL_EVENTS]
                }
            }
        },
        {
            "$project": {
                "product_id": {
                    "$ifNull": ["$product_id", "$viewing_product_id"]
                },
                "url": {
                    "$cond": [
                        {"$eq": ["$collection", SPECIAL_EVENTS]},
                        "$referrer_url",
                        "$current_url"
                    ]
                }
            }
        },
        {
            "$match": {
                "product_id": {"$ne": None},
                "url": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "product_id": "$product_id",
                    "url": "$url"
                }
            }
        }
    ]
    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
        batchSize=10000
    )
    products = {}
    for doc in cursor:
        pid = doc["_id"]["product_id"]
        url = doc["_id"]["url"]
        if pid not in products:
            products[pid] = set()
        products[pid].add(url)
    # convert set 
    products = {k: list(v) for k, v in products.items()}
    logging.info(f"Collected {len(products)} unique product_ids")
    return products

def save_urls(products, output_path):
    logging.info(f"Saving results to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        for pid, urls in products.items():
            record = {
                "product_id": pid,
                "urls": urls
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    logging.info("Saved JSONL successfully")