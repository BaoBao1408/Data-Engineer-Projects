import json
import gzip
from pathlib import Path
from tqdm import tqdm
from config.mongo_connection import connect_mongo
# =========================
# CONFIG
# =========================

client, db  = connect_mongo()
DB_NAME = "glamira"

OUTPUT_DIR = Path("data/export")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)



def json_safe(doc):
    return json.dumps(doc, default=str, ensure_ascii=False)


# =========================
# GENERIC EXPORT (STREAM + GZIP)
# =========================

def export_stream(collection_name, output_file, transform=None):

    if output_file.exists():
        print(f"⚠️ SKIP {collection_name} (file exists): {output_file}")
        return
        
    collection = db[collection_name]

    count = 0

    print(f"🚀 Start export: {collection_name}")

    with gzip.open(output_file, "wt", encoding="utf-8") as f:

        cursor = collection.find({}).batch_size(1000)

        total = collection.estimated_document_count()

        pbar = tqdm(cursor, total=total, desc=collection_name)

        for doc in pbar:

            if transform:
                doc = transform(doc)

            f.write(json_safe(doc) + "\n")

            count += 1

            if count % 100000 == 0:
                pbar.set_postfix(count=count)


        # for doc in tqdm(cursor, desc=collection_name):

        #     if transform:
        #         doc = transform(doc)

        #     f.write(json_safe(doc) + "\n")

        #     count += 1

        #     if count % 100000 == 0:
        #         tqdm.write(f"{collection_name}: {count}")

    print(f"✅ DONE {collection_name}: {count}")


# =========================
# MAIN
# =========================

def main():

    # 1️⃣ Export raw user behavior
    export_stream(
        collection_name="glamira_raw",
        output_file=OUTPUT_DIR / "glamira_raw.jsonl.gz"
    )

    # 2️⃣ Export product (flatten)
    export_stream(
        collection_name="products_raw",
        output_file=OUTPUT_DIR / "products_raw.jsonl.gz",
        # transform=flatten_product
    )


if __name__ == "__main__":
    main()