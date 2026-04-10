import json
import hashlib
from tqdm import tqdm
from google.cloud import storage
from config.mongo_connection import connect_mongo
from utils.utils import *

BUCKET = "glamira-data-lake-qb"
PREFIX = "raw/product_stone/"
MAX_SIZE = 100 * 1024 * 1024

client, db = connect_mongo()
col = db["products_raw"]
bucket = storage.Client().bucket(BUCKET)


class Writer:
    def __init__(self):
        self.buf, self.size, self.idx = [], 0, 0

    def write(self, row):
        line = json.dumps(row) + "\n"
        self.buf.append(line)
        self.size += len(line)
        if self.size >= MAX_SIZE:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        blob = bucket.blob(f"{PREFIX}part_{self.idx}.jsonl")
        blob.upload_from_string("".join(self.buf))
        print("✅", blob.name)
        self.buf, self.size = [], 0
        self.idx += 1


def gen_variant_id(product_id, sku):
    raw = f"{product_id}_{sku}"
    return hashlib.md5(raw.encode()).hexdigest()


def extract(doc):
    react = doc.get("react_data", {})
    items = ensure_list(react.get("items"))

    for item in items:
        pid = safe_str(item.get("product_id"))
        stones = ensure_list(item.get("stones"))

        for s in stones:
            yield {
                "product_id": pid,
                "stone_type": safe_str(s.get("label")),
                "carat": parse_float(s.get("carat", {}).get("value")),
                "total_carat": parse_float(s.get("total_carat", {}).get("value")),
                "shape": safe_str(s.get("shape", {}).get("label")),
                "clarity": safe_str(s.get("clarity", {}).get("label")),
                "cut": safe_str(s.get("cut", {}).get("label")),
                "colour": safe_str(s.get("colour", {}).get("label")),
                "quality": safe_str(s.get("quality", {}).get("label")),
            }


def run():
    writer = Writer()

    for doc in tqdm(col.find({}, no_cursor_timeout=True)):
        try:
            for r in extract(doc):
                writer.write(r)
        except Exception as e:
            print("❌ STONE ERROR:", e)

    writer.flush()


if __name__ == "__main__":
    run()