import json
import hashlib
from tqdm import tqdm
from google.cloud import storage
from config.mongo_connection import connect_mongo
from utils.utils import *

BUCKET = "glamira-data-lake-qb"
PREFIX = "raw/variant_option_map/"
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
    pid = safe_str(react.get("product_id"))

    quick_options = ensure_list(react.get("quick_options"))

    for q in quick_options:
        reqs = ensure_list(q.get("request_values"))

        for r in reqs:
            sku = safe_str(r.get("sku"))
            if not sku:
                continue

            variant_id = gen_variant_id(pid, sku)

            yield {
                "variant_id": variant_id,
                "product_id": pid,
                "option_id": safe_str(r.get("option_id")),
                "value": safe_str(r.get("value")),
                "sku": sku,
                "option_price": parse_float(r.get("optionPrice")),
            }
def run():
    writer = Writer()

    for doc in tqdm(col.find({}, no_cursor_timeout=True)):
        try:
            for r in extract(doc):
                writer.write(r)
        except Exception as e:
            print("❌ MAP ERROR:", e)

    writer.flush()


if __name__ == "__main__":
    run()