import json
from tqdm import tqdm
from google.cloud import storage
from config.mongo_connection import connect_mongo
from utils.utils import *

BUCKET = "glamira-data-lake-qb"
PREFIX = "raw/product_option_value/"
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


def extract(doc):
    react = doc.get("react_data", {})
    pid = safe_str(react.get("product_id"))

    options = ensure_list(safe_json(react.get("options")))

    for opt in options:
        oid = safe_str(opt.get("option_id"))
        opt_type = safe_str(opt.get("type"))

        values = ensure_list(opt.get("values"))

        for v in values:
            # skip junk
            if not v:
                continue

            # detect stone
            stones = ensure_dict(v.get("data_stones"))

            yield {
                "product_id": pid,
                "option_id": oid,
                "option_type": opt_type,

                "option_type_id": safe_str(v.get("option_type_id")),
                "label": safe_str(v.get("title")),

                "price": parse_float(v.get("price")),
                "price_type": safe_str(v.get("price_type")),

                "stone_group": safe_str(v.get("stone_group")),
                "is_default": v.get("is_default", False),
            }


def run():
    writer = Writer()

    for doc in tqdm(col.find({}, no_cursor_timeout=True)):
        try:
            for r in extract(doc):
                writer.write(r)
        except Exception as e:
            print("❌ VALUE ERROR:", e)

    writer.flush()


if __name__ == "__main__":
    run()