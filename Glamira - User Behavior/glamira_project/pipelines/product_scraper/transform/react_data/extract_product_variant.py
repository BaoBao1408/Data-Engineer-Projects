import json
import hashlib
import itertools
import re
from tqdm import tqdm
from google.cloud import storage
from config.mongo_connection import connect_mongo
from utils.utils import *

# ========================
# CONFIG
# ========================
BUCKET = "glamira-data-lake-qb"
PREFIX = "raw/product_variant/"
MAX_SIZE = 100 * 1024 * 1024

VALID_VARIANT_TYPES = {"stone", "alloy", "ctsize", "size"}

client, db = connect_mongo()
col = db["products_raw"]
bucket = storage.Client().bucket(BUCKET)


# ========================
# WRITER
# ========================
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
        blob.upload_from_string(
            "".join(self.buf),
            content_type="application/json"
        )

        print(f"✅ Uploaded {blob.name} ({self.size/1024/1024:.2f} MB)")

        self.buf, self.size = [], 0
        self.idx += 1


# ========================
# 🔥 NORMALIZE ALLOY (FIX CORE BUG)
# ========================
def normalize_alloy(value):
    if not value or "&" not in value:
        return value

    # extract karat (9k, 14k...)
    karat_match = re.search(r"\d+k", value.lower())
    karat = karat_match.group(0) if karat_match else ""

    # detect material
    material = ""
    if "gold" in value.lower():
        material = "gold"
    elif "platinum" in value.lower():
        material = "platinum"
    elif "palladium" in value.lower():
        material = "palladium"

    # remove karat + material
    tmp = value.lower()
    tmp = tmp.replace(karat, "")
    tmp = tmp.replace(material, "")

    # split colors
    parts = [p.strip() for p in tmp.split("&")]

    # clean + sort colors
    colors = sorted([p for p in parts if p])

    # rebuild canonical
    result = f"{karat} {' & '.join(colors)} {material}".strip()

    return result.title()


# ========================
# NORMALIZE VALUE
# ========================
def normalize_value(key, value):
    if not value:
        return value

    value = value.strip()

    if key == "alloy":
        return normalize_alloy(value)

    return value


# ========================
# NORMALIZE CONFIG
# ========================
def normalize_config(config):
    return {
        k: normalize_value(k, v)
        for k, v in config.items()
        if v
    }


# ========================
# VARIANT ID (CANONICAL)
# ========================
def gen_variant_id(product_id, normalized_config):
    raw = json.dumps(normalized_config, sort_keys=True)
    return hashlib.md5((product_id + raw).encode()).hexdigest()


# ========================
# PRICE
# ========================
def get_base_price(react):
    return parse_float(
        react.get("product_price", {})
             .get("prices", {})
             .get("finalPrice", {})
             .get("amount")
    )


def calc_price(base_price, option):
    price = parse_float(option.get("price"))
    price_type = safe_str(option.get("price_type"))

    if not price:
        return 0

    if price_type == "percent":
        return base_price * price / 100

    return price


# ========================
# EXTRACT
# ========================
def extract(doc):
    react = doc.get("react_data", {})
    pid = safe_str(react.get("product_id"))

    options = ensure_list(safe_json(react.get("options")))
    base_price = get_base_price(react)

    option_map = {}

    # ========================
    # BUILD OPTION MAP + DEDUP SOURCE
    # ========================
    for opt in options:
        opt_type = safe_str(opt.get("type"))

        if opt_type not in VALID_VARIANT_TYPES:
            continue

        values = ensure_list(opt.get("values"))

        clean_values = []
        seen_labels = set()

        for v in values:
            label = safe_str(v.get("title"))

            if not label:
                continue

            label = normalize_value(opt_type, label)

            # 🔥 dedup ngay từ source
            if label in seen_labels:
                continue
            seen_labels.add(label)

            clean_values.append({
                "label": label,
                "price": parse_float(v.get("price")),
                "price_type": safe_str(v.get("price_type"))
            })

        if clean_values:
            option_map[opt_type] = clean_values

    if not option_map:
        return

    keys = list(option_map.keys())
    values_product = itertools.product(*option_map.values())

    seen_variants = set()

    # ========================
    # GENERATE VARIANT
    # ========================
    for combo in values_product:
        config = {}
        total_price = base_price or 0

        for k, v in zip(keys, combo):
            config[k] = v["label"]
            total_price += calc_price(base_price, v)

        normalized_config = normalize_config(config)

        # 🔥 dedup theo business meaning
        key = json.dumps(normalized_config, sort_keys=True)

        if key in seen_variants:
            continue
        seen_variants.add(key)

        total_price = round(total_price, 2)

        yield {
            "variant_id": gen_variant_id(pid, normalized_config),
            "product_id": pid,
            "price": total_price,
            "config": normalized_config
        }


# ========================
# RUN
# ========================
def run():
    writer = Writer()
    total = 0

    for doc in tqdm(col.find({}, no_cursor_timeout=True)):
        try:
            for r in extract(doc):
                writer.write(r)
                total += 1
        except Exception as e:
            print("❌ ERROR:", e)

    writer.flush()
    print(f"🔥 TOTAL VARIANTS: {total}")


if __name__ == "__main__":
    run()