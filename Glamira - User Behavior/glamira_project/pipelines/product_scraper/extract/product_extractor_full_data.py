import json
from tqdm import tqdm
from urllib.parse import urlparse
from google.cloud import storage

from config.mongo_connection import connect_mongo


# =========================
# CONFIG
# =========================

BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/raw_products"

BATCH_SIZE = 10000


DOMAIN_CURRENCY = {

    # DEFAULT
    "glamira.com": "USD",

    # EUROPE
    "glamira.de": "EUR",
    "glamira.fr": "EUR",
    "glamira.it": "EUR",
    "glamira.es": "EUR",
    "glamira.nl": "EUR",
    "glamira.be": "EUR",
    "glamira.at": "EUR",
    "glamira.fi": "EUR",
    "glamira.pt": "EUR",
    "glamira.ie": "EUR",
    "glamira.sk": "EUR",
    "glamira.si": "EUR",
    "glamira.ee": "EUR",
    "glamira.lv": "EUR",
    "glamira.lt": "EUR",
    "glamira.hr": "EUR",

    # NON-EU EUROPE
    "glamira.ch": "CHF",
    "glamira.no": "NOK",
    "glamira.se": "SEK",
    "glamira.dk": "DKK",
    "glamira.pl": "PLN",
    "glamira.hu": "HUF",
    "glamira.cz": "CZK",
    "glamira.ro": "RON",
    "glamira.bg": "BGN",
    "glamira.rs": "RSD",
    "glamira.is": "ISK",
    "glamira.md": "MDL",

    # UK
    "glamira.co.uk": "GBP",

    # AMERICAS
    "glamira.com.au": "AUD",
    "glamira.ca": "CAD",
    "glamira.com.mx": "MXN",
    "glamira.com.br": "BRL",
    "glamira.com.ar": "ARS",
    "glamira.com.co": "COP",
    "glamira.com.pe": "PEN",
    "glamira.com.cl": "CLP",
    "glamira.com.gt": "GTQ",
    "glamira.com.hn": "HNL",
    "glamira.com.cr": "CRC",
    "glamira.com.pa": "USD",
    "glamira.com.do": "DOP",
    "glamira.com.sv": "USD",
    "glamira.com.uy": "UYU",
    "glamira.com.bo": "BOB",

    # APAC
    "glamira.sg": "SGD",
    "glamira.com.my": "MYR",
    "glamira.co.th": "THB",
    "glamira.vn": "VND",
    "glamira.jp": "JPY",
    "glamira.cn": "CNY",
    "glamira.hk": "HKD",
    "glamira.tw": "TWD",
    "glamira.kr": "KRW",
    "glamira.in": "INR",
    "glamira.co.nz": "NZD",
    "glamira.com.ph": "PHP",

    # MIDDLE EAST
    "glamira.ae": "AED",
    "glamira.com.tr": "TRY",
    "glamira.sa": "SAR",
    "glamira.az": "AZN",

    # AFRICA
    "glamira.co.za": "ZAR",
}


# =========================
# INIT
# =========================

gcs_client = storage.Client()
bucket = gcs_client.bucket(BUCKET_NAME)

mongo_client, db = connect_mongo()
collection = db["raw_products"]


# =========================
# HELPER
# =========================

def normalize_domain(domain):
    domain = domain.lower()

    if domain.startswith("www."):
        domain = domain[4:]

    if domain.startswith("m."):
        domain = domain[2:]

    return domain


def get_currency(url):

    domain = normalize_domain(urlparse(url).netloc)

    if domain in DOMAIN_CURRENCY:
        return DOMAIN_CURRENCY[domain]

    # fallback theo TLD
    if domain.endswith(".de") or domain.endswith(".nl"):
        return "EUR"
    if domain.endswith(".uk"):
        return "GBP"
    if domain.endswith(".au"):
        return "AUD"

    return "USD"


# =========================
# TRANSFORM
# =========================

def transform(doc):

    data = doc.get("react_data", {})
    url = doc.get("url", "")

    currency = get_currency(url)

    return {
        "product_id": data.get("product_id"),
        "name": data.get("name"),

        "collection_id": data.get("collection_id"),
        "collection": data.get("collection"),

        "category_id": data.get("category"),
        "category_name": data.get("category_name"),

        "type_id": data.get("type_id"),

        "sku": data.get("sku"),
        "gender": data.get("gender"),

        "attribute_set_id": data.get("attribute_set_id"),
        "attribute_set": data.get("attribute_set"),

        "store_code": data.get("store_code"),

        "product_type": data.get("product_type"),
        "product_type_value": data.get("product_type_value"),

        "price": float(data.get("price", 0)),
        "min_price": float(data.get("min_price", 0)),
        "max_price": float(data.get("max_price", 0)),

        "currency": currency,   # 🔥 theo yêu cầu của bạn

        "url": url
    }


# =========================
# UPLOAD
# =========================

def upload_to_gcs():

    buffer = []
    file_index = 1

    def flush():
        nonlocal buffer, file_index

        if not buffer:
            return

        blob = bucket.blob(f"{PREFIX}/product_{file_index}.jsonl")

        blob.upload_from_string(
            "\n".join(buffer),
            content_type="application/json"
        )

        buffer = []
        file_index += 1

    cursor = collection.find({
        "react_data": {"$exists": True}
    })

    for doc in tqdm(cursor):

        row = transform(doc)

        buffer.append(json.dumps(row))

        if len(buffer) >= BATCH_SIZE:
            flush()

    flush()


# =========================
# RUN
# =========================

if __name__ == "__main__":
    upload_to_gcs()