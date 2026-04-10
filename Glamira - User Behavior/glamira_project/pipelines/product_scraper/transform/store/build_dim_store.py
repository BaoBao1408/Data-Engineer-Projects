import json
from tqdm import tqdm
from google.cloud import storage
from config.mongo_connection import connect_mongo

# =========================
# CONFIG
# =========================
BUCKET = "glamira-data-lake-qb"
PREFIX = "raw/store/"
MAX_SIZE = 20 * 1024 * 1024

client, db = connect_mongo()
collection = db["products_raw"]

bucket = storage.Client().bucket(BUCKET)

# =========================
# STORE DIM MAPPING
# =========================

STORE_DIM = {
    "glus": {"country": "United States", "region": "NA", "currency": "USD"},
    "glca": {"country": "Canada", "region": "NA", "currency": "CAD"},
    "glgb": {"country": "United Kingdom", "region": "EU", "currency": "GBP"},
    "glde": {"country": "Germany", "region": "EU", "currency": "EUR"},
    "glfr": {"country": "France", "region": "EU", "currency": "EUR"},
    "glit": {"country": "Italy", "region": "EU", "currency": "EUR"},
    "glnl": {"country": "Netherlands", "region": "EU", "currency": "EUR"},
    "glpl": {"country": "Poland", "region": "EU", "currency": "PLN"},
    "glcz": {"country": "Czech Republic", "region": "EU", "currency": "CZK"},
    "glro": {"country": "Romania", "region": "EU", "currency": "RON"},
    "glhu": {"country": "Hungary", "region": "EU", "currency": "HUF"},
    "glse": {"country": "Sweden", "region": "EU", "currency": "SEK"},
    "gldk": {"country": "Denmark", "region": "EU", "currency": "DKK"},
    "glno": {"country": "Norway", "region": "EU", "currency": "NOK"},
    "glch": {"country": "Switzerland", "region": "EU", "currency": "CHF"},
    "gljp": {"country": "Japan", "region": "APAC", "currency": "JPY"},
    "glkr": {"country": "South Korea", "region": "APAC", "currency": "KRW"},
    "glcn": {"country": "China", "region": "APAC", "currency": "CNY"},
    "glvn": {"country": "Vietnam", "region": "APAC", "currency": "VND"},
    "glin": {"country": "India", "region": "APAC", "currency": "INR"},
    "glau": {"country": "Australia", "region": "APAC", "currency": "AUD"},
    "glmy": {"country": "Malaysia", "region": "APAC", "currency": "MYR"},
    "glph": {"country": "Philippines", "region": "APAC", "currency": "PHP"},
    "glmx": {"country": "Mexico", "region": "LATAM", "currency": "MXN"},
    "glbr": {"country": "Brazil", "region": "LATAM", "currency": "BRL"},
    "glcl": {"country": "Chile", "region": "LATAM", "currency": "CLP"},
    "glpe": {"country": "Peru", "region": "LATAM", "currency": "PEN"},
    "glcr": {"country": "Costa Rica", "region": "LATAM", "currency": "CRC"},
    "glgt": {"country": "Guatemala", "region": "LATAM", "currency": "GTQ"},
    "glhn": {"country": "Honduras", "region": "LATAM", "currency": "HNL"},
    "glpa": {"country": "Panama", "region": "LATAM", "currency": "USD"},
    "glza": {"country": "South Africa", "region": "AFRICA", "currency": "ZAR"},
    "glae": {"country": "UAE", "region": "ME", "currency": "AED"},
    "glaz": {"country": "Azerbaijan", "region": "ME", "currency": "AZN"},
    "glbo": {"country": "Bolivia", "region": "LATAM", "currency": "BOB"},
    "glrs": {"country": "Serbia", "region": "EU", "currency": "RSD"},
    "glis": {"country": "Iceland", "region": "EU", "currency": "ISK"},
    "glmd": {"country": "Moldova", "region": "EU", "currency": "MDL"},
    "glie": {"country": "Ireland", "region": "EU", "currency": "EUR"},
    "glhk": {"country": "Hong Kong", "region": "APAC", "currency": "HKD"},
    "glhr": {"country": "Croatia", "region": "EU", "currency": "EUR"},
    "glsi": {"country": "Slovenia", "region": "EU", "currency": "EUR"},
    "glmt": {"country": "Malta", "region": "EU", "currency": "EUR"},
    "gllv": {"country": "Latvia", "region": "EU", "currency": "EUR"},
    "gllt": {"country": "Lithuania", "region": "EU", "currency": "EUR"},
    "glnz": {"country": "New Zealand", "region": "APAC", "currency": "NZD"},
    "glco": {"country": "Colombia", "region": "LATAM", "currency": "COP"},
    "glar": {"country": "Argentina", "region": "LATAM", "currency": "ARS"},
    "glfi": {"country": "Finland", "region": "EU", "currency": "EUR"},
    "glpt": {"country": "Portugal", "region": "EU", "currency": "EUR"},
    "glbe": {"country": "Belgium", "region": "EU", "currency": "EUR"},
    "glbg": {"country": "Bulgaria", "region": "EU", "currency": "BGN"},
    "glat": {"country": "Austria", "region": "EU", "currency": "EUR"},
    "glsk": {"country": "Slovakia", "region": "EU", "currency": "EUR"},
    "gles": {"country": "Spain", "region": "EU", "currency": "EUR"},
    "glsg": {"country": "Singapore", "region": "APAC", "currency": "SGD"},
    "gluy": {"country": "Uruguay", "region": "LATAM", "currency": "UYU"},
    "gldo": {"country": "Dominican Republic", "region": "LATAM", "currency": "DOP"},
    "gltw": {"country": "Taiwan", "region": "APAC", "currency": "TWD"},
    "glsv": {"country": "El Salvador", "region": "LATAM", "currency": "USD"},
}

COUNTRY_LANG = {
    "Germany": "de",
    "France": "fr",
    "Italy": "it",
    "Spain": "es",
    "Portugal": "pt",
    "Netherlands": "nl",
    "Poland": "pl",
    "Vietnam": "vi",
    "Japan": "ja",
    "South Korea": "ko",
    "China": "zh",
    "Taiwan": "zh",
    "United States": "en",
    "United Kingdom": "en",
    "Canada": "en",
    "Australia": "en",
}
# =========================
# HELPER
# =========================

def get_base_store(store_code):
    return store_code.split("_")[0]


def get_language(store_code, country):
    # case 1: suffix
    if "_" in store_code:
        return store_code.split("_")[1]

    # case 2: map country
    lang = COUNTRY_LANG.get(country)
    if lang:
        return lang

    return "unknown"


# =========================
# WRITER
# =========================

class Writer:
    def __init__(self):
        self.buf = []
        self.size = 0
        self.idx = 0

    def write(self, row):
        line = json.dumps(row, ensure_ascii=False) + "\n"
        self.buf.append(line)
        self.size += len(line)

        if self.size >= MAX_SIZE:
            self.flush()

    def flush(self):
        if not self.buf:
            return

        blob = bucket.blob(f"{PREFIX}part_{self.idx}.jsonl")
        blob.upload_from_string("".join(self.buf))

        print("✅ Uploaded", blob.name)

        self.buf = []
        self.size = 0
        self.idx += 1


# =========================
# MAIN
# =========================

def run():
    writer = Writer()
    seen = set()

    for doc in tqdm(collection.find({}, no_cursor_timeout=True)):
        react = doc.get("react_data", {})
        store_code = react.get("store_code")

        if not store_code:
            continue

        if store_code in seen:
            continue

        seen.add(store_code)

        base = get_base_store(store_code)
        dim = STORE_DIM.get(base)

        if not dim:
            print(f"⚠️ Missing mapping: {store_code}")

            dim = {
                "country": "UNKNOWN",
                "region": "UNKNOWN",
                "currency": None
            }

        writer.write({
            "store_code": store_code,
            "base_store": base,
            "country": dim["country"],
            "region": dim["region"],
            "currency": dim["currency"],
            "language": get_language(store_code, dim["country"])
        })

    writer.flush()
    print("🎯 DONE DIM_STORE")


if __name__ == "__main__":
    run()