import asyncio
import random
import json
import re
from pathlib import Path

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

from config.mongo_connection import connect_mongo


# =========================
# CONFIG
# =========================

BASE_DIR = Path("data/retry_product_extract")

INPUT_FILE = Path("data/product_extract/failed_id.jsonl")
FAILED_FILE = BASE_DIR / "failed_id2.json"
PROCESSED_FILE = BASE_DIR / "processed_id_retry.txt"

MAX_RETRIES = 5
CONCURRENT = 50 


HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124 Safari/537.36",
}


# =========================
# MONGO
# =========================

client, db = connect_mongo()
collection = db["raw_products"]


# =========================
# CHECKPOINT (RESUME)
# =========================

def load_processed():
    if not PROCESSED_FILE.exists():
        return set()
    return set(PROCESSED_FILE.read_text().splitlines())


def save_processed(pid):
    with open(PROCESSED_FILE, "a") as f:
        f.write(pid + "\n")


# =========================
# FAILED (APPEND SAFE)
# =========================

def load_failed():
    if not FAILED_FILE.exists():
        return set()
    try:
        return set(json.loads(FAILED_FILE.read_text()))
    except:
        return set()


failed_cache = load_failed()


def save_failed():
    with open(FAILED_FILE, "w") as f:
        json.dump(list(failed_cache), f, indent=2)


# =========================
# LOAD INPUT
# =========================

def load_items():

    processed = load_processed()
    items = []

    with open(INPUT_FILE, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            pid = str(row["product_id"])

            if pid not in processed:
                items.append(row)

    print("Remaining:", len(items))
    return items


# =========================
# BUILD URL (fallback global domain)
# =========================

def build_url(pid):
    return f"https://www.glamira.com/catalog/product/view/id/{pid}"


# =========================
# EXTRACT
# =========================

def extract_react_data(html):

    soup = BeautifulSoup(html, "lxml")

    for script in soup.find_all("script"):
        text = script.string or script.text

        if not text:
            continue

        if "react_data" in text:

            match = re.search(
                r"(?:var|window)\.?\s*react_data\s*=\s*(\{.*?\})\s*;",
                text,
                re.DOTALL
            )

            if match:
                try:
                    return json.loads(match.group(1))
                except:
                    return None

    return None


# =========================
# FETCH
# =========================

async def fetch(session, url):

    for attempt in range(MAX_RETRIES):

        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))

            r = await session.get(
                url,
                headers=HEADERS,
                impersonate="chrome110"
            )

            if r.status_code == 200:

                html = r.text

                if len(html) < 5000:
                    continue

                if "Access denied" in html:
                    continue

                return html

            if r.status_code in [403, 429]:
                await asyncio.sleep(3)

            if r.status_code >= 500:
                await asyncio.sleep(2 ** attempt)

        except:
            await asyncio.sleep(2 ** attempt)

    return None


# =========================
# WORKER
# =========================

async def worker(session, item):

    pid = str(item["product_id"])
    original_url = item.get("url")

    url = build_url(pid)

    html = await fetch(session, url)

    if not html and original_url:
        html = await fetch(session, original_url)

    if not html:
        failed_cache.add(pid)
        return

    data = extract_react_data(html)

    if not data:
        failed_cache.add(pid)
        return

    # ✅ INSERT MONGO (UPSERT)
    collection.update_one(
        {"product_id": pid},
        {
            "$set": {
                "product_id": pid,
                "url": url,
                "react_data": data
            }
        },
        upsert=True
    )

    save_processed(pid)


# =========================
# CRAWL
# =========================

async def crawl(items):

    semaphore = asyncio.Semaphore(CONCURRENT)

    async with AsyncSession() as session:

        async def bound(item):
            async with semaphore:
                await worker(session, item)

        tasks = [bound(i) for i in items]

        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await f


# =========================
# MAIN
# =========================

def main():

    items = load_items()

    asyncio.run(crawl(items))

    save_failed()

    print("DONE")
    print("Failed:", len(failed_cache))


if __name__ == "__main__":
    main()