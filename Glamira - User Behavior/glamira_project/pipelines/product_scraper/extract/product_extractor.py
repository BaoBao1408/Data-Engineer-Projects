import asyncio
import random
import json
import re
import os

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from tqdm import tqdm

from config.mongo_connection import connect_mongo


# ===============================
# CONFIG
# ===============================

INPUT_FILE = "data/raw/product_urls.jsonl"
OUTPUT_DIR = "data/product_extract"

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "processed_id.txt")
FAILED_FILE = os.path.join(OUTPUT_DIR, "failed_id.jsonl")

CONCURRENT_REQUESTS = 50
MAX_RETRIES = 5

os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124 Safari/537.36",

    "Accept":
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",

    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.glamira.com/"
}


# ===============================
# MONGO
# ===============================

client, db = connect_mongo()
collection = db["raw_products"]


# ===============================
# CHECKPOINT
# ===============================

def load_checkpoint():

    if not os.path.exists(CHECKPOINT_FILE):
        return set()

    processed = set()

    with open(CHECKPOINT_FILE) as f:
        for line in f:
            processed.add(line.strip())

    print("Checkpoint loaded:", len(processed))

    return processed


def save_checkpoint(pid):

    with open(CHECKPOINT_FILE, "a") as f:
        f.write(str(pid) + "\n")


# ===============================
# EXTRACT REACT DATA
# ===============================

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


# ===============================
# FETCH HTML
# ===============================

async def fetch_html(session, url):

    for attempt in range(MAX_RETRIES):

        try:

            await asyncio.sleep(random.uniform(1, 2))

            r = await session.get(
                url,
                headers=HEADERS,
                impersonate="chrome110"
            )

            if r.status_code == 200:

                html = r.text

                if len(html) < 8000:
                    return None

                return html

            if r.status_code in [403, 429]:

                await asyncio.sleep(5)

            if r.status_code >= 500:

                await asyncio.sleep(2 ** attempt)

        except:

            await asyncio.sleep(2 ** attempt)

    return None


# ===============================
# FAILED LOGGER
# ===============================

def log_failed(item):

    with open(FAILED_FILE, "a", encoding="utf-8") as f:

        f.write(json.dumps(item) + "\n")


# ===============================
# WORKER
# ===============================

async def worker(queue, session, processed, pbar):

    while True:

        item = await queue.get()

        if item is None:
            queue.task_done()
            break

        pid = str(item["product_id"])
        url = item["url"]

        if pid in processed:
            pbar.update(1)
            queue.task_done()
            continue

        pbar.set_postfix({"pid": pid})

        html = await fetch_html(session, url)

        if not html:
            log_failed(item)
            pbar.update(1)
            queue.task_done()
            continue


        react_data = extract_react_data(html)

        if not react_data:
            log_failed(item)
            pbar.update(1)
            queue.task_done()
            continue


        doc = {
            "product_id": pid,
            "url": url,
            "react_data": react_data
        }

        collection.update_one(
            {"product_id": pid},
            {"$set": doc},
            upsert=True
        )

        save_checkpoint(pid)

        pbar.update(1)

        queue.task_done()


# ===============================
# PRODUCER
# ===============================

async def producer(queue):

    with open(INPUT_FILE, encoding="utf-8") as f:

        for line in f:

            item = json.loads(line)

            await queue.put(item)


# ===============================
# MAIN
# ===============================

async def main():

    processed = load_checkpoint()

    total = sum(1 for _ in open(INPUT_FILE, encoding="utf-8"))

    pbar = tqdm(total=total, desc="Crawling products", ncols=100)

    queue = asyncio.Queue(maxsize=200)

    async with AsyncSession() as session:

        workers = [
            asyncio.create_task(worker(queue, session, processed, pbar))
            for _ in range(CONCURRENT_REQUESTS)
        ]

        await producer(queue)

        await queue.join()

        for _ in workers:
            await queue.put(None)

        await asyncio.gather(*workers)

    pbar.close()


# ===============================
# RUN
# ===============================

if __name__ == "__main__":

    asyncio.run(main())