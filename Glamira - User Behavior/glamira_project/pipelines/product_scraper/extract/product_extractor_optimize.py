import asyncio
import random
import json
import re
import os

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from tqdm import tqdm
from pymongo import InsertOne

from config.mongo_connection import connect_mongo


# ===============================
# CONFIG
# ===============================

INPUT_FILE = "data/raw/product_urls.jsonl"
OUTPUT_DIR = "data/product_extract"

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "processed_id_optimized.txt")
FAILED_FILE = os.path.join(OUTPUT_DIR, "failed_id_optimized.jsonl")

CONCURRENT_REQUESTS = 100
MAX_RETRIES = 4
BULK_SIZE = 50

os.makedirs(OUTPUT_DIR, exist_ok=True)


HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124 Safari/537.36",

    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}


# ===============================
# MONGO
# ===============================

client, db = connect_mongo()
collection = db["collection_product_optimized"]


# ===============================
# GLOBAL STATE
# ===============================

TOTAL_COUNT = 0


# ===============================
# LOAD PROGRESS
# ===============================

def load_progress():

    if not os.path.exists(CHECKPOINT_FILE):
        return set()

    processed = set()

    try:
        with open(CHECKPOINT_FILE) as f:

            lines = f.readlines()

            if len(lines) >= 1:

                progress = lines[0].strip()

                if "/" in progress:

                    processed_count = int(progress.split("/")[0])

                    print("Resume from processed:", processed_count)

    except:
        pass

    return processed


# ===============================
# UPDATE PROGRESS FILE
# ===============================

def update_progress(processed_count, total, current_id):

    with open(CHECKPOINT_FILE, "w") as f:

        f.write(f"{processed_count}/{total}\n")

        f.write(str(current_id))


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

            await asyncio.sleep(random.uniform(0, 0.1))

            r = await session.get(url, headers=HEADERS)

            if r.status_code == 200:

                html = r.text

                if len(html) > 5000:
                    return html

            if r.status_code in [403, 429]:

                await asyncio.sleep(2)

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

async def worker(queue, session, processed, pbar, mongo_buffer):

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

        mongo_buffer.append(InsertOne(doc))

        if len(mongo_buffer) >= BULK_SIZE:

            collection.bulk_write(mongo_buffer, ordered=False)

            mongo_buffer.clear()


        processed.add(pid)

        update_progress(len(processed), TOTAL_COUNT, pid)

        pbar.update(1)

        queue.task_done()


# ===============================
# PRODUCER
# ===============================

async def producer(queue):

    items = []

    with open(INPUT_FILE, encoding="utf-8") as f:

        for line in f:

            items.append(json.loads(line))

    random.shuffle(items)

    for item in items:

        await queue.put(item)


# ===============================
# MAIN
# ===============================

async def main():

    global TOTAL_COUNT

    processed = load_progress()

    TOTAL_COUNT = sum(1 for _ in open(INPUT_FILE, encoding="utf-8"))

    pbar = tqdm(total=TOTAL_COUNT, desc="Crawling products", ncols=100)

    queue = asyncio.Queue(maxsize=500)

    mongo_buffer = []

    async with AsyncSession(max_clients=CONCURRENT_REQUESTS) as session:

        workers = [

            asyncio.create_task(
                worker(queue, session, processed, pbar, mongo_buffer)
            )

            for _ in range(CONCURRENT_REQUESTS)
        ]

        await producer(queue)

        await queue.join()

        for _ in workers:

            await queue.put(None)

        await asyncio.gather(*workers)


    if mongo_buffer:

        collection.bulk_write(mongo_buffer, ordered=False)

    pbar.close()


# ===============================
# RUN
# ===============================

if __name__ == "__main__":

    asyncio.run(main())