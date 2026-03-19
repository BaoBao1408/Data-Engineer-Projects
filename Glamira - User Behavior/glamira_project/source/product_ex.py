import asyncio
import random
import json
import re
import os

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm.asyncio import tqdm


# ===============================
# ENV + MONGO
# ===============================

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

collection = db["products"]


# ===============================
# CONFIG
# ===============================

INPUT_FILE = "data/raw/product_urls_test.jsonl"

CONCURRENT_REQUESTS = 5
MAX_RETRIES = 5


HEADERS = {
 "User-Agent":
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
 "(KHTML, like Gecko) Chrome/124 Safari/537.36",

 "Accept":
 "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",

 "Accept-Language":
 "en-US,en;q=0.9",

 "Referer": "https://www.glamira.com/"
}


# ===============================
# LOAD URLS
# ===============================

def load_urls():

    items = []

    with open(INPUT_FILE, encoding="utf-8") as f:

        for line in f:

            row = json.loads(line)

            items.append(row)

    print("Loaded", len(items), "products")

    return items


# ===============================
# EXTRACT REACT DATA
# ===============================

def extract_react_data(html):

    soup = BeautifulSoup(html, "lxml")

    scripts = soup.find_all("script")

    for script in scripts:

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

                except Exception as e:

                    print("JSON parse error:", e)

    return None


# ===============================
# FETCH HTML
# ===============================

async def fetch_html(session, url):

    for attempt in range(MAX_RETRIES):

        try:

            await asyncio.sleep(random.uniform(1,2))

            r = await session.get(
                url,
                headers=HEADERS,
                impersonate="chrome110"
            )

            status = r.status_code

            if status == 200:

                html = r.text

                if len(html) < 8000:

                    print("Blocked or small HTML", url)

                    return None

                return html


            if status in [403,429]:

                print("Rate limited", status)

                await asyncio.sleep(5)


            if status == 502:

                print("Bad gateway retry")

                await asyncio.sleep(2**attempt)


            if status >= 500:

                await asyncio.sleep(2**attempt)


        except Exception as e:

            print("Request error:", e)

            await asyncio.sleep(2**attempt)

    return None


# ===============================
# WORKER
# ===============================

async def worker(session, semaphore, item):

    async with semaphore:

        pid = item["product_id"]
        url = item["url"]

        html = await fetch_html(session, url)

        if not html:

            print("HTML FAIL:", pid)

            return


        react_data = extract_react_data(html)

        if not react_data:

            print("PARSE FAIL:", pid)

            return


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

        print("INSERT:", pid)


# ===============================
# CRAWL
# ===============================

async def crawl(items):

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with AsyncSession() as session:

        tasks = [

            worker(session, semaphore, item)

            for item in items

        ]

        for future in tqdm(

            asyncio.as_completed(tasks),
            total=len(tasks)

        ):

            await future


# ===============================
# MAIN
# ===============================

def main():

    items = load_urls()

    asyncio.run(crawl(items))


if __name__ == "__main__":

    main()