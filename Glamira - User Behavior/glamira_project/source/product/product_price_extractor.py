import asyncio
import random
import logging
import aiohttp
import pandas as pd
import json
import re
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


logging.basicConfig(level=logging.INFO)

FALLBACK_URL = "https://www.glamira.com/catalog/product/view/id/{}"

MAX_RETRIES = 5
CONCURRENT_REQUESTS = 200
TIMEOUT = aiohttp.ClientTimeout(total=10)

async def fetch_product(session, url):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url) as response:
                status = response.status
                if status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    react_data = None
                    # Find script has react_data
                    scripts = soup.find_all("script", {"type": "text/javascript"})
                    for script in scripts:
                        if script.string and "var react_data" in script.string:
                            match = re.search(
                                r"var\s+react_data\s*=\s*(\{.*?\});",
                                script.string,
                                re.DOTALL
                            )
                            if match:
                                json_str = match.group(1)
                                try:
                                    react_data = json.loads(json_str)
                                except Exception as e:
                                    logging.warning(f"JSON parse error {url} {e}")
                                break
                    if not react_data:
                        logging.warning(f"react_data not found {url}")
                        return None
                    return react_data

                elif status in [403, 404]:
                    logging.warning(f"Skip {url} status={status}")
                    return None

                elif status == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    sleep = retry_after + random.uniform(1, 3)
                    logging.warning(f"Rate limited {url}, sleeping {sleep:.2f}s")
                    await asyncio.sleep(sleep)

                elif status >= 500:
                    sleep = (2**attempt) + random.uniform(0, 2)
                    logging.warning(f"Server error {status} retry {url}")
                    await asyncio.sleep(sleep)

        except asyncio.TimeoutError:
            sleep = (2**attempt) + random.uniform(0, 2)
            await asyncio.sleep(sleep)

        except aiohttp.ClientError as e:
            logging.warning(f"Request error {url} {e}")
            sleep = (2**attempt) + random.uniform(0, 2)
            await asyncio.sleep(sleep)
    return None

async def worker(session, semaphore, product_id, urls, fallback_products, failed_products):
    async with semaphore:
        # Try original URLs
        for url in urls:
            info = await fetch_product(session, url)
            if info:
                return {
                    "product_id": product_id,
                    **info
                }
        # Fallback URLs 
        fallback_url = FALLBACK_URL.format(product_id)
        logging.info(f"Trying fallback URL for product {product_id}")
        info = await fetch_product(session, fallback_url)
        if info:
            fallback_products.add(product_id)
            return {
                "product_id": product_id,
                **info
            }
        #Failed 
        failed_products.add(product_id)
        logging.warning(f"Product failed completely: {product_id}")
        return None

async def crawl_product(products, output_path):
    failed_products = set()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(
        timeout=TIMEOUT,
        connector=connector
    ) as session:
        tasks = [
            worker(session, semaphore, pid, urls, failed_products)
            for pid, urls in products.items()
        ]
        with open(output_path, "w", encoding="utf-8") as f:
            for future in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc="Crawling products"
            ):
                try:
                    result = await future
                    if result:
                        f.write(
                            json.dumps(result, ensure_ascii=False)
                            + "\n"
                        )
                except Exception as e:
                    logging.warning(f"Worker failed: {e}")
    return list(failed_products)

def load_urls(jsonl_path):
    logging.info(f"Loading urls from {jsonl_path}")
    products = {}
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line.strip())
            pid = record["product_id"]
            urls = record["urls"]
            products[pid] = urls
    logging.info(f"Loaded {len(products)} products")
    return products
    
def save_product_info(results, path):
    logging.info(f"Saving results to {path} (JSONL format)")
    with open(path, "w", encoding="utf-8") as f:
        for item in results:
            json_line = json.dumps(item, ensure_ascii=False)
            f.write(json_line + "\n")
    logging.info("Saved JSONL successfully")

def save_error_products(failed_products, path):
    logging.info(f"Saving failed product_ids to {path}")
    with open(path, "w") as f:
        for pid in failed_products:
            f.write(str(pid) + "\n")

def collect_product(urls_path, output_path, failed_path):
    products = load_urls(urls_path)
    logging.info(f"{len(products)} products loaded")
    logging.info("Start crawling pipeline")
    failed_products = asyncio.run(
        crawl_product(products, output_path)
    )
    logging.info("Collect finished")
    if failed_products:
        save_error_products(failed_products, failed_path)
        logging.info(f"{len(failed_products)} products failed completely")