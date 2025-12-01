#!/usr/bin/env python3
"""
product_downloader_full.py
- Input: CSV file with product ids (column contains product id)
- Output: folder `output/` containing files products_00001.jsonl ... each ~chunk_size
- Usage example:
    python product_downloader_full.py --ids /path/to/products-0-200000.csv --outdir output --chunk 1000 --concurrency 50
"""

import asyncio
import aiohttp
import aiofiles
import argparse
import csv
import json
import os
import time
import math
import logging
import re
from bs4 import BeautifulSoup
from aiohttp import ClientConnectorError, ClientResponseError, ServerTimeoutError
from tqdm.asyncio import tqdm_asyncio  # optional, not used directly below
from tqdm import tqdm

API_TEMPLATE = "https://api.tiki.vn/product-detail/api/v1/products/{}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_PROMO_PATTERNS = [
    # vietnamese marketing phrases (case-insensitive)
    r"(?i)\bđây là sản phẩm\b.*",
    r"(?i)\bshop\b.*",
    r"(?i)\bchi(ỉ|́|̣)?\s+co(́|́)?\b.*",   # "chỉ có..."
    r"(?i)\btặng kèm\b.*",
    r"(?i)\bkhuyến mãi\b.*",
    r"(?i)\bcam kết\b.*",
    r"(?i)\bmiễn phí\b.*",
    r"(?i)\bmiễn phí vận chuyển\b.*",
    r"(?i)\bliên hệ\b.*",
    r"(?i)\bgiao hàng\b.*",
    r"(?i)\bthanh toán\b.*",
    # urls / image tags etc
    r"(?i)http[s]?://\S+",
    r"(?i)www\.\S+",
    # extremely short promotional fragments with typical punctuation
    r"(?i)\bhot\b.*",
    r"(?i)\bnew\b.*",
    r"(?i)\bbest seller\b.*",
]


def remove_marketing_sentences(text):
    """
    Remove sentences that match common promotional patterns.
    Returns cleaned text (sentences concatenated).
    """
    logging.debug("ENTER remove_marketing_sentences; text_len=%s", 0 if text is None else len(str(text)))
    if not text:
        return text
    # split into sentences (simple heuristic)
    sents = re.split(r'(?<=[.!?])\s+', str(text))
    kept = []
    for s in sents:
        s_stripped = s.strip()
        if not s_stripped:
            continue
        skip = False
        for pat in _PROMO_PATTERNS:
            if re.search(pat, s_stripped):
                skip = True
                break
        if not skip:
            kept.append(s_stripped)
    kept_text = " ".join(kept).strip()
    logging.debug("EXIT remove_marketing_sentences; kept_len=%s", len(kept_text))
    return kept_text


def refine_marketing_text(html_text, max_len=240):
    """
    Strip HTML (if needed), remove marketing sentences and return a short
    1-2 sentence professional summary clipped to max_len.
    """
    logging.debug("ENTER refine_marketing_text; html_len=%s", 0 if html_text is None else len(str(html_text)))
    if not html_text:
        return ""
    if isinstance(html_text, (dict, list)):
        html_text = str(html_text)
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r'\s+', ' ', text).strip()
    # remove marketing lines
    text = remove_marketing_sentences(text)
    # take first 1-2 sentences
    parts = re.split(r'(?<=[.!?])\s+', text)
    short = " ".join(parts[:2]) if parts else text
    if len(short) > max_len:
        short = short[:max_len].rstrip()
        if not short.endswith((".", "!", "?")):
            # avoid cutting mid-word
            short = short.rsplit(" ", 1)[0] + "..."
    logging.debug("EXIT refine_marketing_text; short_len=%s", len(short))
    return short


def clean_description(html_text, max_len=400):
    """Strip HTML, collapse whitespace, return first 1-2 sentences trimmed to max_len."""
    logging.debug("ENTER clean_description; html_len=%s", 0 if html_text is None else len(str(html_text)))
    if not html_text:
        return ""
    if isinstance(html_text, (dict, list)):
        html_text = str(html_text)
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = " ".join(text.split())
    # take first ~2 sentences
    import re
    parts = re.split(r'(?<=[.!?])\s+', text, maxsplit=2)
    short = " ".join(parts[:2]) if parts else text
    if len(short) > max_len:
        short = short[:max_len].rstrip()
        if not short.endswith((".", "!", "?")):
            short = short.rsplit(" ", 1)[0] + "..."
    logging.debug("EXIT clean_description; out_len=%s", len(short))
    return short


def description_pipeline(html_text, max_len=400, remove_marketing=True):
    """
    Compose description processing steps in a single pipeline used by extract_fields.
    Steps:
      1. Basic HTML strip + sentence trimming using clean_description
      2. Optionally remove marketing sentences
      3. Final safe trim to max_len
    """
    logging.debug("ENTER description_pipeline; remove_marketing=%s", remove_marketing)
    # 1. normalize using existing clean_description logic (keeps sentence logic)
    s = clean_description(html_text, max_len=max_len)
    # 2. remove marketing fragments (operate on plain text)
    if remove_marketing:
        # remove_marketing_sentences expects plain text
        s = remove_marketing_sentences(s)
    # 3. final trim & punctuation fix
    if len(s) > max_len:
        s = s[:max_len].rstrip()
        if not s.endswith((".", "!", "?")):
            s = s.rsplit(" ", 1)[0] + "..."
    logging.debug("EXIT description_pipeline; out_len=%s", len(s))
    return s


def extract_fields(product_json):
    """Extract fields robustly from Tiki product JSON."""
    if not isinstance(product_json, dict):
        return None

    # if wrapped
    if "data" in product_json and isinstance(product_json["data"], dict):
        product_json = product_json["data"]
    if "product" in product_json and isinstance(product_json["product"], dict):
        product_json = product_json["product"]

    pid = product_json.get("id") or product_json.get("product_id") or product_json.get("sku")
    name = product_json.get("name") or product_json.get("title")
    url_key = product_json.get("url_key") or product_json.get("url_path") or product_json.get("short_url")
    price = None
    for k in ("price", "final_price", "original_price", "list_price", "current_price"):
        if k in product_json and product_json.get(k) is not None:
            price = product_json.get(k)
            break

    desc_html = product_json.get("description") or product_json.get("short_description") or product_json.get("long_description")
    # Use the composed pipeline so marketing removal + clean trimming happen
    description = description_pipeline(desc_html, max_len=400, remove_marketing=True)

    images = []
    imgs = product_json.get("images") or []
    if isinstance(imgs, list):
        for it in imgs:
            if isinstance(it, dict):
                url = it.get("large_url") or it.get("base_url") or it.get("medium_url") or it.get("thumbnail_url") or it.get("url")
                if url:
                    images.append(url)
            elif isinstance(it, str):
                images.append(it)
    for k in ("thumbnail_url", "thumbnail", "image", "thumb"):
        v = product_json.get(k)
        if v and v not in images:
            images.insert(0, v)

    return {
        "id": pid,
        "name": name,
        "url_key": url_key,
        "price": price,
        "description": description,
        "images": images
    }

class ProductDownloader:
    def __init__(self, ids, outdir="output", chunk_size=1000, concurrency=50, timeout=30, retries=3, limit_per_host=10, save_raw=False, resume=False):
        self.ids = ids
        self.outdir = outdir
        self.chunk_size = chunk_size
        self.concurrency = concurrency
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retries = retries
        self.limit_per_host = limit_per_host
        self.save_raw = save_raw
        self.resume = resume
        os.makedirs(outdir, exist_ok=True)

    async def fetch_one(self, session, pid, sem):
        url = API_TEMPLATE.format(pid)
        async with sem:
            attempt = 0
            while attempt <= self.retries:
                attempt += 1
                try:
                    async with session.get(url, timeout=self.timeout) as resp:
                        status = resp.status
                        if status == 200:
                            try:
                                data = await resp.json()
                            except Exception:
                                text = await resp.text()
                                logging.warning("Non-json for %s: %s", pid, text[:200])
                                return pid, None, "Non-JSON response"
                            return pid, data, None
                        # 404 or 403 -> return quickly
                        if status in (404, 403):
                            return pid, None, f"HTTP {status}"
                        # otherwise record error and retry
                        text = await resp.text()
                        logging.warning("Non-200 status %s for %s (attempt %s)", status, pid, attempt)
                        err = f"HTTP {status}"
                except (asyncio.TimeoutError, ServerTimeoutError, ClientConnectorError) as e:
                    err = str(e)
                    logging.warning("Network error for %s attempt %s: %s", pid, attempt, e)
                except Exception as e:
                    err = str(e)
                    logging.exception("Unexpected error for %s attempt %s: %s", pid, attempt, e)

                # backoff
                await asyncio.sleep(min(2 ** attempt, 10))
            return pid, None, err

    async def process_chunk(self, ids_chunk, idx, session):
        sem = asyncio.Semaphore(self.concurrency)
        tasks = [asyncio.create_task(self.fetch_one(session, pid, sem)) for pid in ids_chunk]

        results = []
        pbar = tqdm(total=len(tasks), desc=f"Chunk {idx}", leave=True)
        try:
            for fut in asyncio.as_completed(tasks):
                pid, data, err = await fut
                results.append((pid, data, err))
                pbar.update(1)
        finally:
            pbar.close()

        outpath = os.path.join(self.outdir, f"products_{idx:05d}.jsonl")
        async with aiofiles.open(outpath, "w", encoding="utf-8") as f:
            for pid, data, err in results:
                if data:
                    cleaned = extract_fields(data)
                    # optionally save raw
                    if self.save_raw:
                        rawpath = os.path.join(self.outdir, f"raw_{pid}.json")
                        async with aiofiles.open(rawpath, "w", encoding="utf-8") as rf:
                            await rf.write(json.dumps(data, ensure_ascii=False))
                    await f.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
                else:
                    await f.write(json.dumps({"id": pid, "error": err}, ensure_ascii=False) + "\n")

    async def run(self):
        chunks = [ self.ids[i:i+self.chunk_size] for i in range(0, len(self.ids), self.chunk_size) ]

        connector = aiohttp.TCPConnector(limit=self.limit_per_host)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; ProductDownloader/1.0)"}
        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            for i, chunk in enumerate(chunks, start=1):
                # resume: skip if file exists and resume True
                outpath = os.path.join(self.outdir, f"products_{i:05d}.jsonl")
                if self.resume and os.path.exists(outpath):
                    logging.info("Resume: skipping %s (exists)", outpath)
                    continue
                logging.info("Processing chunk %s/%s size=%s", i, len(chunks), len(chunk))
                t0 = time.time()
                await self.process_chunk(chunk, i, session)
                logging.info("Chunk %s done in %.1fs", i, time.time() - t0)

def load_ids_from_csv(path):
    ids = []
    with open(path, "r", encoding="utf-8") as f:
        # try csv or plain lines
        try:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                # find a numeric-looking field
                for cell in row:
                    cell = cell.strip()
                    if cell and any(ch.isdigit() for ch in cell):
                        ids.append(cell)
                        break
        except Exception:
            f.seek(0)
            for line in f:
                s = line.strip()
                if s:
                    ids.append(s)
    # dedupe preserve order
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ids", required=True, help="CSV or txt file with one id per line")
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--chunk", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--limit-per-host", type=int, default=10)
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    ids = load_ids_from_csv(args.ids)
    logging.info("Loaded %s ids from %s", len(ids), args.ids)
    downloader = ProductDownloader(ids, args.outdir, chunk_size=args.chunk, concurrency=args.concurrency, timeout=args.timeout, retries=args.retries, limit_per_host=args.limit_per_host, save_raw=args.save_raw, resume=args.resume)
    asyncio.run(downloader.run())

if __name__ == "__main__":
    main()
