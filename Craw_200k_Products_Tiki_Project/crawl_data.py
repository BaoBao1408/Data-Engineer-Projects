#!/usr/bin/env python3
"""
crawl_data.py (updated)
- successes -> outdir/products_00001.jsonl ... (only cleaned records)
- failures  -> errordir/errors_00001.jsonl ... (only error entries {"id":..., "error":...})
- processed_success.txt in outdir -> append id immediately after success (used by --resume to skip only successes)
- No raw_<pid>.json files will be created (unless you explicitly request chunk-raw via --save-raw-chunk; default off)
"""
import asyncio
import aiohttp
import aiofiles
import argparse
import csv
import json
import os
import time
import logging
import re
import random
from bs4 import BeautifulSoup
from aiohttp import ClientConnectorError, ServerTimeoutError
from tqdm import tqdm

API_TEMPLATE = "https://api.tiki.vn/product-detail/api/v1/products/{}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_PROMO_PATTERNS = [
    r"(?i)\bđây là sản phẩm\b.*", r"(?i)\bshop\b.*", r"(?i)\bchi(ỉ|́|̣)?\s+co(́|́)?\b.*",
    r"(?i)\btặng kèm\b.*", r"(?i)\bkhuyến mãi\b.*", r"(?i)\bcam kết\b.*",
    r"(?i)\bmiễn phí\b.*", r"(?i)\bmiễn phí vận chuyển\b.*", r"(?i)\bliên hệ\b.*",
    r"(?i)\bgiao hàng\b.*", r"(?i)\bthanh toán\b.*", r"(?i)http[s]?://\S+",
    r"(?i)www\.\S+", r"(?i)\bhot\b.*", r"(?i)\bnew\b.*", r"(?i)\bbest seller\b.*",
]

def remove_marketing_sentences(text):
    if not text:
        return text
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
    return " ".join(kept).strip()

def clean_description(html_text, max_len=400):
    if not html_text:
        return ""
    if isinstance(html_text, (dict, list)):
        html_text = str(html_text)
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = " ".join(text.split())
    parts = re.split(r'(?<=[.!?])\s+', text, maxsplit=2)
    short = " ".join(parts[:2]) if parts else text
    if len(short) > max_len:
        short = short[:max_len].rstrip()
        if not short.endswith((".", "!", "?")):
            short = short.rsplit(" ", 1)[0] + "..."
    return short

def description_pipeline(html_text, max_len=400, remove_marketing=True):
    s = clean_description(html_text, max_len=max_len)
    if remove_marketing:
        s = remove_marketing_sentences(s)
    if len(s) > max_len:
        s = s[:max_len].rstrip()
        if not s.endswith((".", "!", "?")):
            s = s.rsplit(" ", 1)[0] + "..."
    return s

def extract_fields(product_json):
    if not isinstance(product_json, dict):
        return None
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
    def __init__(self, ids, outdir="output", errordir="output_error", chunk_size=1000,
                 concurrency=50, timeout=30, retries=3, limit_per_host=10, resume=False):
        self.ids = ids
        self.outdir = outdir
        self.errordir = errordir
        self.chunk_size = chunk_size
        self.concurrency = concurrency
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retries = retries
        self.limit_per_host = limit_per_host
        self.resume = resume

        os.makedirs(self.outdir, exist_ok=True)
        os.makedirs(self.errordir, exist_ok=True)

        self.success_file = os.path.join(self.outdir, "processed_success.txt")
        self.errors_summary_path = os.path.join(self.errordir, "errors_summary.json")
        self.failed_ids_path = os.path.join(self.errordir, "failed_ids_to_retry.txt")

        self.processed_success_set = set()
        if self.resume and os.path.exists(self.success_file):
            with open(self.success_file, "r", encoding="utf-8") as pf:
                for line in pf:
                    self.processed_success_set.add(line.strip())

        self.aggregate_errors = {}

    async def fetch_one(self, session, pid, sem):
        url = API_TEMPLATE.format(pid)
        async with sem:
            attempt = 0
            last_err = None
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
                                return pid, None, "Non-JSON response", text
                            return pid, data, None, None
                        if status in (404, 403):
                            return pid, None, f"HTTP {status}", None
                        text = await resp.text()
                        logging.warning("Non-200 status %s for %s (attempt %s)", status, pid, attempt)
                        last_err = f"HTTP {status}"
                except (asyncio.TimeoutError, ServerTimeoutError, ClientConnectorError) as e:
                    last_err = f"NetworkError: {str(e)}"
                    logging.warning("Network error for %s attempt %s: %s", pid, attempt, e)
                except Exception as e:
                    last_err = f"Unexpected: {str(e)}"
                    logging.exception("Unexpected error for %s attempt %s: %s", pid, attempt, e)

                backoff = min(2 ** attempt, 10)
                jitter = random.random() * 0.5
                await asyncio.sleep(backoff + jitter)
            return pid, None, last_err or "MaxRetriesExceeded", None

    async def process_chunk(self, ids_chunk, idx, session):
        sem = asyncio.Semaphore(self.concurrency)
        tasks = [asyncio.create_task(self.fetch_one(session, pid, sem)) for pid in ids_chunk]

        results = []
        pbar = tqdm(total=len(tasks), desc=f"Chunk {idx}", leave=True)
        try:
            for fut in asyncio.as_completed(tasks):
                pid, data, err, raw_text = await fut
                results.append((pid, data, err, raw_text))
                pbar.update(1)
        finally:
            pbar.close()

        # keep original input order
        id_to_result = {pid: (pid, data, err, raw_text) for pid, data, err, raw_text in results}
        ordered_results = [id_to_result.get(pid, (pid, None, "NoResult", None)) for pid in ids_chunk]

        outpath = os.path.join(self.outdir, f"products_{idx:05d}.jsonl")
        errpath = os.path.join(self.errordir, f"errors_{idx:05d}.jsonl")

        error_lines = []

        async with aiofiles.open(outpath, "w", encoding="utf-8") as f_out:
            for pid, data, err, raw_text in ordered_results:
                if data:
                    cleaned = extract_fields(data)
                    if cleaned is None:
                        err_obj = {"id": pid, "error": "ExtractFailed"}
                        error_lines.append(json.dumps(err_obj, ensure_ascii=False))
                        self.aggregate_errors["ExtractFailed"] = self.aggregate_errors.get("ExtractFailed", 0) + 1
                        continue

                    await f_out.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
                    # append processed_success as before...
                    async with aiofiles.open(self.success_file, "a", encoding="utf-8") as pf:
                        await pf.write(str(pid) + "\n")
                    self.processed_success_set.add(str(pid))

                else:
                    err_obj = {"id": pid, "error": err}
                    error_lines.append(json.dumps(err_obj, ensure_ascii=False))
                    self.aggregate_errors[err] = self.aggregate_errors.get(err, 0) + 1

        # only write errpath if we have errors
        if error_lines:
            async with aiofiles.open(errpath, "w", encoding="utf-8") as f_err:
                await f_err.write("\n".join(error_lines) + "\n")

    async def run(self):
        chunks = [ self.ids[i:i+self.chunk_size] for i in range(0, len(self.ids), self.chunk_size) ]
        connector = aiohttp.TCPConnector(limit=self.limit_per_host)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; ProductDownloader/1.0)"}
        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            for i, chunk in enumerate(chunks, start=1):
                if self.resume:
                    filtered = [pid for pid in chunk if pid not in self.processed_success_set]
                    if not filtered:
                        logging.info("Resume: all ids in chunk %s already succeeded, skipping", i)
                        continue
                    chunk = filtered

                logging.info("Processing chunk %s/%s size=%s", i, len(chunks), len(chunk))
                t0 = time.time()
                try:
                    await self.process_chunk(chunk, i, session)
                except Exception as e:
                    logging.exception("Chunk %s failed unexpectedly: %s", i, e)
                logging.info("Chunk %s done in %.1fs", i, time.time() - t0)

        # write errors summary to errordir
        try:
            with open(self.errors_summary_path, "w", encoding="utf-8") as sf:
                json.dump({"total_ids": len(self.ids), "errors": self.aggregate_errors}, sf, ensure_ascii=False, indent=2)
            logging.info("Wrote errors summary to %s", self.errors_summary_path)
        except Exception as e:
            logging.warning("Failed to write errors summary: %s", e)

        # aggregate failed ids into failed_ids_to_retry.txt in errordir
        try:
            failed_ids = []
            for fn in sorted(os.listdir(self.errordir)):
                if fn.startswith("errors_") and fn.endswith(".jsonl"):
                    p = os.path.join(self.errordir, fn)
                    with open(p, "r", encoding="utf-8") as ef:
                        for line in ef:
                            try:
                                obj = json.loads(line)
                                if "id" in obj:
                                    failed_ids.append(str(obj["id"]))
                            except Exception:
                                continue
            with open(self.failed_ids_path, "w", encoding="utf-8") as ffail:
                for fid in failed_ids:
                    ffail.write(fid + "\n")
            logging.info("Wrote failed ids to %s (count=%s)", self.failed_ids_path, len(failed_ids))
        except Exception as e:
            logging.warning("Failed to aggregate failed ids: %s", e)

def load_ids_from_csv(path):
    ids = []
    with open(path, "r", encoding="utf-8") as f:
        try:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
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
    parser.add_argument("--outdir", default="output", help="directory for successful outputs and processed_success.txt")
    parser.add_argument("--errordir", default="output_error", help="directory for error files")
    parser.add_argument("--chunk", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--limit-per-host", type=int, default=10)
    parser.add_argument("--resume", action="store_true", help="resume by skipping ids that previously succeeded (processed_success.txt in outdir)")
    args = parser.parse_args()

    ids = load_ids_from_csv(args.ids)
    logging.info("Loaded %s ids from %s", len(ids), args.ids)
    downloader = ProductDownloader(ids, outdir=args.outdir, errordir=args.errordir, chunk_size=args.chunk,
                                   concurrency=args.concurrency, timeout=args.timeout, retries=args.retries,
                                   limit_per_host=args.limit_per_host, resume=args.resume)
    asyncio.run(downloader.run())

if __name__ == "__main__":
    main()
