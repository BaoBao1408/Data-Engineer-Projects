import csv
import json
import os
import time
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import argparse

API_TEMPLATE = "https://api.tiki.vn/product-detail/api/v1/products/{}"
OUTDIR = "output2"
OUTFILE = os.path.join(OUTDIR, "products.jsonl")


def load_ids(path: str) -> List[str]:
    ids = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            for cell in row:
                s = cell.strip()
                if s and any(ch.isdigit() for ch in s):
                    ids.append(s)
                    break
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def simple_clean_description(html: Optional[str], max_len: int = 300) -> str:
    if not html:
        return ""
    if isinstance(html, (dict, list)):
        html = str(html)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    parts = []
    for sep in (".", "!", "?"):
        if sep in text:
            parts = text.split(sep)
            break
    if parts:
        core = ". ".join([p.strip() for p in parts[:2] if p.strip()]) + "."
    else:
        core = text
    if len(core) > max_len:
        core = core[:max_len].rstrip()
        if " " in core:
            core = core.rsplit(" ", 1)[0] + "..."
    return core


def fetch_product(session: requests.Session, pid: str, timeout: int = 15, retries: int = 2):
    url = API_TEMPLATE.format(pid)
    attempt = 0
    while attempt <= retries:
        attempt += 1
        try:
            resp = session.get(url, timeout=timeout, headers={"User-Agent": "SimpleDownloader/1.0"})
            if resp.status_code == 200:
                try:
                    return resp.json(), None
                except Exception as e:
                    return None, f"Invalid JSON: {e}"
            if resp.status_code in (403, 404):
                return None, f"HTTP {resp.status_code}"
            err = f"HTTP {resp.status_code}"
        except requests.exceptions.RequestException as e:
            err = str(e)
        if attempt <= retries:
            time.sleep(1 + attempt) 
        else:
            return None, err

def chunkify(lst: List[str], chunk_size: int):
    """Yield successive chunk_size-sized chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def process_chunk(session: requests.Session, chunk_ids: List[str], outpath: str):
    """Xử lý 1 chunk: gọi API tuần tự rồi ghi file jsonl."""
    records = []
    for pid in tqdm(chunk_ids, desc=f"Chunk {os.path.basename(outpath)}", unit="id"):
        data, err = fetch_product(session, pid)
        if data:
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
                data = data["data"]
            if isinstance(data, dict) and "product" in data and isinstance(data["product"], dict):
                data = data["product"]
            rec = {
                "id": data.get("id") or data.get("product_id") or pid,
                "name": data.get("name") or data.get("title"),
                "price": data.get("price") or data.get("final_price") or data.get("current_price"),
                "url_key": data.get("url_key") or data.get("url_path"),
            }
            desc_html = data.get("description") or data.get("short_description") or data.get("long_description") or ""
            rec["description"] = simple_clean_description(desc_html)
            imgs = []
            images = data.get("images") or []
            if isinstance(images, list):
                for it in images:
                    if isinstance(it, dict):
                        url = it.get("large_url") or it.get("base_url") or it.get("url")
                        if url:
                            imgs.append(url)
                    elif isinstance(it, str):
                        imgs.append(it)
            rec["images"] = imgs
            records.append(rec)
        else:
            records.append({"id": pid, "error": err})
    # ghi file jsonl
    with open(outpath, "w", encoding="utf-8") as fw:
        for r in records:
            fw.write(json.dumps(r, ensure_ascii=False) + "\n")


def main(ids_path: str, outdir: str = OUTDIR, chunk: int = 1000):
    ids = load_ids(ids_path)
    print(f"Loaded {len(ids)} ids from {ids_path}")
    if not ids:
        print("No ids found, exiting.")
        return
    os.makedirs(outdir, exist_ok=True)
    total_chunks = (len(ids) + chunk - 1) // chunk
    session = requests.Session()

    for idx, ids_chunk in enumerate(chunkify(ids, chunk), start=1):
        outpath = os.path.join(outdir, f"products_{idx:05d}.jsonl")
        print(f"Processing chunk {idx}/{total_chunks} -> {outpath} (size={len(ids_chunk)})")
        process_chunk(session, ids_chunk, outpath)
        # small pause between chunks to be polite to server (tweak as needed)
        time.sleep(0.5)

    print("All chunks done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple chunked product downloader")
    parser.add_argument("--ids", required=True, help="CSV/TXT file with product ids")
    parser.add_argument("--chunk", type=int, default=1000, help="Chunk size (records per output file)")
    args = parser.parse_args()

    main(args.ids, chunk=args.chunk)

# python self_product_download.py --ids test_10000_products.csv --outdir output --chunk 1000

# def main(ids_path: str):
#     ids = load_ids(ids_path)
#     print(f"Loaded {len(ids)} ids from {ids_path}")
#     os.makedirs(OUTDIR, exist_ok=True)

#     mode = "a" if os.path.exists(OUTFILE) else "w"
#     session = requests.Session()
#     total = len(ids)
#     with open(OUTFILE, mode, encoding="utf-8") as out_f:
#         for pid in tqdm(ids, desc="Processing", unit="id"):
#             data, err = fetch_product(session, pid)
#             if data:
#                 if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
#                     data = data["data"]
#                 if isinstance(data, dict) and "product" in data and isinstance(data["product"], dict):
#                     data = data["product"]
#                 record = {
#                     "id": data.get("id") or data.get("product_id") or pid,
#                     "name": data.get("name") or data.get("title"),
#                     "price": data.get("price") or data.get("final_price") or data.get("current_price"),
#                     "url_key": data.get("url_key") or data.get("url_path"),
#                 }
#                 desc_html = data.get("description") or data.get("short_description") or data.get("long_description") or ""
#                 record["description"] = simple_clean_description(desc_html)

#                 images = data.get("images") or []
#                 if isinstance(images, list):
#                     imgs = []
#                     for it in images:
#                         if isinstance(it, dict):

#                             url = it.get("large_url") or it.get("base_url") or it.get("url")
#                             if url:
#                                 imgs.append(url)
#                         elif isinstance(it, str):
#                             imgs.append(it)
#                     record["images"] = imgs
#                 else:
#                     record["images"] = []
#                 out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
#             else:
#                 out_f.write(json.dumps({"id": pid, "error": err}, ensure_ascii=False) + "\n")
#     print(f"Done. Output saved to {OUTFILE}")


# if __name__ == "__main__":
#     import sys
#     if len(sys.argv) < 2:
#         print("Usage: python simple_product_downloader.py /path/to/ids.csv")
#         sys.exit(1)
#     main(sys.argv[1])
