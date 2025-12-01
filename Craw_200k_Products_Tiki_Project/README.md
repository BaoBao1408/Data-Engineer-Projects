Tiki Product Crawler (Async Python Project)



This project is an end-to-end asynchronous data crawler designed to fetch product information from Tiki’s public product-detail API.

It uses aiohttp, asyncio, BeautifulSoup, and text-cleaning pipelines to build a clean dataset ready for downstream ETL or analytics workloads.



📁 Project Structure

Craw\_Data\_Project/

│

├── .venv/                      # Python virtual environment

├── output/                     # JSONL output files (products\_00001.jsonl, ...)

├── product\_download\_full.py    # main crawler script

├── products-0-200000.csv       # input raw product IDs

├── test\_10000\_products.csv     # optional test subset

├── requirements.txt

└── README.md



🚀 1. Setup Instructions

1\. Create project folder

mkdir Craw\_Data\_Project

cd Craw\_Data\_Project

code .



2\. Create and activate a virtual environment

Windows (PowerShell)

python -m venv .venv

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

.\\.venv\\Scripts\\Activate.ps1



macOS / Linux

python3 -m venv .venv

source .venv/bin/activate





After activation, you will see:



(.venv)



3\. Install dependencies

pip install -r requirements.txt





If you don't have the file yet:



pip install aiohttp aiofiles beautifulsoup4 tqdm

pip freeze > requirements.txt



4\. Select Interpreter in VS Code



Press Ctrl + Shift + P



Choose Python: Select Interpreter



Select:



.venv/Scripts/python.exe



📥 2. Preparing Input Data



Copy your product ID CSV to the project folder:



products-0-200000.csv



Create a 1,000-ID test file (recommended before running full)

PowerShell:

Get-Content products-0-200000.csv | Select-Object -First 1000 | Set-Content test\_1000\_products.csv



macOS/Linux:

head -n 1000 products-0-200000.csv > test\_1000\_products.csv



⚙️ 3. Running the Script

Run test (1,000 IDs)

python product\_download\_full.py --ids test\_1000\_products.csv --outdir output\_test --chunk 1000 --concurrency 20 --limit-per-host 10 --retries 3 --timeout 30





Expected output:



output\_test/

└── products\_00001.jsonl



Run full 200,000 IDs (with resume mode)

python product\_download\_full.py --ids products-0-200000.csv --outdir output --chunk 1000 --concurrency 50 --limit-per-host 10 --retries 3 --timeout 30 --resume





Produces: products\_00001.jsonl, products\_00002.jsonl, ..., products\_000200.jsonl



Each file contains 1000 product records in JSON Lines format



🔎 4. Code Flow Explanation

Step 1: Load IDs



load\_ids\_from\_csv()



Reads CSV or TXT



Removes duplicates



Returns a clean list of product IDs



Step 2: Chunking



chunk\_size=1000



IDs 0–999 → products\_00001.jsonl



IDs 1000–1999 → products\_00002.jsonl



… etc.



Step 3: Async fetching



asyncio + aiohttp handles:



concurrency control



connection pooling



retry logic



timeout handling



HTTP error handling (404, 403, etc.)



Step 4: Cleaning product data



extract\_fields():



unwraps nested JSON (data, product)



extracts:



id



name



price



description



image URLs



clean\_description():



Strip HTML



Remove noisy whitespace



Keep first 1–2 meaningful sentences



Trim to max length



Step 5: Save output



Each product → 1 JSON object per line (JSONL format).



Example line:



{"id": "200828469", "name": "...", "price": 1149000, "description": "...", "images": \["url1","url2"]}



📦 5. Output Format



Each output file contains 1000 JSON objects, e.g.:



products\_00001.jsonl

products\_00002.jsonl

...





Each line:



{"id": "123456", "name": "Product Name", "price": 150000, "description": "Cleaned description...", "images": \["..."]}





If error:



{"id": "123456", "error": "HTTP 404"}



📈 6. Why this project is valuable



You practice real-world Data Engineering skills:



Async I/O at scale



API-based data ingestion



HTML → clean text processing



Error handling \& retries



Chunked output



Virtual environment + dependency management



Data pipeline design



JSON normalization



This is exactly the kind of logic used before loading data into S3, Glue, Redshift, BigQuery, or Spark.



🎯 7. Next Steps (optional improvements)



Add --max-chunks for testing



Write unit tests for text cleaning functions



Add logging to file



Upload JSONL files to S3



Build a Glue → Redshift ETL pipeline



Convert JSONL to Parquet using PyArrow

