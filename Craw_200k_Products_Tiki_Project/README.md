\# Crawling 200K Tiki Products to PostgreSQL



This is an end-to-end \*\*Data Engineering mini project\*\* that demonstrates how to:



\- Crawl \*\*200,000 product IDs\*\* from an API using \*\*asynchronous Python\*\*

\- Store raw data as \*\*JSONL chunks\*\*

\- Handle \*\*resume, retries, and error logging\*\*

\- Load clean data into \*\*PostgreSQL\*\*

\- Track performance and optimize crawling throughput



---



\## 1. Tech Stack



\- \*\*Language:\*\* Python 3.x  

\- \*\*Async Crawling:\*\* `asyncio`, `aiohttp`, `aiofiles`  

\- \*\*HTML Parsing:\*\* `beautifulsoup4`  

\- \*\*Utilities:\*\* `tqdm`, `pandas`, `ujson`, `pyarrow`  

\- \*\*Database:\*\* PostgreSQL  

\- \*\*Postgres Driver:\*\* `psycopg2-binary`  

\- \*\*Environment:\*\* `venv`  

\- \*\*Version Control:\*\* Git + GitHub  



---



\## 2. Project Structure



```text

Craw\_200k\_Products\_Tiki\_Project/

├─ .gitignore

├─ README.md

├─ requirements.txt

├─ products-0-200000.csv        # Input: 200K product IDs

├─ crawl\_data.py                # Async crawler

├─ note.txt                     # Development \& performance notes

├─ config.py                    # Read database.ini

├─ connect.py                   # Test connection to PostgreSQL

├─ create\_table.py              # Create destination table

├─ load\_products.py             # Load JSONL data into PostgreSQL

├─ database.ini                 # Database credentials (ignored by git)

├─ output/                      # Crawled JSONL chunks (ignored by git)

└─ output\_error/                # Error logs and failed IDs (ignored by git)

⚠️ The following are NOT committed to GitHub via .gitignore:



output/



output\_error/



\*.json, \*.jsonl



database.ini



.venv/



3\. Environment Setup



3.1 Create Virtual Environment

python -m venv .venv

Activate (PowerShell):



.\\.venv\\Scripts\\Activate.ps1

VS Code:



Ctrl + Shift + P → Python: Select Interpreter → 

.\\Craw\_200k\_Products\_Tiki\_Project\\.venv\\Scripts\\python.exe



3.2 Install Dependencies

pip install -r requirements.txt

requirements.txt:



aiohttp>=3.8.0

aiofiles

beautifulsoup4

tqdm

pandas

ujson

pyarrow

psycopg2-binary

(Optional regenerate)



pip freeze > requirements.txt

4\. PostgreSQL Configuration



4.1 Create Database

CREATE DATABASE "Tiki\_Data";



4.2 Configure database.ini

\[postgresql]

host=localhost

database=Tiki\_Data

user=postgres

password=123456

port=5432

⚠️ This file is ignored by Git to protect credentials.



4.3 Create Table

python create\_table.py

Table schema:



CREATE TABLE IF NOT EXISTS tiki\_products (

&nbsp;   id           BIGINT PRIMARY KEY,

&nbsp;   name         TEXT,

&nbsp;   url\_key      TEXT,

&nbsp;   price        BIGINT,

&nbsp;   description  TEXT,

&nbsp;   images       JSONB

);



5\. Crawling 200K Products



5.1 Clean Old Output

Remove-Item -Recurse -Force output, output\_error

\# or:

ri output, output\_error -r -force



5.2 Run Crawler

python crawl\_data.py \\

&nbsp; --ids products-0-200000.csv \\

&nbsp; --outdir output \\

&nbsp; --errordir output\_error \\

&nbsp; --chunk 1000 \\

&nbsp; --concurrency 50 \\

&nbsp; --retries 3 \\

&nbsp; --resume

Parameters Explained

Parameter	Meaning

--ids	CSV file with product IDs

--outdir	Output folder for crawled JSONL files

--errordir	Folder for error logs

--chunk	Number of IDs per batch

--concurrency	Number of async requests

--retries	Retry count on failures

--resume	Resume from last successful run



5.3 Output Files

output/products\_00001.jsonl → products\_00200.jsonl



output/processed\_success.txt → Resume checkpoint



output\_error/errors\_summary.json → Error summary



output\_error/failed\_ids\_to\_retry.txt → Failed IDs + reason



6\. Load Data into PostgreSQL

python load\_products.py

Features:



Supports both JSON array and JSONL



Batch insert using execute\_values



Auto-upsert using:



ON CONFLICT (id) DO UPDATE



7\. Validation Queries

-- Total rows

SELECT COUNT(\*) FROM tiki\_products;



-- First records

SELECT \* FROM tiki\_products LIMIT 5;



-- Top 10 highest price products

SELECT id, name, price

FROM tiki\_products

ORDER BY price DESC

LIMIT 10;



-- Search by keyword

SELECT id, name, price

FROM tiki\_products

WHERE name ILIKE '%xe máy điện%'

LIMIT 20;



-- Extract first image

SELECT id, name, images->0 AS first\_image

FROM tiki\_products

LIMIT 10;



8\. Final Results

Metric	Value

Total Input IDs	200,000

Successfully Inserted	198,942

Failed (404 Not Found)	1,058

Start Time	09:27:06

End Time	10:43:05

Total Time	~76 minutes (~1.27 hours)



9\. Performance Analysis

Stack: asyncio + aiohttp + aiofiles



concurrency = 50, chunk = 1000



Average per chunk: 25–36 seconds



Throughput:



1000 / 25s ≈ 40 req/s



1000 / 36s ≈ 28 req/s



Bottlenecks

TCP connection limits (limit\_per\_host)



Network latency



CPU-bound HTML parsing (BeautifulSoup)



API throttling and backoff



Python GIL during parsing



10\. Optimization Roadmap

Increase limit\_per\_host (50–100)



Separate raw crawling and parsing pipeline



Replace BeautifulSoup with:



lxml



selectolax



Use orjson for faster JSON parsing



Check for batch API endpoints



Compress raw output (.jsonl.gz)



11\. Git \& Large File Handling

.gitignore:



output/

output\_error/

\*.json

\*.jsonl

raw\_\*.json

\*.gz

database.ini

.venv/

\_\_pycache\_\_/

If files were already tracked:



git rm -r --cached output

git rm -r --cached output\_error

git rm -r --cached \*.jsonl

git commit -m "Remove output data from git tracking"



12\. Summary

✅ Async crawl 200K products

✅ Resume \& retry support

✅ Full error logging

✅ PostgreSQL ingestion

✅ Performance measurement

✅ Production-style folder structure

✅ Git safe for large data \& secrets



This project demonstrates a real-world data pipeline:



CSV → Async API Crawl → JSONL → PostgreSQL

It is suitable as a Data Engineer portfolio project.



---



If you want, I can also:

\- Add \*\*architecture diagram\*\*

\- Add \*\*flow chart\*\*

\- Add \*\*ERD for PostgreSQL\*\*

\- Add \*\*benchmark comparison before/after optimization\*\*



Just tell me ✅

baoquocnguyen1408@gmail.com

