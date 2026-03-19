# Glamira Data Engineering Pipeline

A production-style **Data Engineering pipeline** for extracting, enriching, and storing product interaction data from Glamira event logs.

This project simulates a real-world **data engineering workflow**, including:

* Event processing
* Asynchronous web scraping
* IP geolocation enrichment
* Structured storage in MongoDB

The pipeline processes user interaction events, extracts product URLs, crawls product information from the Glamira website, and enriches user IP addresses with geographic location data.

---

# Pipeline Architecture

The pipeline follows a layered architecture commonly used in data engineering systems.

```
Raw Data → Extraction → Enrichment → Storage
```

### Data Flow

```
User Events
   │
   ▼
Extract Product URLs
   │
   ▼
product_urls.json
   │
   ▼
Async Product Scraper
   │
   ▼
MongoDB (products_raw)
   │
   ▼
IP Enrichment
   │
   ▼
processed_ip_location
```

---

# Project Structure

```
glamira_project
│
├── config
│   ├── mongo_connection.py
│   └── .env
│
├── pipelines
│   ├── user_events
│   │   └── extract_product_urls.py
│   │
│   ├── product_scraper
│   │   ├── product_ex.py
│   │   └── run_scraper.py
│   │
│   └── ip_enrichment
│       ├── ip_lookup.py
│       └── run_ip_enrichment.py
│
├── data
│   ├── raw
│   │   └── product_urls.json
│   │
│   ├── product_extract
│   │   ├── processed_id.txt
│   │   └── failed_id.jsonl
│   │
│   └── processed_ip_location
│       └── ip_locations.csv
│
├── source
│   ├── product
│   │   └── product_ex.py
│   │
│   └── ip_geolocation
│       └── ip_lookup.py
│
├── notebooks
├── docs
└── main.py
```

---

# Features

### Async Web Scraping

The scraper uses **AsyncIO + curl_cffi** for high-performance crawling.

Features include:

* Concurrent asynchronous requests
* Retry handling
* Failure logging
* Checkpoint recovery

### Data Enrichment

User IP addresses are enriched with geolocation data using the **IP2Location database**.

### Data Storage

Processed product data is stored in **MongoDB collections** for further analytics and data processing.

---

# Installation

## 1. Clone repository

```
git clone https://github.com/your_repo/glamira_data_pipeline.git
cd glamira_project
```

## 2. Create virtual environment

Linux / Mac:

```
python -m venv .venv
source .venv/bin/activate
```

Windows:

```
python -m venv .venv
.venv\Scripts\activate
```

## 3. Install dependencies

```
pip install -r requirements.txt
```

---

# Environment Configuration

Create a `.env` file inside the **config/** directory.

Example:

```
MONGO_URI=mongodb://localhost:27017
DB_NAME=glamira
```

---

# Running the Pipeline

## Step 1 — Extract product URLs from event logs

```
python pipelines/user_events/extract_product_urls.py
```

This generates:

```
data/raw/product_urls.json
```

---

## Step 2 — Crawl product information

```
python source/product/product_ex.py
```

The crawler collects:

* product name
* price
* alloy
* size
* product URL

and stores them in **MongoDB**.

---

## Step 3 — Enrich IP addresses with geolocation

```
python source/ip_geolocation/ip_lookup.py
```

This step enriches user IPs with location data using **IP2Location**.

---

# Example MongoDB Output

Collection:

```
products_raw
```

Example document:

```
{
  "product_id": "123456",
  "name": "Diamond Ring",
  "price": 999,
  "alloy": "White Gold",
  "size": "54",
  "url": "https://www.glamira.com/..."
}
```

---

# Tech Stack

| Component       | Technology         |
| --------------- | ------------------ |
| Language        | Python             |
| Async Scraping  | AsyncIO, curl_cffi |
| HTML Parsing    | BeautifulSoup      |
| Database        | MongoDB            |
| Data Enrichment | IP2Location        |
| Environment     | Python venv        |

---

# Future Improvements

Planned enhancements for the pipeline:

* Kafka-based event streaming
* Airflow orchestration
* Spark data processing layer
* Data warehouse integration

---

# Author

**Nguyen Quoc Bao**
**baoquocnguyen1408@gmail.com**

Aspiring Data Engineer
