Glamira Data Engineering Pipeline

A production-style data pipeline for extracting, enriching, and storing product interaction data from Glamira event logs.

This project simulates a real-world Data Engineering workflow, including event processing, asynchronous web scraping, IP geolocation enrichment, and structured data storage in MongoDB.

The system processes user interaction events, extracts product URLs, crawls product information from the Glamira website, and enriches user IP addresses with geographic location data.

Pipeline Architecture
                    MongoDB (glamira_raw)
                          │
                          ▼
              Event Processing Pipeline
         Extract product URLs from user events
                          │
                          ▼
                product_urls.jsonl
                          │
                          ▼
                Product Scraper Pipeline
         Crawl product information from Glamira
                          │
                          ▼
              MongoDB (collection_product)
                          │
                          ▼
               IP Enrichment Pipeline
          Enrich user IPs using IP2Location
                          │
                          ▼
                    ip_locations.csv

This architecture represents a typical layered data pipeline:

Raw Data → Extraction → Enrichment → Storage
Project Structure
glamira_project
│
├── pipelines
│   ├── user_events
│   │   └── extract
│   │       └── extract_product_urls.py
│   │
│   ├── product_scraper
│   │   └── extract
│   │       ├── product_extractor.py
│   │       └── product_ex.py
│   │
│   └── ip_enrichment
│       ├── ip_lookup.py
│       └── ip_geolocation
│           └── IP-COUNTRY-REGION-CITY.BIN
│
├── scripts
│   ├── run_event_pipeline.py
│   ├── run_scraper.py
│   └── run_ip_enrichment.py
│
├── config
│   └── mongo_connection.py
│
├── data
│   ├── raw
│   │   ├── product_urls.jsonl
│   │   └── checkpoint.txt
│   │
│   ├── product_extract
│   │   ├── processed_id.txt
│   │   └── failed_id.jsonl
│   │
│   └── processed_ip_location
│       └── ip_locations.csv
│
├── main.py
└── README.md

The project follows a modular pipeline structure to simulate real production data workflows.

Data Storage (MongoDB)

Data is stored in MongoDB using the following structure:

glamira_dataset
   └── glamira
        ├── glamira_raw
        └── collection_product
glamira_raw

Contains raw user interaction events including:

product views

recommendation interactions

add-to-cart events

user IP addresses

page navigation data

Example fields:

{
  "collection": "view_product_detail",
  "product_id": "104178",
  "current_url": "https://www.glamira.com/glamira-ring-jaselle.html",
  "ip": "192.168.1.1"
}
collection_product

Contains scraped product data extracted from Glamira product pages.

Example:

{
  "product_id": "104178",
  "url": "https://www.glamira.com/glamira-ring-jaselle.html",
  "react_data": {
    "name": "Glamira Ring Jaselle",
    "price": 899,
    "variants": [...],
    "materials": [...],
    "sizes": [...]
  }
}
Pipelines
1. Event Processing Pipeline

Extracts product URLs from user interaction events stored in MongoDB.

Source collection:

glamira_raw

Events processed:

view_product_detail
select_product_option
select_product_option_quality
add_to_cart_action
product_detail_recommendation_visible
product_detail_recommendation_noticed
product_view_all_recommend_clicked

Output:

data/raw/product_urls.jsonl

Run pipeline:

python scripts/run_event_pipeline.py
2. Product Scraper Pipeline

Crawls Glamira product pages asynchronously and extracts structured product data.

Features:

asynchronous HTTP crawling

retry mechanism

rate limiting

checkpoint resume

failed request logging

real-time MongoDB insertion

BeautifulSoup HTML parsing

extraction of React-based page data

Output:

MongoDB → collection_product

Run pipeline:

python scripts/run_scraper.py
3. IP Enrichment Pipeline

Extracts unique user IP addresses from event logs and enriches them using the IP2Location database.

Source:

glamira_raw

Output:

data/processed_ip_location/ip_locations.csv

Run pipeline:

python scripts/run_ip_enrichment.py
Running the Full Pipeline

To execute the entire pipeline sequentially:

python main.py --all

Run individual components:

python main.py --events
python main.py --scraper
python main.py --ip
Technologies Used

Core stack used in this project:

Python

MongoDB

AsyncIO

BeautifulSoup

IP2Location

tqdm

curl_cffi

JSONL data pipelines

Engineering Highlights

This project demonstrates several key Data Engineering practices:

modular pipeline architecture

asynchronous web scraping

checkpoint-based resume logic

MongoDB document storage

event-driven data extraction

IP geolocation enrichment

structured project organization

The architecture mirrors real-world ETL/ELT workflows used in data platforms.

Example MongoDB View

MongoDB Compass visualization:

glamira_dataset
   └── glamira
        ├── glamira_raw
        └── collection_product
Future Improvements

Potential improvements for scaling this pipeline include:

workflow orchestration with Apache Airflow

streaming ingestion using Kafka

transformation layer using dbt

loading structured data into a data warehouse

analytics dashboards (Power BI / Superset)

containerization with Docker

deployment on cloud infrastructure

Author

Nguyen Quoc Bao
Email: baoquocnguyen1408@gmail.com

Data Engineering project for practicing real-world pipeline design, event processing, and web data extraction.