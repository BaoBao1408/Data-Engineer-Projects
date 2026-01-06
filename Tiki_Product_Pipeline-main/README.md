# 🛒 Tiki Product Data Pipeline

A **production-style Data Engineering pipeline** to collect, process, and store large-scale product data (~200,000 products) from **Tiki.vn** using asynchronous I/O and multi-threading.

> 🚀 Designed for **Data Engineer / Big Data Engineer portfolio**

---

## 📌 Key Features

* ⚡ **High-performance crawling** with `asyncio` + `aiohttp`
* 🧵 **Multi-threaded data processing** for CPU-bound tasks
* 🧩 **Modular pipeline architecture** (Loader → Fetcher → Transformer → Writer)
* ♻️ **Resume & checkpoint** (auto-continue after crash)
* 🗑️ **Dead Letter Queue** for failed product IDs
* ⏱️ **Monitoring**: total records & execution time
* 🚨 **Alerting** via Google Drive sync folder
* 🔄 **Reset mode** to rerun pipeline from scratch

---

## 🏗️ Architecture Overview

```text
CSV (product_id)
   │
   ▼
[Loader]
   │
   ▼
[Fetcher]  (async I/O)
   │
   ├── success ──▶ [Transformer] ──▶ [Writer] ──▶ output/*.json
   │
   └── failed  ──▶ [Error Handler] ──▶ errors/*.json
   │
   ▼
[Checkpoint + Monitor]
```

---

## 📂 Project Structure

```text
tiki_product_pipeline/
│
├── pipelines/
│   ├── loader.py         # Load product IDs from CSV
│   ├── fetcher.py        # Async API calls
│   ├── transformer.py   # Data normalization
│   ├── writer.py        # JSON batch writer
│   ├── checkpoint.py    # Resume & progress tracking
│   ├── error_handler.py # Dead Letter Queue
│   ├── monitor.py       # Metrics & alerting
│   └── reset.py         # Reset pipeline state
│
├── config.py             # Global configuration
├── main.py               # Pipeline orchestrator
├── requirements.txt
│
├── output/               # Successful data batches
├── errors/               # Failed product IDs
├── checkpoints/          # Resume state
├── gdrive_alert/         # Alert folder (Google Drive sync)
└── venv/                 # Virtual environment (ignored)
```

---

## ⚙️ Tech Stack

* **Language**: Python 3.10+
* **Async I/O**: asyncio, aiohttp
* **Concurrency**: ThreadPoolExecutor
* **Data Processing**: pandas, BeautifulSoup
* **Monitoring**: tqdm, custom metrics

---

## 🚀 Getting Started

### 1️⃣ Setup Environment

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2️⃣ Run Pipeline (Resume Mode – default)

```bash
python main.py
```

### 3️⃣ Run Pipeline from Scratch (Reset Mode)

```bash
python main.py --reset
```

> Reset mode will remove `output/`, `errors/`, and `checkpoints/`

---

## 📊 Output Format

Each batch (~1000 products):

```json
{
  "id": 1380832182,
  "name": "Product name",
  "url_key": "product-url",
  "price": 199000,
  "description": "Clean text description",
  "images": ["https://..."]
}
```

---

## ❌ Error Handling (Dead Letter Queue)

Failed product IDs are logged for later retry:

```json
{
  "product_id": "1380832182",
  "batch": 5,
  "stage": "fetch",
  "error": "timeout"
}
```

---

## 🔁 Resume & Idempotency

* Progress is tracked via `checkpoints/progress.json`
* Pipeline can safely resume after interruption
* Reset mode ensures no duplicated data

---

## 📈 Performance Notes

* Batch size: **1000 products**
* Concurrent connections: **40**
* Optimized for laptop environments
* Scales to hundreds of thousands of records

---

## 🚧 Limitations

* API rate limiting may occur
* Retry with exponential backoff not yet implemented
* Output format currently JSON only

---

## 🔮 Future Improvements

* Retry + exponential backoff
* Export to Parquet / DuckDB
* Deduplication by `product_id`
* Airflow DAG integration
* Dockerized deployment

---

## 👨‍💻 Author

**Tiki Product Data Pipeline**
Designed as a **Data Engineering portfolio project**.

If you are a recruiter or interviewer, this project demonstrates:

* Pipeline orchestration
* Fault tolerance
* Async & concurrent processing
* Production-ready design thinking

---

⭐ If you find this project useful, feel free to star the repository!
