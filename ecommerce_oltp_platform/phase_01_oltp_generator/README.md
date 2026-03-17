# 🛒 E-commerce OLTP Data Generator (Phase 01)

## Overview

This phase implements a **production-like E-commerce OLTP data generator** using **Python and PostgreSQL**, designed to simulate realistic transactional workloads at **multi-million row scale**.

The generator produces **business-consistent, analytics-ready OLTP data** with strong referential integrity and resumable execution. It acts as the **source system** for downstream Data Engineering phases (Analytics, Reporting, Cloud Pipelines, BI).

This is intentionally **not a toy dataset** — the data volume, relationships, and constraints are designed to surface real-world performance and modeling challenges.

---

## ✨ Key Features

* 🚀 Generate **millions of OLTP records** efficiently
* 🔁 **Checkpoint & resume** support for long-running jobs
* 🧠 **Business-aware data generation** (not random noise)
* ⚡ High-performance **batch inserts** with progress tracking
* 🔗 Strict **foreign key relationships**
* 📊 Fully compatible with **analytics & ETL workloads**

---

## 🧱 Tech Stack

* **Python 3.12**
* **PostgreSQL**
* **SQLAlchemy (Core)**
* **psycopg2**
* **Poetry** (dependency management)
* **tqdm** (progress visualization)

---

## 🗄️ OLTP Database Schema

### Core Entities

* `category` – hierarchical (self-referencing)
* `brand`
* `seller`
* `product`
* `promotion`
* `promotion_product` (many-to-many)
* `orders`
* `order_item`

### Key Relationships

* `product` → `category`, `brand`, `seller`
* `orders` → `seller`
* `order_item` → `orders` → `product`
* `promotion_product` → `promotion` ↔ `product`

All relationships are enforced using **foreign keys** to ensure data integrity under scale.

---

## 🧠 Business Logic Highlights

### Orders

* Order dates constrained to **Aug–Oct** (configurable)
* Weighted order status distribution:

  * `DELIVERED`
  * `PLACED`
  * `CANCELLED`
* `created_at` derived from `order_date`
* `total_amount` is **calculated from order_items**, never random

### Order Items

* **2–5 items per order**
* `subtotal = quantity × unit_price`
* `order_date` and `created_at` inherited from parent order

### Promotions

* Active periods between **2022–2025**
* Discount types:

  * `percentage` (5–20%)
  * `fixed_amount` (realistic absolute values)
* Discount logic stored **only in promotion tables**
* Final discounted price calculated **at query time** (analytics-ready)

---

## 📁 Project Structure

```text
ecommerce_oltp_platform/
│
├── docs/
│
├── phase_01_oltp_generator/
│   │
│   ├── checkpoints/
│   │   └── processed_state.json        # Resume state for generators
│   │
│   ├── config/
│   │   └── settings.py                 # Volumes, date ranges, distributions
│   │
│   ├── db/
│   │   ├── connection.py               # SQLAlchemy engine
│   │   ├── ddl.sql                     # OLTP schema
│   │   └── run_ddl.py                  # Schema execution
│   │
│   ├── generators/
│   │   ├── category.py
│   │   ├── brand.py
│   │   ├── seller.py
│   │   ├── product.py
│   │   ├── promotion.py
│   │   ├── promotion_product.py
│   │   ├── order.py
│   │   └── order_item.py
│   │
│   ├── loaders/
│   │   └── bulk_insert.py              # High-performance inserts
│   │
│   ├── scripts/
│   │   └── run_ddl.py
│   │
│   ├── utils/
│   │   ├── checkpoint.py               # Resume logic
│   │   ├── logger.py
│   │   ├── time_helper.py
│   │   ├── order_status.py
│   │   ├── discount.py
│   │   └── update_order_totals.py
│   │
│   ├── main.py                         # Orchestration entrypoint
│   ├── test_connection.py
│   ├── pyproject.toml
│   └── README.md
│
├── phase_02_sql_analytics/
├── phase_03_cloud_pipeline/
└── README.md
```

---

## ▶️ How to Run

### 1️⃣ Create Database Schema

```bash
python -m scripts.run_ddl
```

### 2️⃣ Generate Data

```bash
python main.py
```

The pipeline automatically:

* Skips completed steps
* Resumes from last checkpoint
* Logs progress with timestamps

---

## 📊 Performance Characteristics

Typical throughput on **local PostgreSQL**:

| Table      | Throughput (rows/sec) |
| ---------- | --------------------- |
| orders     | ~6,000 – 7,000        |
| order_item | ~1,000 – 2,000        |

Throughput intentionally degrades after ~30% due to:

* Index maintenance
* WAL pressure
* Disk I/O contention

This behavior is **by design** to reflect real OLTP constraints.

---

## 🔢 Data Volume (Configurable)

Defined in `config/settings.py`:

* Orders: ~3.8M
* Order items: ~13M+
* Products: ~1K
* Promotions: ~500

---

## 🎯 Why This Project Exists

This generator is built to:

* Mimic **real OLTP behavior**
* Produce **ETL-worthy data**
* Expose **performance & modeling challenges**
* Serve as a **Data Engineer portfolio foundation**

It intentionally prioritizes **realism over convenience**.

---

## 🔮 Next Phases

* **Phase 02** – SQL Analytics & Reporting
* **Phase 03** – Cloud Pipeline (Airflow, dbt, BigQuery / Redshift)
* **Phase 04** – BI Layer & Advanced Optimization

---

## 🙌 Author Notes

Built as part of a hands-on **Data Engineering learning path**, focused on:

* Realistic systems
* Measurable performance
* Production-style thinking

If you are reviewing this repository:
👉 this dataset is intentionally designed to be **hard** — just like real life.

## 🙌 Author
## 🙌 Name: Quoc Bao
## 🙌 Email: Baoquocnguyen1408@gmail.com
