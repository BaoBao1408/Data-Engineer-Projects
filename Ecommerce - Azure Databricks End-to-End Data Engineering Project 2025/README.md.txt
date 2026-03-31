📌 Azure Databricks End-to-End Data Engineering Project 2025
A comprehensive hands-on Data Engineering project using Azure Databricks, Delta Lake, PySpark, and Delta Live Tables (DLT). This project showcases how to build a full ETL pipeline from scratch by applying dimensional modeling, SCD Type 1, and Star Schema in a production-like setup.

✅ Project Goals
Build a real-time-ready data pipeline using Azure services

Practice dimensional data modeling with Fact & Dimension tables

Handle Slowly Changing Dimensions (SCD Type 1) using Delta Lake merge

Visualize and orchestrate the pipeline using Databricks Workflows (DAG)

🔧 Tools & Technologies
Category	Tools
Cloud	Azure
Compute	Azure Databricks
Storage	Azure Data Lake Gen2 (ADLS Gen2)
Processing	PySpark, Delta Lake, Delta Live Tables
Orchestration	Databricks Workflows (Pipelines)
Modeling	Star Schema, SCD Type 1
BI (Optional)	Power BI

🧱 Project Architecture

Bronze Layer: Raw ingestion using AutoLoader  
Silver Layer: Cleaned & joined data  
Gold Layer: Fact + Dimension tables in Star Schema  

🛠️ Pipeline Design
The DLT Workflow includes:

🔹 Bronze_Autoloader_iteration

🔹 Silver_Customers

🔹 Silver_Orders

🔹 Silver_Products

🔹 Gold_Customers

🔹 Gold_Products

🔹 Fact_Orders

All notebooks run via a shared job cluster configured in the DLT pipeline settings.


🗂️ Dataset
Source: Public e-commerce CSV files

Ingestion method: AutoLoader into ADLS Gen2

Bronze storage path:
abfss://bronze@databrickseteqb.dfs.core.windows.net

📈 Data Modeling
✔️ Star Schema using:

Fact_Orders

Dim_Customers

Dim_Products

✔️ SCD Type 1 implemented using DeltaTable.merge()

📁 Project Structure
bash
Databricks ETE Project/
│
├── parameters.py
├── Bronze_Autoloader_iteration.py
├── Silver_Customers.py
├── Silver_Orders.py
├── Silver_Products.py
├── Gold_Customers.py
├── Gold_Products.py
└── Fact_Orders.py

💡 Archived
Worked with Delta Live Tables for ETL orchestration

Implemented SCD Type 1 with Delta merge

Optimized cluster configuration for Azure Student Plan

Troubleshooted region, vCPU quota, and resource limits

📚 Author
🙋‍♂️ Implemented by: Nguyen Quoc Bao
