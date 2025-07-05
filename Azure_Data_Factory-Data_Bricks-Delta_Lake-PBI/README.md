✅ README: Azure End-to-End Data Engineering & Analytics Project – NYC Taxi Trips

📌 Project Overview
This is a complete data engineering and analytics project using:

Azure Data Factory (ADF) – For automated data ingestion

Azure Data Lake Gen2 – For multi-layered storage: bronze/, silver/, gold/

Azure Databricks + PySpark – For data cleaning, transformation, and analysis

Delta Lake – For robust, versioned storage with Time Travel capabilities

Power BI – For rich, interactive dashboards analyzing NYC taxi movement

🗺️ Detailed Workflow
1️⃣ Azure Environment Setup
Created Resource Group and Azure Data Lake Gen2

Structured layered folders: bronze/, silver/, gold/

Configured Managed Identity and granted Storage Blob Data Contributor role

Registered Credentials and External Locations in Unity Catalog (Databricks)

2️⃣ Data Ingestion with Azure Data Factory
Used Copy Activity to ingest CSV data into bronze/ layer

Source: NYC Taxi Trip dataset

Format: Parquet

Scheduled ingestion via pipeline

3️⃣ Silver Layer Processing (Notebook: silver_notebook)
Read data from the bronze layer

Performed data cleaning:

Removed null/negative values from fare_amount, trip_distance

Enriched data:

Joined with trip_type, pickup_zone, and dropoff_zone using LocationID

Wrote results to silver Delta tables:

silver.trip_trip

silver.trip_zone

4️⃣ Gold Layer Aggregation (Notebook: gold_notebook)
Read from silver Delta tables

Created final analytical tables:

gold.trip_trip: complete enriched trip records

gold.trip_zone: standardized zone information with Zone1, Zone2

Wrote results to gold layer in Delta format

Applied performance optimization:

OPTIMIZE gold.trip_trip

VACUUM gold.trip_trip RETAIN 168 HOURS

5️⃣ Dashboarding with Power BI ✅ (Completed)
Connected Power BI directly to Databricks via SQL Warehouse

Built interactive dashboards with:

KPI Cards: Total Trips, Total Revenue, Avg Fare, Avg Distance

Bar Charts: Revenue by Borough, Zone, Vendor

Matrix Heatmap: Pickup Zone to Dropoff Zone flows

Slicers: Filter by trip_type, borough, fare_amount

🧠 Analytical Thinking & Insights
Applied Medallion Architecture for clear data lifecycle separation

Segmented customer behavior via trip_type: Dispatch vs Street-Hail

Identified hot zones (high traffic routes) for pricing/dispatch strategies

Compared performance across VendorID to optimize operations

Used Top-N filtering to focus on high-impact zones

📊 Project Status
Deliverable	Status
ADF ingestion pipeline to Bronze		✅
Data cleaning and transformation to Silver	✅
Final modeling and optimization in Gold layer	✅
Delta Lake features (OPTIMIZE, VACUUM, HISTORY)	✅
Power BI dashboard and reporting		✅

🔗 References
NYC TLC Trip Data: 🔗 Official Dataset