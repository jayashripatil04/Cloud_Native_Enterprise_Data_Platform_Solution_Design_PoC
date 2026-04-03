# 🚀 Data Engineering PoC (AWS)

## 📌 What this PoC does
This project implements a simple **end-to-end data pipeline** using AWS.

- Ingests CSV data into S3
- Processes data using AWS Glue
- Cleans and transforms data
- Stores data in staging (Parquet)
- Creates aggregated datasets in curated layer
- Enables querying using Athena

---

## 🔄 Pipeline Flow
Local Files → Python Script → S3 Raw → Glue Crawler → Glue Catalog → Glue ETL → S3 Staging → S3 Curated → Athena

---

## ⚙️ Key Features
- Batch ingestion using Python
- Partitioning (year/month/day)
- Data cleaning (null & duplicate removal)
- Parquet format for better performance
- Aggregation for business use cases
- Logging using CloudWatch

---

## ▶️ Steps to Run

1. Place CSV files in local folders (customer/product)
2. Run Python ingestion script → uploads data to S3 raw
3. Run Glue crawler → creates table in catalog
4. Run Glue ETL job:
   - Reads latest partition
   - Cleans data
   - Writes to staging (Parquet)
   - Creates aggregated data in curated layer
5. Query data in Athena

---

## 📊 Sample Queries

### 🔹 Staging Layer (Partitioned)
```sql
SELECT *
FROM staging_customer
WHERE year = '2026' AND month = '03' AND day = '19';
