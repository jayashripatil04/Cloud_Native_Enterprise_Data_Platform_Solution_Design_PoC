Project: ETL Pipeline

Overview:
This project demonstrates an end-to-end ETL pipeline using AWS services.

Components:
- S3 for data lake
- Glue for ETL
- Athena for querying

Pipeline Flow:
Local → S3 Raw → Glue Crawler → Glue ETL → Staging → Curated

Key Features:
- Partition-based processing
- Incremental logic (latest partition)
- Data quality checks
- Aggregation layer

How to Run:
1. Upload data to S3
2. Run crawler
3. Execute Glue job
4. Query using Athena