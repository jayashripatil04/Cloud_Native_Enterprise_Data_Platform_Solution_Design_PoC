import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, concat_ws, max, avg, count
# import logging

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

print("****************************************")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database',
    'table_name',
    'target_path'
])

database = args['database']
table_name = args['table_name']
target_path = args['target_path']
output_path = f"s3://etl-data-platform-poc/{target_path}/{table_name}/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"database: {database}")
print(f"Starting Glue Job for table: {table_name}")

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=table_name
)
    
df = datasource.toDF()

df = df.withColumn(
    "full_date",
    to_date(concat_ws("-", "partition_0", "partition_1", "partition_2"))
)
# df.show()
latest_date = df.select(max("full_date")).collect()[0][0]

print(f"Latest partition detected: {latest_date}")

df = df.filter(col("full_date") == latest_date)

print(f"Source count (latest partition only): {df.count()}")

df = df.drop("full_date")

df_clean = df.dropna()

df_clean = df_clean.dropDuplicates()
print(f"records count after cleaning: {df_clean.count()}")

df_clean = df_clean \
    .withColumn("year", df["partition_0"]) \
    .withColumn("month", df["partition_1"]) \
    .withColumn("day", df["partition_2"]) \
    .drop("partition_0", "partition_1", "partition_2")
    
print("Partition columns created (year, month, day)")

final_dyf = DynamicFrame.fromDF(df_clean, glueContext, "final_dyf")

print(f"Writing data to: {output_path}")

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["year", "month", "day"],
        "mode": "overwrite"
    },
    format="parquet"
)

print("Data successfully written to S3")

staging_count = spark.read.parquet(output_path).count()
print(f"Staging count: {staging_count}")

print("Starting curated layer transformation...")

df_curated = df_clean.groupBy("city").agg(
    avg("age").alias("avg_age"),
    count("*").alias("customer_count")
)

print(f"Curated record count: {df_curated.count()}")

curated_dyf = DynamicFrame.fromDF(df_curated, glueContext, "curated_dyf")

curated_path = f"s3://etl-data-platform-poc/curated/{table_name}/"

print(f"Writing curated data to: {curated_path}")

glueContext.write_dynamic_frame.from_options(
    frame=curated_dyf,
    connection_type="s3",
    connection_options={
        "path": curated_path,
        "mode": "overwrite"
    },
    format="parquet"
)

print("Curated data successfully written to S3")

job.commit()

print("Glue Job completed successfully")