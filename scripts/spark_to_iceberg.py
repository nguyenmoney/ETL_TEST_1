import os
import sys
from pyspark.sql import SparkSession, functions as F

month = sys.argv[1]

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/nyc_taxi"

JARS = ",".join([
    "/home/airflow/jars/postgresql-42.7.3.jar",
    "/home/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
])

spark = (
    SparkSession.builder.appName("pg_to_iceberg_bronze")
    .master("local[2]")
    .config("spark.jars", JARS)
    .config("spark.driver.memory", "2g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "/opt/airflow/warehouse")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# 1. Doc tu Postgres (chi lay thang can nap)
src = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("user", "admin")
    .option("password", "admin")
    .option("driver", "org.postgresql.Driver")
    .option("query", f"SELECT * FROM raw.yellow_trips WHERE source_month = '{month}'")
    .option("fetchsize", 50000)
    .load()
)

df = (
    src.withColumn("_source_table", F.lit("raw.yellow_trips"))
       .withColumn("_ingested_at", F.current_timestamp())
)
rows_src = df.count()
print(f"Nguon (Postgres) co {rows_src:,} dong cho thang {month}")

# 2. Tao bang Iceberg neu chua co (phan vung theo source_month)
if not spark.catalog.tableExists("lake.bronze.yellow_trips"):
    (
        df.limit(0)
        .writeTo("lake.bronze.yellow_trips")
        .using("iceberg")
        .partitionedBy(F.col("source_month"))
        .create()
    )

# 3. Ghi de dung phan vung cua thang nay (idempotent)
df.writeTo("lake.bronze.yellow_trips").overwritePartitions()

# 4. Doi soat so dong
rows_lake = spark.sql(
    f"SELECT count(*) FROM lake.bronze.yellow_trips WHERE source_month = '{month}'"
).collect()[0][0]
spark.stop()

if rows_lake != rows_src:
    sys.exit(f"LOI: nguon {rows_src:,} dong nhung lakehouse co {rows_lake:,} dong")
print(f"Xong: lakehouse co {rows_lake:,} dong cho thang {month}, khop voi nguon")