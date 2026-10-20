import os
import sys
import pathlib
import psycopg
from pyspark.sql import SparkSession, functions as F

month = sys.argv[1]
path = pathlib.Path("data/raw") / f"yellow_tripdata_{month}.parquet"

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/nyc_taxi?reWriteBatchedInserts=true"
CONN = f"host={PG_HOST} port={PG_PORT} dbname=nyc_taxi user=admin password=admin"
JAR = os.getenv("PG_JDBC_JAR", "/home/airflow/jars/postgresql-42.7.3.jar")

spark = (
    SparkSession.builder.appName("load_raw_yellow")
    .master("local[2]")
    .config("spark.jars", JAR)
    .config("spark.driver.memory", "2g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# 1. Doc file Parquet
df = spark.read.parquet(str(path))
df = df.toDF(*[c.lower() for c in df.columns])
df = df.withColumn("source_month", F.lit(month))
rows_in_file = df.count()
print(f"File co {rows_in_file:,} dong")

# 2. Xoa du lieu cu cua thang nay (de chay lai khong bi trung)
with psycopg.connect(CONN) as conn:
    exists = conn.execute("SELECT to_regclass('raw.yellow_trips')").fetchone()[0]
    if exists:
        conn.execute("DELETE FROM raw.yellow_trips WHERE source_month = %s", (month,))

# 3. Ghi vao Postgres qua JDBC
(
    df.write.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "raw.yellow_trips")
    .option("user", "admin")
    .option("password", "admin")
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", 20000)
    .mode("append")
    .save()
)

# 4. Doi soat so dong
with psycopg.connect(CONN) as conn:
    loaded = conn.execute(
        "SELECT count(*) FROM raw.yellow_trips WHERE source_month = %s", (month,)
    ).fetchone()[0]

spark.stop()

if loaded != rows_in_file:
    sys.exit(f"LOI: file {rows_in_file:,} dong nhung bang co {loaded:,} dong")
print(f"Xong: nap {loaded:,} dong, khop voi file")