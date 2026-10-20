import sys
from pyspark.sql import SparkSession

JAR = "/home/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.jars", JAR)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "/opt/airflow/warehouse")
    .getOrCreate()
)
spark.sql(sys.argv[1]).show(truncate=False)
spark.stop()