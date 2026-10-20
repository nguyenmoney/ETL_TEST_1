from pyspark.sql import SparkSession

JARS = ",".join([
    "/home/airflow/jars/postgresql-42.7.3.jar",
    "/home/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
])

spark = (
    SparkSession.builder.appName("iceberg_hello")
    .master("local[2]")
    .config("spark.jars", JARS)
    .config("spark.driver.memory", "2g")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "/opt/airflow/warehouse")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.demo")
spark.sql("CREATE TABLE IF NOT EXISTS lake.demo.hello (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO lake.demo.hello VALUES (1, 'a'), (2, 'b')")

spark.sql("SELECT * FROM lake.demo.hello").show()
spark.sql(
    "SELECT snapshot_id, operation, committed_at FROM lake.demo.hello.snapshots"
).show(truncate=False)

spark.stop()