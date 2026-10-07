"""Native Databricks SDP test; configure spark.demo.source_table to a UC foreign Iceberg table."""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()


@dp.table(name='native_foreign_iceberg_copy')
def native_foreign_iceberg_copy():
    # Use UC name resolution. Do not add OSS Iceberg JARs to this native test.
    # Leave failures uncaught so the pipeline event log preserves the actual exception.
    return spark.readStream.table(spark.conf.get('spark.demo.source_table'))
