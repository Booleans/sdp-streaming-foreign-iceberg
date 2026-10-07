"""Portable SDP streaming definition: source name is configured per environment."""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()


@dp.table(name='iceberg_stream_copy')
def iceberg_stream_copy():
    return spark.readStream.format('iceberg').load(spark.conf.get('spark.demo.source_table'))
