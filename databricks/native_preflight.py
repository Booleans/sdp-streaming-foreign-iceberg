# Databricks notebook source
# Configure the source before executing this notebook; do not embed account details in Git.
dbutils.widgets.text('source_table', '')
source = dbutils.widgets.get('source_table')
import re
if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*', source):
    raise ValueError('Set source_table to a three-part Unity Catalog identifier')

# COMMAND ----------
# Prove access and source classification before attributing any streaming failure to support.
spark.sql(f'DESCRIBE TABLE EXTENDED {source}').show(100, truncate=False)
print('Native Spark version:', spark.version)
print('Source row count:', spark.table(source).count())

# COMMAND ----------
# Optional minimal streaming-plan probe. The actual SDP test remains the separate pipeline file.
stream = spark.readStream.table(source)
print('Streaming DataFrame:', stream.isStreaming)
stream.explain()
