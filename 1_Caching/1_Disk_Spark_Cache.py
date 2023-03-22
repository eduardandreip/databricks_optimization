# Databricks notebook source
# MAGIC %md ###1. Create Dataset

# COMMAND ----------

# MAGIC %python
# MAGIC blob_account_name = "azureopendatastorage"
# MAGIC blob_container_name = "nyctlc"
# MAGIC blob_relative_path = "yellow"
# MAGIC blob_sas_token = "r"
# MAGIC 
# MAGIC wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
# MAGIC spark.conf.set(
# MAGIC  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
# MAGIC  blob_sas_token)
# MAGIC print('Remote blob path: ' + wasbs_path)
# MAGIC 
# MAGIC df = spark.read.parquet(wasbs_path)
# MAGIC print('Register the DataFrame as a SQL temporary view: source')
# MAGIC df.createOrReplaceTempView('source')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE optimization.data USING DELTA AS
# MAGIC SELECT
# MAGIC   sha1(
# MAGIC     CONCAT(
# MAGIC       vendorID,
# MAGIC       tpepPickupDateTime,
# MAGIC       tpepDropoffDateTime,
# MAGIC       puLocationId,
# MAGIC       doLocationId
# MAGIC     )
# MAGIC   ) AS Id,
# MAGIC   source.*,
# MAGIC   CAST(
# MAGIC     date_format(tpepPickupDateTime, 'yyyyMMdd') AS INT
# MAGIC   ) AS puDate
# MAGIC FROM
# MAGIC   source
# MAGIC WHERE
# MAGIC   1 = 1
# MAGIC   AND puYear > 2016
# MAGIC   AND puYear < 2019

# COMMAND ----------

# MAGIC %md ###2. Delta Cache and Spark Cache

# COMMAND ----------

# MAGIC %md ####2.1 Explicit Disk Cache

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW ssdf_specific_trip_type AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     optimization.data
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND vendorID = 1
# MAGIC     AND passengerCount = 4
# MAGIC     AND tripDistance = 3.1
# MAGIC     AND doLocationId = 148
# MAGIC     AND fareAmount = 13
# MAGIC     AND extra = 0.5
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN optimization

# COMMAND ----------

# MAGIC %sql SELECT * FROM ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %md ###2.2 Automatic Disk Cache

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql SELECT * FROM ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %sql SELECT * FROM ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %python
# MAGIC print(spark.conf.get("spark.databricks.io.cache.maxDiskUsage"))
# MAGIC print(spark.conf.get("spark.databricks.io.cache.maxMetaDataCache"))
# MAGIC print(spark.conf.get("spark.databricks.io.cache.compression.enabled"))

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
# MAGIC spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")
# MAGIC spark.conf.set("spark.databricks.io.cache.compression.enabled", "true")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %python
# MAGIC print(spark.conf.get("spark.databricks.io.cache.maxDiskUsage"))
# MAGIC print(spark.conf.get("spark.databricks.io.cache.maxMetaDataCache"))
# MAGIC print(spark.conf.get("spark.databricks.io.cache.compression.enabled"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW ssdf_specific_trip_type AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     optimization.data
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND vendorID = 1
# MAGIC     AND passengerCount = 4
# MAGIC     AND tripDistance = 3.1
# MAGIC     AND doLocationId = 148
# MAGIC     AND fareAmount = 13
# MAGIC     AND extra = 0.5
# MAGIC )

# COMMAND ----------

# MAGIC %sql SELECT * FROM ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %sql SELECT * FROM ssdf_specific_trip_type

# COMMAND ----------

# MAGIC %md ####2.2 Apache Spark Cache

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_specific_trip_type = spark.sql("""
# MAGIC SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     optimization.data
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND vendorID = 1
# MAGIC     AND passengerCount = 4
# MAGIC     AND tripDistance = 3.1
# MAGIC     AND doLocationId = 148
# MAGIC     AND fareAmount = 13
# MAGIC     AND extra = 0.5
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_specific_trip_type.display()

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_specific_trip_type.cache().count()

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_specific_trip_type.display()
