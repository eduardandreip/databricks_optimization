# Databricks notebook source
# MAGIC %md ##Delta Table Functionality

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
# MAGIC CREATE SCHEMA tips

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE tips.delta_tables USING DELTA AS
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
# MAGIC   ) AS puDate,
# MAGIC   CAST(
# MAGIC     date_format(tpepPickupDateTime, 'yyyMM') AS INT
# MAGIC   ) AS puYearMonth
# MAGIC FROM
# MAGIC   source
# MAGIC WHERE
# MAGIC   1 = 1
# MAGIC   AND puYear = 2019

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Analyzing changes in data over time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tips.delta_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE
# MAGIC   tips.delta_tables
# MAGIC SET
# MAGIC   tripDistance = 1
# MAGIC WHERE
# MAGIC   vendorId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tips.delta_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only retrieve the records in the current version that received an update from version 0
# MAGIC SELECT
# MAGIC   v1.*
# MAGIC FROM
# MAGIC   tips.delta_tables VERSION AS OF 1 v1
# MAGIC   INNER JOIN tips.delta_tables VERSION AS OF 0 v0 ON v1.id = v0.id
# MAGIC   AND v1.tripDistance <> v0.tripDistance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reverting to previous versions of data

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE tips.delta_tables TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tips.delta_tables

# COMMAND ----------

# MAGIC %md ##3. Auditing data changes

# COMMAND ----------

# MAGIC %python
# MAGIC change_log_df = spark.sql("""SELECT h.version, h.timestamp, h.userId, h.userName, h.operation from (DESCRIBE HISTORY tips.delta_tables) AS h""")
# MAGIC change_log_df.display()
