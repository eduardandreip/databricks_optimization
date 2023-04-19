# Databricks notebook source
# MAGIC %md ##1. Create Tables

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
# MAGIC CREATE SCHEMA IF NOT EXISTS optimization

# COMMAND ----------

# MAGIC %md ###1.1 Unpartitioned Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE optimization.data_unpartitioned USING DELTA AS
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

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_unpartitioned = spark.sql("""SELECT * FROM optimization.data_unpartitioned""")
# MAGIC spdf_data_unpartitioned.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/optimization.db/data_unpartitioned")

# COMMAND ----------

# MAGIC %md ###1.2 Partitioned Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE optimization.data_partitioned_year_month USING DELTA PARTITIONED BY (puYearMonth) AS
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

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/optimization.db/data_partitioned_year_month/puYearMonth=201701/")

# COMMAND ----------

# MAGIC %md ###1.3 Partitioned + Optimized Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE optimization.data_partitioned_year_month_optimized USING DELTA 
# MAGIC PARTITIONED BY (puYearMonth) 
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

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE optimization.data_partitioned_year_month_optimized

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/optimization.db/data_partitioned_year_month_optimized/puYearMonth=201701/")

# COMMAND ----------

# MAGIC %md ###1.4 Partitioned + Adapted Target File Size + Optimized Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE optimization.data_partitioned_year_month_target_file_size_optimized
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (puYearMonth) 
# MAGIC OPTIONS (delta.targetFileSize='256M')
# MAGIC AS (
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
# MAGIC   source)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE optimization.data_partitioned_year_month_target_file_size_optimized

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/optimization.db/data_partitioned_year_month_target_file_size_optimized/puYearMonth=201803/")

# COMMAND ----------

# MAGIC %md ##2. SELECT Statements

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_unpartitioned = spark.sql("""
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   optimization.data_unpartitioned
# MAGIC WHERE
# MAGIC   (puYearMonth = 201806 OR puYearMonth = 201805)
# MAGIC   AND totalAmount > 6
# MAGIC   AND vendorID = 1
# MAGIC   AND tipAmount > 2
# MAGIC   AND tripDistance > 2""")

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_unpartitioned.display()

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_partitioned_year_month = spark.sql("""
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   optimization.data_partitioned_year_month
# MAGIC WHERE
# MAGIC   (puYearMonth = 201806 OR puYearMonth = 201805)
# MAGIC   AND vendorID = 1
# MAGIC   AND totalAmount > 6
# MAGIC   AND tipAmount > 2
# MAGIC   AND tripDistance > 2""")

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_partitioned_year_month.display()

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_partitioned_year_month_target_file_size_optimized = spark.sql("""
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   optimization.data_partitioned_year_month_target_file_size_optimized
# MAGIC WHERE
# MAGIC   (puYearMonth = 201806 OR puYearMonth = 201805)
# MAGIC   AND totalAmount > 6
# MAGIC   AND vendorID = 1
# MAGIC   AND tipAmount > 2
# MAGIC   AND tripDistance > 2""")

# COMMAND ----------

# MAGIC %python
# MAGIC spdf_data_partitioned_year_month_target_file_size_optimized.display()

# COMMAND ----------

# MAGIC %md ##3. MERGE Statements

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE optimization.data_subset_unpartitioned USING DELTA AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   optimization.data_unpartitioned
# MAGIC WHERE
# MAGIC   (puYearMonth = 201806 OR puYearMonth = 201805)
# MAGIC   AND totalAmount > 6
# MAGIC   AND vendorID = 1
# MAGIC   AND tipAmount > 2
# MAGIC   AND tripDistance > 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM
# MAGIC   optimization.data_unpartitioned
# MAGIC WHERE
# MAGIC   EXISTS (
# MAGIC     SELECT
# MAGIC       1
# MAGIC     FROM
# MAGIC       optimization.data_subset_unpartitioned
# MAGIC     WHERE
# MAGIC       optimization.data_unpartitioned.Id = optimization.data_subset_unpartitioned.Id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM
# MAGIC   optimization.data_partitioned_year_month
# MAGIC WHERE
# MAGIC   EXISTS (
# MAGIC     SELECT
# MAGIC       1
# MAGIC     FROM
# MAGIC       optimization.data_subset_unpartitioned
# MAGIC     WHERE
# MAGIC       optimization.data_partitioned_year_month.Id = optimization.data_subset_unpartitioned.Id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM
# MAGIC   optimization.data_partitioned_year_month_target_file_size_optimized
# MAGIC WHERE
# MAGIC   EXISTS (
# MAGIC     SELECT
# MAGIC       1
# MAGIC     FROM
# MAGIC       optimization.data_subset_unpartitioned
# MAGIC     WHERE
# MAGIC       optimization.data_partitioned_year_month_target_file_size_optimized.Id = optimization.data_subset_unpartitioned.Id
# MAGIC   )

# COMMAND ----------

# MAGIC %python
# MAGIC min_puYearMonth = spark.sql("""
# MAGIC SELECT
# MAGIC     min(puYearMonth) AS min_puYearMonth
# MAGIC   FROM
# MAGIC     optimization.data_subset_unpartitioned""").collect()[0][0]
# MAGIC     
# MAGIC max_puYearMonth = spark.sql("""
# MAGIC SELECT
# MAGIC     max(puYearMonth) AS min_puYearMonth
# MAGIC   FROM
# MAGIC     optimization.data_subset_unpartitioned""").collect()[0][0]

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(
# MAGIC   f"""
# MAGIC MERGE INTO optimization.data_unpartitioned target USING (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     optimization.data_subset_unpartitioned
# MAGIC ) AS source ON target.id = source.id
# MAGIC AND target.puYearMonth >= {min_puYearMonth}
# MAGIC AND target.puYearMonth <= {max_puYearMonth}
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *"""
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(
# MAGIC   f"""
# MAGIC MERGE INTO optimization.data_partitioned_year_month target USING (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     optimization.data_subset_unpartitioned
# MAGIC ) AS source ON target.id = source.id
# MAGIC AND target.puYearMonth >= {min_puYearMonth}
# MAGIC AND target.puYearMonth <= {max_puYearMonth}
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *"""
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(
# MAGIC   f"""
# MAGIC MERGE INTO optimization.data_partitioned_year_month_target_file_size_optimized target USING (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     optimization.data_subset_unpartitioned
# MAGIC ) AS source ON target.id = source.id
# MAGIC AND target.puYearMonth >= {min_puYearMonth}
# MAGIC AND target.puYearMonth <= {max_puYearMonth}
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *"""
# MAGIC )

# COMMAND ----------

# MAGIC %md ##4. GROUP BYs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   puYearMonth,
# MAGIC   max(tipAmount)
# MAGIC FROM
# MAGIC   optimization.data_unpartitioned
# MAGIC GROUP BY
# MAGIC   puYearMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   puYearMonth,
# MAGIC   max(tipAmount)
# MAGIC FROM
# MAGIC   optimization.data_partitioned_year_month
# MAGIC GROUP BY
# MAGIC   puYearMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   puYearMonth,
# MAGIC   max(tipAmount)
# MAGIC FROM
# MAGIC   optimization.data_partitioned_year_month_target_file_size_optimized
# MAGIC GROUP BY
# MAGIC   puYearMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   puYearMonth,
# MAGIC   max(tipAmount)
# MAGIC FROM
# MAGIC   optimization.data_unpartitioned
# MAGIC WHERE
# MAGIC   puYearMonth = 201805
# MAGIC   OR puYearMonth = 201806
# MAGIC GROUP BY
# MAGIC   puYearMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   puYearMonth,
# MAGIC   max(tipAmount)
# MAGIC FROM
# MAGIC   optimization.data_partitioned_year_month
# MAGIC WHERE
# MAGIC   puYearMonth = 201805
# MAGIC   OR puYearMonth = 201806
# MAGIC GROUP BY
# MAGIC   puYearMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   puYearMonth,
# MAGIC   max(tipAmount)
# MAGIC FROM
# MAGIC   optimization.data_partitioned_year_month_target_file_size_optimized
# MAGIC WHERE
# MAGIC   puYearMonth = 201805
# MAGIC   OR puYearMonth = 201806
# MAGIC GROUP BY
# MAGIC   puYearMonth
