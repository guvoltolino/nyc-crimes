# Databricks notebook source
import os
from pyspark.sql import functions as F

# COMMAND ----------

datalake_name = dbutils.secrets.get(scope="nyc-crimes-project", key="storage-account-name")
datalake_key = dbutils.secrets.get(scope="nyc-crimes-project", key="storage-account-key")
container_name = dbutils.secrets.get(scope="nyc-crimes-project", key="container-name")
api_calendar_key = dbutils.secrets.get(scope="nyc-crimes-project", key="api-calendar-key")

# COMMAND ----------

if not any(mount.mountPoint == "/mnt/ouro" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{datalake_name}.blob.core.windows.net/ouro",
        mount_point="/mnt/ouro",
        extra_configs={
            f"fs.azure.account.key.{datalake_name}.blob.core.windows.net": datalake_key
        }
    )
else:
    print("A montagem '/mnt/ouro' já existe.")

# COMMAND ----------

df = spark.read.parquet("/mnt/prata/")

# COMMAND ----------

incident_counts = df.groupBy("borough_name").count() \
    .withColumnRenamed("count", "previous_incidents")

df_enriched = df \
    .withColumn("day_of_week", F.date_format("start_date", "EEEE")) \
    .withColumn("month", F.month("start_date")) \
    .withColumn("year", F.year("start_date")) \
    .withColumn("time_of_day", 
                F.when(F.hour("start_time") < 12, "Morning")
                 .when((F.hour("start_time") >= 12) & (F.hour("start_time") < 18), "Afternoon")
                 .otherwise("Evening")) \
    .withColumn("suspect_age_group", F.when(F.col("suspect_age_group").isNull(), "UNKNOWN").otherwise(F.col("suspect_age_group"))) \
    .withColumn("premises_type_description", F.when(F.col("premises_type_description").isNull(), "UNKNOWN").otherwise(F.col("premises_type_description"))) \
    .join(incident_counts, on="borough_name", how="left")  # Unir com a contagem de incidentes

# COMMAND ----------

df_enriched.display()

# COMMAND ----------

path = "/mnt/ouro/"

df_enriched.write.mode("overwrite").parquet(path)
