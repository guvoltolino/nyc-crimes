# Databricks notebook source
import os
import json
from pyspark.sql.functions import to_date, to_timestamp, when, lit, col

# COMMAND ----------

datalake_name = dbutils.secrets.get(scope="nyc-crimes-project", key="storage-account-name")
datalake_key = dbutils.secrets.get(scope="nyc-crimes-project", key="storage-account-key")
container_name = dbutils.secrets.get(scope="nyc-crimes-project", key="container-name")

# COMMAND ----------

if not any(mount.mountPoint == "/mnt/prata" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{datalake_name}.blob.core.windows.net/prata",
        mount_point="/mnt/prata",
        extra_configs={
            f"fs.azure.account.key.{datalake_name}.blob.core.windows.net": datalake_key
        }
    )
else:
    print("A montagem '/mnt/prata' já existe.")

# COMMAND ----------

bronze_path = "/mnt/bronze/"

df = spark.read.json(bronze_path)

# COMMAND ----------

df_clean = df.select(
    "latitude", 
    "longitude", 
    "ofns_desc", 
    "cmplnt_fr_dt", 
    "cmplnt_fr_tm", 
    "cmplnt_to_dt", 
    "cmplnt_to_tm", 
    "boro_nm", 
    "law_cat_cd", 
    "juris_desc", 
    "patrol_boro", 
    "cmplnt_num", 
    "vic_age_group", 
    "vic_race", 
    "vic_sex", 
    "susp_age_group", 
    "susp_race", 
    "susp_sex", 
    "crm_atpt_cptd_cd", 
    "prem_typ_desc", 
    "rpt_dt"
)

df_clean = df_clean \
    .withColumnRenamed("latitude", "latitude") \
    .withColumnRenamed("longitude", "longitude") \
    .withColumnRenamed("ofns_desc", "offense_description") \
    .withColumnRenamed("cmplnt_fr_dt", "start_date") \
    .withColumnRenamed("cmplnt_fr_tm", "start_time") \
    .withColumnRenamed("cmplnt_to_dt", "end_date") \
    .withColumnRenamed("cmplnt_to_tm", "end_time") \
    .withColumnRenamed("boro_nm", "borough_name") \
    .withColumnRenamed("law_cat_cd", "legal_category") \
    .withColumnRenamed("juris_desc", "jurisdiction_description") \
    .withColumnRenamed("patrol_boro", "patrol_borough") \
    .withColumnRenamed("cmplnt_num", "complaint_id") \
    .withColumnRenamed("vic_age_group", "victim_age_group") \
    .withColumnRenamed("vic_race", "victim_race") \
    .withColumnRenamed("vic_sex", "victim_sex") \
    .withColumnRenamed("susp_age_group", "suspect_age_group") \
    .withColumnRenamed("susp_race", "suspect_race") \
    .withColumnRenamed("susp_sex", "suspect_sex") \
    .withColumnRenamed("crm_atpt_cptd_cd", "crime_status") \
    .withColumnRenamed("prem_typ_desc", "premises_type_description") \
    .withColumnRenamed("rpt_dt", "report_date").distinct()

# COMMAND ----------

df_clean.display()

# COMMAND ----------

df_clean = df_clean.withColumn("start_date", to_date(col("start_date").substr(1, 10), "yyyy-MM-dd")) \
                   .withColumn("end_date", to_date(col("end_date").substr(1, 10), "yyyy-MM-dd")) \
                   .withColumn("report_date", to_date(col("report_date").substr(1, 10), "yyyy-MM-dd")) \
                   .withColumn("start_date", when(col("start_date").isNull(), lit("UNKNOWN")).otherwise(col("start_date"))) \
                   .withColumn("end_date", when(col("end_date").isNull(), lit("UNKNOWN")).otherwise(col("end_date"))) \
                   .withColumn("report_date", when(col("report_date").isNull(), lit("UNKNOWN")).otherwise(col("report_date")))


# COMMAND ----------

df_clean = df_clean.filter(col("start_date") >= "2019-01-01")

# COMMAND ----------

df_clean = df_clean.withColumn(
    "end_time", 
    when(col("end_time") == "(null)", lit("UNKNOWN")).otherwise(col("end_time"))
)

# COMMAND ----------

df_clean = df_clean.withColumn(
    "suspect_sex", 
    when(col("suspect_sex").isin("M", "F"), col("suspect_sex"))
    .otherwise(lit("U"))
).withColumn(
    "victim_sex", 
    when(col("victim_sex").isin("M", "F"), col("victim_sex"))
    .otherwise(lit("U"))
)

# COMMAND ----------

df_clean = df_clean \
    .withColumn(
        "suspect_age_group",
        when(col("suspect_age_group").isin("18-24", "25-44", "45-64", "65+", "<18"), col("suspect_age_group"))
        .otherwise(lit("UNKNOWN"))
    ) \
    .withColumn(
        "victim_age_group",
        when(col("victim_age_group").isin("18-24", "25-44", "45-64", "65+", "<18"), col("victim_age_group"))
        .otherwise(lit("UNKNOWN"))
    )

# COMMAND ----------

df_clean = df_clean \
    .withColumn(
        "suspect_race",
        when(col("suspect_race") == "(null)", lit("UNKNOWN")).otherwise(col("suspect_race"))
    ) \
    .withColumn(
        "premises_type_description",
        when(col("premises_type_description") == "(null)", lit("UNKNOWN")).otherwise(col("premises_type_description"))
    ) \
    .withColumn(
        "borough_name",
        when(col("borough_name") == "(null)", lit("UNKNOWN")).otherwise(col("borough_name"))
    )

# COMMAND ----------

df = df_clean.select(
    "complaint_id",
    "offense_description",        
    "crime_status",              
    "premises_type_description",   
    "victim_age_group",          
    "victim_race",               
    "victim_sex",                 
    "suspect_age_group",         
    "suspect_race",              
    "suspect_sex",               
    "start_date",                
    "start_time",               
    "end_date",                   
    "end_time",                  
    "borough_name",              
    "legal_category",             
    "jurisdiction_description",   
    "patrol_borough",             
    "latitude",                   
    "longitude",                  
    "report_date"                 
)

df.display()


# COMMAND ----------

path = "/mnt/prata/"

df.write.mode("overwrite").parquet(path)
