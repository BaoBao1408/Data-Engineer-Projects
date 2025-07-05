# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading & Writing & Creating Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Access**

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxidatalakeqb.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatalakeqb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatalakeqb.dfs.core.windows.net", "4f7ec787-b438-4ece-8a0a-db23c04d7622")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatalakeqb.dfs.core.windows.net", "C8Q8Q~w13Yq0awR7tItvT6xoPJ6wy3zHOduhldfy")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatalakeqb.dfs.core.windows.net", "https://login.microsoftonline.com/d16be8d3-0382-48a4-9d12-ad9a2d916c0f/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Database Creation**

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS gold CASCADE;
# MAGIC
# MAGIC CREATE DATABASE gold;

# COMMAND ----------

# MAGIC %md
# MAGIC **Storage Variable**

# COMMAND ----------

silver = "abfss://silver@nyctaxidatalakeqb.dfs.core.windows.net"
gold = 'abfss://gold@nyctaxidatalakeqb.dfs.core.windows.net/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Zone**

# COMMAND ----------

df_zone = spark.read.format("parquet")\
                    .option("inferSchema", "true")\
                    .option("header", "true")\
                    .load(f"{silver}/trip_zone")
                    # .load("abfss://silver@nyctaxidatalakeqb.dfs.core.windows.net")

# COMMAND ----------

df_zone.display()  # Check if df_zone is correctly loaded and path is correct

# COMMAND ----------

df_zone.write.format('delta')\
        .mode('append')\
        .option('path',f'{gold}/trip_zone')\
        .saveAsTable('gold.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone
# MAGIC WHERE Borough = 'EWR'

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

df_type = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format('delta')\
        .mode('append')\
        .option('path',f'{gold}/trip_type')\
        .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trips2023data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta')\
        .mode('append')\
        .option('path',f'{gold}/tripsdata')\
        .saveAsTable('gold.trip_trip')

# COMMAND ----------

# MAGIC %md
# MAGIC # About Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_catalog();
# MAGIC
# MAGIC -- SELECT current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC update gold.trip_zone
# MAGIC set Borough = 'EMR' 
# MAGIC where locationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone
# MAGIC where Borough = 'EMR'
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql DELETE FROM gold.trip_zone WHERE locationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone where LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE gold.trip_zone TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone where locationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data 2023**

# COMMAND ----------

# MAGIC %sql select * from gold.trip_trip

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL gold.trip_trip;