# Databricks notebook source
# MAGIC %md
# MAGIC ##  DATA ACCESS

# COMMAND ----------

# secret_key: "C8Q8Q~w13Yq0awR7tItvT6xoPJ6wy3zHOduhldfy"
# app_id: "4f7ec787-b438-4ece-8a0a-db23c04d7622"
# dir_id: "d16be8d3-0382-48a4-9d12-ad9a2d916c0f"

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.nyctaxidatalakeqb.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatalakeqb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatalakeqb.dfs.core.windows.net", "4f7ec787-b438-4ece-8a0a-db23c04d7622")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatalakeqb.dfs.core.windows.net", "C8Q8Q~w13Yq0awR7tItvT6xoPJ6wy3zHOduhldfy")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatalakeqb.dfs.core.windows.net", "https://login.microsoftonline.com/d16be8d3-0382-48a4-9d12-ad9a2d916c0f/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxidatalakeqb.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DATA READING**

# COMMAND ----------

# MAGIC %md
# MAGIC IMPORTING LIBRARIES

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CSV Data**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type Data**

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load("abfss://bronze@nyctaxidatalakeqb.dfs.core.windows.net/trip_type")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load("abfss://bronze@nyctaxidatalakeqb.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.head(5)

# COMMAND ----------

df_trip_zone.show()

# COMMAND ----------

df_trip_zone.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**
# MAGIC

# COMMAND ----------

# myschema = '''
#                 VendorID BIGINT,
#                 lpep_pickup_datetime TIMESTAMP,
#                 lpep_dropoff_datetime TIMESTAMP,
#                 store_and_fwd_flag STRING,
#                 RatecodeID BIGINT,
#                 PULocationID BIGINT,
#                 DOLocationID BIGINT,
#                 passenger_count BIGINT,
#                 trip_distance DOUBLE,
#                 fare_amount DOUBLE,
#                 extra DOUBLE,
#                 mta_tax DOUBLE,
#                 tip_amount DOUBLE,
#                 tolls_amount DOUBLE,
#                 ehail_fee DOUBLE,
#                 improvement_surcharge DOUBLE,
#                 total_amount DOUBLE,
#                 payment_type BIGINT,
#                 trip_type BIGINT,
#                 congestion_surcharge DOUBLE,

#       '''

# COMMAND ----------

myschema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("trip_type", LongType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# COMMAND ----------

df_trip = spark.read.format("parquet")\
                    .schema(myschema)\
                    .option("header", "true")\
                    .option("RecursiveFileLookup", "true")\
                    .load("abfss://bronze@nyctaxidatalakeqb.dfs.core.windows.net/trips2023data")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data Tranformation**

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip Type**

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed("description", "trip_description")
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format("Parquet")\
                .mode("append")\
                .option("path", "abfss://silver@nyctaxidatalakeqb.dfs.core.windows.net/trip_type")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

from pyspark.sql.functions import split, col

df_trip_zone = df_trip_zone.withColumn("Zone1", split(col("Zone"), "/")[0])\
                            .withColumn("Zone2", split(col("Zone"), "/")[1])
df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format("Parquet")\
                .mode("append")\
                .option("path", "abfss://silver@nyctaxidatalakeqb.dfs.core.windows.net/trip_zone")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn("trip_date",to_date("lpep_pickup_datetime"))\
                .withColumn("trip_year",year("lpep_pickup_datetime"))\
                .withColumn("trip_month",month("lpep_pickup_datetime"))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select("VendorID","PULocationID","DOLocationID","trip_distance", "fare_amount", "total_amount")
df_trip.display()

# COMMAND ----------

df_trip.write.format("Parquet")\
                .mode("append")\
                .option("path", "abfss://silver@nyctaxidatalakeqb.dfs.core.windows.net/trips2023data")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Analysis**

# COMMAND ----------

display(df_trip)