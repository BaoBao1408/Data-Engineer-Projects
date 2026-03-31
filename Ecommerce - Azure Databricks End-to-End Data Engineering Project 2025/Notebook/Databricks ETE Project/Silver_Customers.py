# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

df = spark.read.format("parquet")\
                .load("abfss://bronze@databrickseteqb.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df = df.withColumn("domains", split(col("email"), "@")[1])
df.display()

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers", ascending=False).display()

# COMMAND ----------

df_gmail = df.filter(col("domains") == "gmail.com")
df_gmail.display()
time.sleep(5)

df_yahoo = df.filter(col("domains") == "yahoo.com")
df_yahoo.display()
time.sleep(5)

df_hotmail = df.filter(col("domains") == "hotmail.com")
df_hotmail.display()
time.sleep(5)

# COMMAND ----------

df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")

df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@databrickseteqb.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Create Schema and Table**

# COMMAND ----------

# %sql
# create schema databricks_cata.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.customer_silver
# MAGIC using delta
# MAGIC location "abfss://silver@databrickseteqb.dfs.core.windows.net/customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  databricks_cata.silver.customer_silver