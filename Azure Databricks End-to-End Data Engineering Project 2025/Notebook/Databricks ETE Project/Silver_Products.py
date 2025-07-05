# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
                .load("abfss://bronze@databrickseteqb.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC RETURN p_price * 0.90;

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, databricks_cata.bronze.discount_func(price) as discount_price
# MAGIC from products

# COMMAND ----------

df = df.withColumn("discount_price", expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN upper(p_brand);

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, databricks_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://silver@databrickseteqb.dfs.core.windows.net/products")\
        .save()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Create table**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.product_silver
# MAGIC using delta
# MAGIC location "abfss://silver@databrickseteqb.dfs.core.windows.net/products"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.product_silver