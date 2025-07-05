# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickseteqb.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Read all Results**

# COMMAND ----------

str_table = input("input table (customers, products, regions, orders): ")
df = spark.read.format("delta")\
                .load(f"abfss://silver@databrickseteqb.dfs.core.windows.net/{str_table}")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Create Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.region_silver
# MAGIC using delta
# MAGIC location "abfss://silver@databrickseteqb.dfs.core.windows.net/regions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.region_silver