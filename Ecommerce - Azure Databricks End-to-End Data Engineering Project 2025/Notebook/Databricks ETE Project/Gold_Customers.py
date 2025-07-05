# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading From Source Catalog**

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.customer_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing Duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=["customer_id"])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dividing Old With New Records**

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date 
                       from databricks_cata.gold.DimCustomers''')
else :
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date from 
                       databricks_cata.silver.customer_silver where 1 = 0''')
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming Columns Of df_old**

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
                .withColumnRenamed("customer_id", "old_customer_id")\
                .withColumnRenamed("create_date", "old_create_date")\
                .withColumnRenamed("update_date", "old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Join With Old Records**

# COMMAND ----------

df_join = df.join(df_old, df["customer_id"] == df_old["old_customer_id"], "left")
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Seperating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join["old_DimCustomerKey"].isNull())



# COMMAND ----------

df_old = df_join.filter(df_join["old_DimCustomerKey"].isNotNull())


# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

# drop all columns are not required

df_old = df_old.drop("old_customer_id", "old_update_date")

# renaming old_DimCustomerKey to DimCustomerKey

df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# renaming old_create_date column to create day and set to timestamp

df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# recreating "update_date" column with current timestamp

df_old = df_old.withColumn("update_date", current_timestamp())

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_new**

# COMMAND ----------

df_new.display()

# COMMAND ----------

# drop all columns which are not required

df_new = df_new.drop("old_DimCustomerKey","old_customer_id", "old_update_date", "old_create_date")

# recreating "update_date" column with current timestamp

df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Key - From 1**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))
df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding Max Surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.DimCustomers")
    
    # Convert df_maxsur to max_surrogate_key Variables
    max_surrogate_key = df_maxsur.collect()[0]["max_surrogate_key"]

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key) + col("DimCustomerKey"))
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Union All df_old and df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SCD Type - 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.DimCustomers"):
    dbt.obj = DeltaTable.forPath(path, "abfss://gold@databrickseteqb.dfs.core.windows.net/DimCustomers")

    dbt.obj.alias("trg").merge(df_final.alias("src"), "src.DimCustomerKey = trg.DimCustomerKey")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .excute()   

else:
    df_final.write.mode("overwrite")\
                .format("delta")\
                .option("path","abfss://gold@databrickseteqb.dfs.core.windows.net/DimCustomers")\
                .saveAsTable("databricks_cata.gold.DimCustomers")    
    


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimcustomers