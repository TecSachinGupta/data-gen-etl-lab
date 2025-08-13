# Databricks notebook source
# MAGIC %run ./configs

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./schemas

# COMMAND ----------

ENABLE_SPARK_DCONNECT_ADLS = False
MOUNT_CONTAINERS = False

# COMMAND ----------

storage_account_name = "alldatastore"
containers = ["input", "output", "warehouse"]

# COMMAND ----------

ADLS_MOUNT_CONFIGS, ADLS_SPARK_CONFIGS = get_adls_configs("azure-managed-secrets")

# COMMAND ----------

if ENABLE_SPARK_DCONNECT_ADLS:
    for key in ADLS_SPARK_CONFIGS:
        spark.conf.set(key, ADLS_SPARK_CONFIGS[key])

# COMMAND ----------

if MOUNT_CONTAINERS == True and len(containers) != 0:
    for container in containers:
        mount_adls_container(container, storage_account_name, ADLS_MOUNT_CONFIGS)

# COMMAND ----------

display(dbutils.fs.mounts())
display(dbutils.fs.ls("/mnt/alldatastore/input"))
display(dbutils.fs.ls("/mnt/alldatastore/output"))
display(dbutils.fs.ls("/mnt/alldatastore/warehouse"))
display(dbutils.fs.ls("abfss://warehouse@alldatastore.dfs.core.windows.net/"))

# COMMAND ----------

spark.sql("""
            CREATE DATABASE IF NOT EXISTS practice_db
            COMMENT "Schema for storing table while practicing or learning."
            LOCATION "/mnt/alldatastore/warehouse/practice_db.db/"
          """)

# COMMAND ----------

spark.sql("USE practice_db")

# COMMAND ----------

csv_file_to_table("/mnt/alldatastore/input/sales_data/sales.csv", "sales", sales_schema)

# COMMAND ----------

csv_file_to_table("/mnt/alldatastore/input/sales_data/products.csv", "products", product_schema)

# COMMAND ----------

csv_file_to_table("/mnt/alldatastore/input/sales_data/sellers.csv", "sellers", sellers_schema)

# COMMAND ----------


