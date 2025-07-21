# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# COMMAND ----------

def get_pyspark_compatible_dict(data):
    new_data = []
    keys = list(data.keys())
    for i in range(len(data[keys[0]])):
        ndict = {}
        for key in keys:
            ndict[key] = data[key][i]
        new_data.append(ndict)
    return new_data

# COMMAND ----------

def groupby_apply_describe(df, groupby_col, stat_col):
    """From a grouby df object provide the stats
    of describe for each key in the groupby object.

    Parameters
    ----------
    df : spark dataframe groupby object
    col : column to compute statistics on
    
    """
    output = df.groupby(groupby_col).agg(
        F.count(stat_col).alias("count"),
        F.mean(stat_col).alias("mean"),
        F.stddev(stat_col).alias("std"),
        F.min(stat_col).alias("min"),
        F.expr(f"percentile({stat_col}, array(0.25))")[0].alias("%25"),
        F.expr(f"percentile({stat_col}, array(0.5))")[0].alias("%50"),
        F.expr(f"percentile({stat_col}, array(0.75))")[0].alias("%75"),
        F.max(stat_col).alias("max"),
    )
    return output.orderBy(groupby_col)

# COMMAND ----------

def mount_adls_container(container_name, storage_account_name, adls_configs):
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = adls_configs)

# COMMAND ----------

def read_tsv_file(file_path):
    return spark.read \
                .option("header", True) \
                .option("sep", "\t") \
                .option("multiLine", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("ignoreTrailingWhiteSpace", True) \
                .option("compression", "gzip") \
                .option("numPartitions", 10) \
                .csv(file_path)

# COMMAND ----------

def csv_file_to_table(file_path, table_name, schema_str = None, has_headers = True):
    df: DataFrame = None
    if  schema_str is not None:
        df = spark.read \
                .option("header", True) \
                .option("multiLine", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("ignoreTrailingWhiteSpace", True) \
                .option("numPartitions", 10) \
                .csv(file_path, schema = schema_str)
    else:
        df = spark.read \
                .option("header", True) \
                .option("multiLine", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("ignoreTrailingWhiteSpace", True) \
                .option("numPartitions", 10) \
                .csv(file_path)
    df.write.format('delta').saveAsTable(table_name)
