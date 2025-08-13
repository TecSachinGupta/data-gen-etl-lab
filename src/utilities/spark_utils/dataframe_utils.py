"""DataFrame utility functions"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when


def show_df_info(df: DataFrame, name: str = "DataFrame") -> None:
    """
    Display comprehensive DataFrame information.
    
    Args:
        df: Spark DataFrame
        name: Name for display
    """
    print(f"\n=== {name} Info ===")
    print(f"Rows: {df.count():,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    
    print("\nSchema:")
    df.printSchema()
    
    print("\nSample Data:")
    df.show(5, truncate=False)
    
    print("\nNull Counts:")
    null_counts = df.select([
        count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) 
        for c in df.columns
    ])
    null_counts.show()


def cache_df(df: DataFrame, storage_level: str = "MEMORY_AND_DISK") -> DataFrame:
    """
    Cache DataFrame with specified storage level.
    
    Args:
        df: DataFrame to cache
        storage_level: Storage level for caching
        
    Returns:
        Cached DataFrame
    """
    from pyspark import StorageLevel
    
    storage_levels = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
    }
    
    level = storage_levels.get(storage_level, StorageLevel.MEMORY_AND_DISK)
    return df.persist(level)

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

# Example usage
if __name__ == "__main__":
    from ..session import create_spark_session
    
    spark = create_spark_session("DataFrame Utils Test")
    
    # Create sample data
    data = [("Alice", 25, "Engineer"), ("Bob", 30, "Manager")]
    columns = ["name", "age", "role"]
    df = spark.createDataFrame(data, columns)
    
    # Test utilities
    show_df_info(df, "Sample Data")
    cached_df = cache_df(df)
    
    spark.stop()
