"""
PySpark utilities for common operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import logging

def create_spark_session(app_name: str, 
                        config: dict = None,
                        log_level: str = "WARN") -> SparkSession:
    """
    Create a Spark session with common configurations.
    
    Args:
        app_name: Name of the Spark application
        config: Additional Spark configurations
        log_level: Logging level (WARN, INFO, DEBUG, ERROR)
    
    Returns:
        SparkSession: Configured Spark session
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Default configurations
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true"
    }
    
    # Merge with user config
    if config:
        default_config.update(config)
    
    # Apply configurations
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    
    return spark

def print_schema_summary(df, table_name: str = "DataFrame"):
    """Print a formatted schema summary."""
    print(f"\n=== {table_name} Schema Summary ===")
    print(f"Rows: {df.count():,}")
    print(f"Columns: {len(df.columns)}")
    print("\nSchema:")
    df.printSchema()
