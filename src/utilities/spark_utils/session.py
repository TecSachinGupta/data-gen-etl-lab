"""Spark Session Management"""

import os
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create and configure Spark session.
    
    Args:
        app_name: Name of the Spark application
        master: Spark master URL
        config: Additional Spark configuration
    
    Returns:
        Configured SparkSession
    """
    # Default configuration
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }
    
    # Merge with user config
    if config:
        default_config.update(config)
    
    # Create Spark configuration
    conf = SparkConf()
    for key, value in default_config.items():
        conf.set(key, value)
    
    # Create session
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config(conf=conf)
        .getOrCreate()
    )
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop Spark session safely."""
    if spark:
        spark.stop()


# Example usage
if __name__ == "__main__":
    spark = create_spark_session("TestApp")
    print(f"Spark Version: {spark.version}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    stop_spark_session(spark)
