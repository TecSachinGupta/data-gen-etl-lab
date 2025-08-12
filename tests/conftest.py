"""
Pytest configuration and fixtures
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def sample_data(spark):
    """Create sample test data"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    data = [
        (1, "John Doe", "active"),
        (2, "Jane Smith", "inactive"),
        (3, "Bob Johnson", "active"),
        (1, "John Doe", "active"),  # duplicate
        (4, None, "active")  # null name
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_dataframe(spark):
    """Create empty DataFrame for testing"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    return spark.createDataFrame([], schema)
