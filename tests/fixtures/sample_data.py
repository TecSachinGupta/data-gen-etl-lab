"""Test data fixtures"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_sample_user_data(spark: SparkSession):
    """Create sample user data for testing."""
    data = [
        ("user_001", "Alice", 25, "Engineering"),
        ("user_002", "Bob", 30, "Marketing"),
        ("user_003", "Charlie", 35, "Engineering"),
        ("user_004", "Diana", 28, "Sales"),
        ("user_005", "Eve", 32, "Marketing"),
    ]
    
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("department", StringType(), False),
    ])
    
    return spark.createDataFrame(data, schema)


def create_sample_sales_data(spark: SparkSession):
    """Create sample sales data for testing."""
    data = [
        ("sale_001", "user_001", 1000.0, "2024-01-15"),
        ("sale_002", "user_002", 1500.0, "2024-01-16"), 
        ("sale_003", "user_001", 750.0, "2024-01-17"),
        ("sale_004", "user_003", 2000.0, "2024-01-18"),
        ("sale_005", "user_004", 1200.0, "2024-01-19"),
    ]
    
    schema = StructType([
        StructField("sale_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", StringType(), False),
        StructField("sale_date", StringType(), False),
    ])
    
    return spark.createDataFrame(data, schema)
