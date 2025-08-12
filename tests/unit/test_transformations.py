"""Unit tests for transformation utilities"""

import pytest
from pyspark.sql import SparkSession
from src.utilities.transformations import clean_data, remove_duplicates
from src.utilities.spark_utils import create_spark_session


class TestTransformations:
    """Test class for data transformations"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        spark = create_spark_session("TestTransformations")
        yield spark
        spark.stop()
    
    def test_clean_data(self, spark):
        """Test data cleaning function."""
        # Sample dirty data
        data = [
            ("  Alice  ", "ENGINEER", "123-456-7890"),
            ("Bob", "manager", "(555) 123-4567"),
            ("  Alice  ", "ENGINEER", "123-456-7890"),  # duplicate
        ]
        df = spark.createDataFrame(data, ["name", "role", "phone"])
        
        # Clean data
        cleaned_df = clean_data(
            df,
            columns_to_trim=["name"],
            columns_to_lower=["role"],
            remove_duplicates=True
        )
        
        # Assertions
        assert cleaned_df.count() == 2  # Should remove duplicate
        
        # Check if trimming worked
        names = [row["name"] for row in cleaned_df.collect()]
        assert "Alice" in names  # Should be trimmed
        assert "  Alice  " not in names
        
        # Check if lowercase worked
        roles = [row["role"] for row in cleaned_df.collect()]
        assert "engineer" in roles
        assert "ENGINEER" not in roles
    
    def test_remove_duplicates(self, spark):
        """Test duplicate removal."""
        data = [
            ("Alice", 25),
            ("Bob", 30),
            ("Alice", 25),  # duplicate
        ]
        df = spark.createDataFrame(data, ["name", "age"])
        
        result_df = remove_duplicates(df)
        
        assert result_df.count() == 2
