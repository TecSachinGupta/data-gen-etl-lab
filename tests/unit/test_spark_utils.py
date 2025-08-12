"""Unit tests for Spark utilities"""

import pytest
from pyspark.sql import SparkSession
from src.utilities.spark_utils import create_spark_session, show_df_info


class TestSparkUtils:
    """Test class for Spark utilities"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        spark = create_spark_session("TestSession")
        yield spark
        spark.stop()
    
    def test_create_spark_session(self):
        """Test Spark session creation."""
        spark = create_spark_session("TestApp")
        assert spark is not None
        assert spark.sparkContext.appName == "TestApp"
        spark.stop()
    
    def test_show_df_info(self, spark, capsys):
        """Test DataFrame info display."""
        # Create test DataFrame
        data = [("Alice", 25), ("Bob", 30)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        # Test show_df_info
        show_df_info(df, "Test DataFrame")
        
        # Capture output
        captured = capsys.readouterr()
        assert "Test DataFrame Info" in captured.out
        assert "Rows: 2" in captured.out
