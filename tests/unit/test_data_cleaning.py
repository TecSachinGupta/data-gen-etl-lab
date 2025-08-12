"""
Unit tests for data cleaning utilities
"""

import pytest
from pyspark.sql.functions import col
from src.utilities.transformations.data_cleaning import (
    remove_duplicates, handle_null_values, validate_data_quality
)


def test_remove_duplicates(spark, sample_data):
    """Test duplicate removal"""
    result = remove_duplicates(sample_data)
    
    # Should have 4 rows instead of 5 (one duplicate removed)
    assert result.count() == 4
    
    # Check that duplicate was actually removed
    id_counts = result.groupBy("id").count().collect()
    for row in id_counts:
        assert row["count"] == 1


def test_handle_null_values_drop(spark, sample_data):
    """Test null value handling with drop strategy"""
    result = handle_null_values(sample_data, strategy="drop")
    
    # Should remove row with null name
    assert result.count() == 4
    
    # No nulls should remain in name column
    null_count = result.filter(col("name").isNull()).count()
    assert null_count == 0


def test_handle_null_values_fill(spark, sample_data):
    """Test null value handling with fill strategy"""
    result = handle_null_values(sample_data, strategy="fill", fill_value="Unknown")
    
    # Should keep all rows
    assert result.count() == 5
    
    # Null should be replaced with "Unknown"
    unknown_count = result.filter(col("name") == "Unknown").count()
    assert unknown_count == 1


def test_validate_data_quality(spark, sample_data):
    """Test data quality validation"""
    metrics = validate_data_quality(sample_data)
    
    # Check basic metrics
    assert metrics["total_rows"] == 5
    assert metrics["total_columns"] == 3
    assert metrics["duplicate_count"] == 1
    
    # Check null counts
    assert "name" in metrics["null_counts"]
    assert metrics["null_counts"]["name"]["count"] == 1


def test_validate_data_quality_empty(spark, empty_dataframe):
    """Test data quality validation on empty DataFrame"""
    metrics = validate_data_quality(empty_dataframe)
    
    assert metrics["total_rows"] == 0
    assert metrics["total_columns"] == 2
    assert metrics["duplicate_count"] == 0
