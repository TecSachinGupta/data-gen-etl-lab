"""
Data cleaning utilities for PySpark DataFrames
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, trim, regexp_replace
from typing import List, Optional


def remove_duplicates(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    """
    Remove duplicate rows from DataFrame
    
    Args:
        df: Input DataFrame
        subset: List of column names to consider for duplicates
        
    Returns:
        DataFrame with duplicates removed
    """
    return df.dropDuplicates(subset)


def handle_null_values(df: DataFrame, strategy: str = "drop", 
                      fill_value=None, subset: List[str] = None) -> DataFrame:
    """
    Handle null values in DataFrame
    
    Args:
        df: Input DataFrame
        strategy: "drop", "fill", or "replace"
        fill_value: Value to fill nulls with (for "fill" strategy)
        subset: Columns to apply the strategy to
        
    Returns:
        DataFrame with null values handled
    """
    if strategy == "drop":
        return df.dropna(subset=subset)
    elif strategy == "fill" and fill_value is not None:
        return df.fillna(fill_value, subset=subset)
    elif strategy == "replace":
        # Replace with column-specific logic
        columns_to_process = subset if subset else df.columns
        for column in columns_to_process:
            if column in df.columns:
                df = df.withColumn(column, 
                    when(col(column).isNull() | isnan(col(column)), 
                         "UNKNOWN").otherwise(col(column)))
        return df
    else:
        return df


def clean_string_columns(df: DataFrame, columns: List[str] = None) -> DataFrame:
    """
    Clean string columns by trimming whitespace and standardizing
    
    Args:
        df: Input DataFrame
        columns: List of columns to clean (all string columns if None)
        
    Returns:
        DataFrame with cleaned string columns
    """
    if columns is None:
        # Get all string columns
        columns = [col_name for col_name, dtype in df.dtypes if dtype == 'string']
    
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, 
                trim(regexp_replace(col(column), r'\s+', ' ')))
    
    return df


def validate_data_quality(df: DataFrame) -> dict:
    """
    Generate basic data quality metrics
    
    Args:
        df: Input DataFrame
        
    Returns:
        dict: Data quality metrics
    """
    total_rows = df.count()
    metrics = {
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "null_counts": {},
        "duplicate_count": df.count() - df.dropDuplicates().count()
    }
    
    # Count nulls per column
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        metrics["null_counts"][column] = {
            "count": null_count,
            "percentage": (null_count / total_rows * 100) if total_rows > 0 else 0
        }
    
    return metrics
