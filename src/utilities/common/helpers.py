"""
Common helper functions for PySpark applications
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from typing import List, Union, Any
import logging


def add_audit_columns(df: DataFrame, user: str = "system") -> DataFrame:
    """
    Add audit columns to DataFrame
    
    Args:
        df: Input DataFrame
        user: User identifier for audit
        
    Returns:
        DataFrame with audit columns added
    """
    return df.withColumn("created_at", current_timestamp()) \
             .withColumn("created_by", lit(user))


def select_columns_safe(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Safely select columns (ignores missing columns)
    
    Args:
        df: Input DataFrame
        columns: List of column names to select
        
    Returns:
        DataFrame with selected columns
    """
    existing_columns = [col_name for col_name in columns if col_name in df.columns]
    missing_columns = [col_name for col_name in columns if col_name not in df.columns]
    
    if missing_columns:
        logging.warning(f"Missing columns: {missing_columns}")
    
    return df.select(*existing_columns)


def rename_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Rename multiple columns using a mapping dictionary
    
    Args:
        df: Input DataFrame
        column_mapping: Dictionary mapping old names to new names
        
    Returns:
        DataFrame with renamed columns
    """
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def cast_columns(df: DataFrame, column_types: dict) -> DataFrame:
    """
    Cast multiple columns to specified types
    
    Args:
        df: Input DataFrame
        column_types: Dictionary mapping column names to target types
        
    Returns:
        DataFrame with columns cast to new types
    """
    for column_name, target_type in column_types.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(target_type))
    return df


def get_column_stats(df: DataFrame, columns: List[str] = None) -> dict:
    """
    Get basic statistics for specified columns
    
    Args:
        df: Input DataFrame
        columns: Columns to analyze (all if None)
        
    Returns:
        dict: Statistics for each column
    """
    if columns is None:
        columns = df.columns
    
    stats = {}
    for column in columns:
        if column in df.columns:
            col_stats = df.select(column).describe().collect()
            stats[column] = {row['summary']: row[column] for row in col_stats}
    
    return stats


def log_dataframe_info(df: DataFrame, name: str = "DataFrame") -> None:
    """
    Log basic information about a DataFrame
    
    Args:
        df: DataFrame to analyze
        name: Name for logging purposes
    """
    logging.info(f"{name} Info:")
    logging.info(f"  Rows: {df.count()}")
    logging.info(f"  Columns: {len(df.columns)}")
    logging.info(f"  Schema: {df.dtypes}")
