"""Data cleaning transformations"""

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, regexp_replace, when, isnan


def clean_data(
    df: DataFrame,
    columns_to_trim: Optional[List[str]] = None,
    columns_to_lower: Optional[List[str]] = None,
    remove_duplicates: bool = True
) -> DataFrame:
    """
    Apply common data cleaning operations.
    
    Args:
        df: Input DataFrame
        columns_to_trim: Columns to trim whitespace
        columns_to_lower: Columns to convert to lowercase
        remove_duplicates: Whether to remove duplicate rows
        
    Returns:
        Cleaned DataFrame
    """
    result_df = df
    
    # Trim whitespace
    if columns_to_trim:
        for column in columns_to_trim:
            if column in df.columns:
                result_df = result_df.withColumn(column, trim(col(column)))
    
    # Convert to lowercase
    if columns_to_lower:
        for column in columns_to_lower:
            if column in df.columns:
                result_df = result_df.withColumn(column, lower(col(column)))
    
    # Remove duplicates
    if remove_duplicates:
        result_df = result_df.dropDuplicates()
    
    return result_df


def remove_duplicates(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    """
    Remove duplicate rows.
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for deduplication
        
    Returns:
        DataFrame without duplicates
    """
    return df.dropDuplicates(subset)


def handle_nulls(
    df: DataFrame, 
    strategy: str = "drop",
    fill_values: Optional[dict] = None
) -> DataFrame:
    """
    Handle null values in DataFrame.
    
    Args:
        df: Input DataFrame
        strategy: "drop", "fill", or "keep"
        fill_values: Values to fill nulls with (column_name: value)
        
    Returns:
        DataFrame with nulls handled
    """
    if strategy == "drop":
        return df.dropna()
    elif strategy == "fill" and fill_values:
        return df.fillna(fill_values)
    elif strategy == "keep":
        return df
    else:
        raise ValueError("Invalid strategy or missing fill_values")


def standardize_phone_numbers(df: DataFrame, phone_column: str) -> DataFrame:
    """
    Standardize phone number format.
    
    Args:
        df: Input DataFrame
        phone_column: Name of phone number column
        
    Returns:
        DataFrame with standardized phone numbers
    """
    return df.withColumn(
        phone_column,
        regexp_replace(
            regexp_replace(col(phone_column), r"[^\d]", ""),
            r"(\d{3})(\d{3})(\d{4})",
            r"($1) $2-$3"
        )
    )
