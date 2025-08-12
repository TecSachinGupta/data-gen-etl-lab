"""Data quality checking utilities"""

import sys
from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when, isnull, mean, stddev, min as spark_min, max as spark_max


def check_data_quality(df: DataFrame) -> Dict[str, Any]:
    """
    Perform comprehensive data quality checks.
    
    Args:
        df: DataFrame to check
        
    Returns:
        Dictionary with quality metrics
    """
    results = {}
    
    # Basic info
    results['total_rows'] = df.count()
    results['total_columns'] = len(df.columns)
    
    # Null/missing value analysis
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_counts[column] = {
            'count': null_count,
            'percentage': (null_count / results['total_rows']) * 100
        }
    results['null_analysis'] = null_counts
    
    # Duplicate rows
    results['duplicate_rows'] = results['total_rows'] - df.dropDuplicates().count()
    
    return results


def generate_quality_report(df: DataFrame, dataset_name: str = "Dataset") -> None:
    """
    Generate and print data quality report.
    
    Args:
        df: DataFrame to analyze
        dataset_name: Name of the dataset for reporting
    """
    print(f"\n{'='*60}")
    print(f"DATA QUALITY REPORT: {dataset_name}")
    print(f"{'='*60}")
    
    quality_metrics = check_data_quality(df)
    
    # Basic Information
    print(f"\nBASIC INFORMATION:")
    print(f"  Total Rows: {quality_metrics['total_rows']:,}")
    print(f"  Total Columns: {quality_metrics['total_columns']}")
    print(f"  Duplicate Rows: {quality_metrics['duplicate_rows']}")
    
    # Missing Values
    print(f"\nMISSING VALUES:")
    for column, stats in quality_metrics['null_analysis'].items():
        if stats['count'] > 0:
            print(f"  {column}: {stats['count']} ({stats['percentage']:.2f}%)")
    
    print(f"\n{'='*60}")
