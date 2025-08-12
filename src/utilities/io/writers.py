"""
Data writing utilities for various destinations
"""

from pyspark.sql import DataFrame
from typing import Optional, Dict, Any


class DataWriter:
    """Utility class for writing data to various destinations"""
    
    def __init__(self, df: DataFrame):
        self.df = df
    
    def write_parquet(self, path: str, mode: str = "overwrite", 
                     partition_by: Optional[list] = None) -> None:
        """
        Write DataFrame as Parquet
        
        Args:
            path: Output path
            mode: Write mode (overwrite, append, etc.)
            partition_by: Columns to partition by
        """
        writer = self.df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
    
    def write_csv(self, path: str, mode: str = "overwrite", 
                 header: bool = True, **options) -> None:
        """
        Write DataFrame as CSV
        
        Args:
            path: Output path
            mode: Write mode
            header: Include header row
            **options: Additional CSV options
        """
        writer = self.df.write.mode(mode)
        writer = writer.option("header", header)
        
        for key, value in options.items():
            writer = writer.option(key, value)
            
        writer.csv(path)
    
    def write_delta(self, path: str, mode: str = "overwrite", 
                   partition_by: Optional[list] = None) -> None:
        """
        Write DataFrame as Delta table
        
        Args:
            path: Output path
            mode: Write mode
            partition_by: Columns to partition by
        """
        writer = self.df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
    
    def write_jdbc(self, url: str, table: str, mode: str = "overwrite",
                  properties: Optional[Dict[str, str]] = None) -> None:
        """
        Write DataFrame to JDBC destination
        
        Args:
            url: JDBC URL
            table: Target table name
            mode: Write mode
            properties: JDBC connection properties
        """
        writer = self.df.write.mode(mode)
        if properties:
            writer.jdbc(url, table, properties=properties)
        else:
            writer.jdbc(url, table)


def write_with_options(df: DataFrame, path: str, format_type: str,
                      options: Dict[str, Any]) -> None:
    """
    Write DataFrame with custom options
    
    Args:
        df: DataFrame to write
        path: Output path
        format_type: Output format
        options: Write options
    """
    writer = df.write.format(format_type)
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.save(path)
