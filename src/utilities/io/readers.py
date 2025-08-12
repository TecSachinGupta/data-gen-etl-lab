"""
Data reading utilities for various sources
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any


class DataReader:
    """Utility class for reading data from various sources"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_csv(self, path: str, header: bool = True, 
                 infer_schema: bool = True, **options) -> DataFrame:
        """
        Read CSV file(s)
        
        Args:
            path: Path to CSV file(s)
            header: Whether CSV has header
            infer_schema: Whether to infer schema automatically
            **options: Additional options for CSV reader
            
        Returns:
            DataFrame: Loaded data
        """
        reader = self.spark.read.format("csv")
        reader = reader.option("header", header)
        reader = reader.option("inferSchema", infer_schema)
        
        for key, value in options.items():
            reader = reader.option(key, value)
            
        return reader.load(path)
    
    def read_parquet(self, path: str) -> DataFrame:
        """
        Read Parquet file(s)
        
        Args:
            path: Path to Parquet file(s)
            
        Returns:
            DataFrame: Loaded data
        """
        return self.spark.read.parquet(path)
    
    def read_json(self, path: str, multiline: bool = False) -> DataFrame:
        """
        Read JSON file(s)
        
        Args:
            path: Path to JSON file(s)
            multiline: Whether JSON is multiline format
            
        Returns:
            DataFrame: Loaded data
        """
        return self.spark.read.option("multiline", multiline).json(path)
    
    def read_delta(self, path: str, version: Optional[int] = None) -> DataFrame:
        """
        Read Delta table
        
        Args:
            path: Path to Delta table
            version: Specific version to read (optional)
            
        Returns:
            DataFrame: Loaded data
        """
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        return reader.load(path)
    
    def read_jdbc(self, url: str, table: str, properties: Dict[str, str]) -> DataFrame:
        """
        Read from JDBC source
        
        Args:
            url: JDBC URL
            table: Table name or SQL query
            properties: JDBC connection properties
            
        Returns:
            DataFrame: Loaded data
        """
        return self.spark.read.jdbc(url, table, properties=properties)


def read_with_schema(spark: SparkSession, path: str, schema, 
                    format_type: str = "parquet") -> DataFrame:
    """
    Read data with predefined schema
    
    Args:
        spark: SparkSession
        path: Path to data
        schema: StructType schema
        format_type: Data format (parquet, csv, json, etc.)
        
    Returns:
        DataFrame: Loaded data with schema
    """
    return spark.read.schema(schema).format(format_type).load(path)
