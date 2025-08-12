"""
ETL Pipeline Example
Demonstrates a complete Extract, Transform, Load pipeline
"""

import sys
from typing import Dict, Any
from pyspark.sql import DataFrame

sys.path.append('.')

from src.utilities.spark_utils import create_spark_session
from src.utilities.io import read_csv, write_parquet
from src.utilities.transformations import clean_data
from src.utilities.common import setup_logging, get_config


class ETLPipeline:
    """ETL Pipeline class"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize ETL Pipeline.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = setup_logging("ETLPipeline")
        self.spark = create_spark_session(
            app_name="ETLPipeline",
            config=config.get('spark', {})
        )
    
    def extract(self) -> DataFrame:
        """
        Extract data from source.
        
        Returns:
            Raw DataFrame
        """
        self.logger.info("Starting data extraction...")
        
        input_path = self.config['data']['input_path']
        df = read_csv(self.spark, input_path)
        
        self.logger.info(f"Extracted {df.count()} rows from {input_path}")
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation...")
        
        # Apply transformations
        transformed_df = clean_data(
            df,
            columns_to_trim=['name', 'department'],
            columns_to_lower=['department'],
            remove_duplicates=True
        )
        
        # Add business logic transformations here
        # Example: Calculate age groups
        from pyspark.sql.functions import col, when
        
        transformed_df = transformed_df.withColumn(
            "age_group",
            when(col("age") < 30, "Young")
            .when(col("age") < 50, "Middle")
            .otherwise("Senior")
        )
        
        self.logger.info(f"Transformation completed. Rows: {transformed_df.count()}")
        return transformed_df
    
    def load(self, df: DataFrame) -> None:
        """
        Load data to destination.
        
        Args:
            df: Transformed DataFrame
        """
        self.logger.info("Starting data loading...")
        
        output_path = self.config['data']['output_path']
        write_parquet(df, output_path, partition_by=['department'])
        
        self.logger.info(f"Data loaded to {output_path}")
    
    def run(self) -> None:
        """Run the complete ETL pipeline."""
        try:
            self.logger.info("Starting ETL Pipeline")
            
            # Extract
            raw_df = self.extract()
            
            # Transform  
            transformed_df = self.transform(raw_df)
            
            # Load
            self.load(transformed_df)
            
            self.logger.info("ETL Pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark.stop()


# Example usage
if __name__ == "__main__":
    # Sample configuration
    config = {
        'spark': {
            'spark.sql.shuffle.partitions': '200'
        },
        'data': {
            'input_path': 'assets/data/sample/employees.csv',
            'output_path': 'tmp/etl_output'
        }
    }
    
    # Run pipeline
    pipeline = ETLPipeline(config)
    pipeline.run()
