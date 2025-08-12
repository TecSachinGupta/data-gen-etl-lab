"""
ETL Job Template
Copy this template to create new ETL jobs
"""

from pyspark.sql import SparkSession, DataFrame
import logging

from src.utilities.spark_utils.session import create_spark_session
from src.utilities.io.readers import DataReader
from src.utilities.io.writers import DataWriter
from src.configs.spark_config import get_config_for_env
from src.configs.logging_config import setup_logging


class {JOB_NAME}:
    """TODO: Add job description"""
    
    def __init__(self, env: str = "dev"):
        self.env = env
        self.config = get_config_for_env(env)
        self.logger = setup_logging(self.config["log_level"])
        self.spark = None
        
    def initialize_spark(self) -> None:
        """Initialize Spark session"""
        self.spark = create_spark_session(
            app_name=f"{self.__class__.__name__}-{self.env}",
            master="local[*]"
        )
        self.reader = DataReader(self.spark)
        
    def extract(self, input_path: str) -> DataFrame:
        """
        Extract data from source
        TODO: Implement extraction logic
        """
        self.logger.info(f"Extracting data from: {input_path}")
        # TODO: Replace with actual extraction logic
        df = self.reader.read_parquet(input_path)
        self.logger.info(f"Extracted {df.count()} rows")
        return df
        
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the data
        TODO: Implement transformation logic
        """
        self.logger.info("Starting data transformation")
        
        # TODO: Add your transformation logic here
        
        self.logger.info("Data transformation completed")
        return df
        
    def load(self, df: DataFrame, output_path: str) -> None:
        """
        Load data to destination
        TODO: Implement loading logic
        """
        self.logger.info(f"Loading data to: {output_path}")
        writer = DataWriter(df)
        # TODO: Choose appropriate output format
        writer.write_parquet(output_path, mode="overwrite")
        self.logger.info("Data loading completed")
        
    def run(self, input_path: str, output_path: str) -> None:
        """Run the complete ETL pipeline"""
        try:
            self.initialize_spark()
            
            # ETL Pipeline
            raw_df = self.extract(input_path)
            transformed_df = self.transform(raw_df)
            self.load(transformed_df, output_path)
            
            self.logger.info("ETL job completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL job failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    # Example usage
    job = {JOB_NAME}(env="dev")
    job.run(
        input_path="TODO: Set input path",
        output_path="TODO: Set output path"
    )
