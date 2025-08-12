"""
Sample ETL job demonstrating common patterns
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, current_timestamp
import logging

from src.utilities.spark_utils.session import create_spark_session
from src.utilities.io.readers import DataReader
from src.utilities.io.writers import DataWriter
from src.utilities.transformations.data_cleaning import handle_null_values, remove_duplicates
from src.configs.spark_config import get_config_for_env
from src.configs.logging_config import setup_logging


class SampleETLJob:
    """Sample ETL job class"""
    
    def __init__(self, env: str = "dev"):
        self.env = env
        self.config = get_config_for_env(env)
        self.logger = setup_logging(self.config["log_level"])
        self.spark = None
        
    def initialize_spark(self) -> None:
        """Initialize Spark session"""
        self.spark = create_spark_session(
            app_name=f"SampleETL-{self.env}",
            master="local[*]"
        )
        self.reader = DataReader(self.spark)
        
    def extract(self, input_path: str) -> DataFrame:
        """Extract data from source"""
        self.logger.info(f"Extracting data from: {input_path}")
        df = self.reader.read_csv(input_path, header=True, infer_schema=True)
        self.logger.info(f"Extracted {df.count()} rows")
        return df
        
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the data"""
        self.logger.info("Starting data transformation")
        
        # Remove duplicates
        df = remove_duplicates(df)
        
        # Handle null values
        df = handle_null_values(df, strategy="fill", fill_value="UNKNOWN")
        
        # Add audit column
        df = df.withColumn("processed_at", current_timestamp())
        
        # Business logic transformations
        if "status" in df.columns:
            df = df.withColumn("status_clean", 
                when(col("status").isin(["active", "ACTIVE"]), "ACTIVE")
                .when(col("status").isin(["inactive", "INACTIVE"]), "INACTIVE")
                .otherwise("UNKNOWN"))
        
        self.logger.info("Data transformation completed")
        return df
        
    def load(self, df: DataFrame, output_path: str) -> None:
        """Load data to destination"""
        self.logger.info(f"Loading data to: {output_path}")
        writer = DataWriter(df)
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
    job = SampleETLJob(env="dev")
    job.run(
        input_path="./assets/data/sample/input.csv",
        output_path="./output/transformed_data"
    )
