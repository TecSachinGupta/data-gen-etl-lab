"""
Batch Job Template

Copy this template to create new batch processing jobs.
"""

import sys
import argparse
from typing import Optional, Dict, Any
from datetime import datetime

# Add src to path for imports
sys.path.append('.')

from src.utilities.spark_utils import create_spark_session, stop_spark_session
from src.utilities.common import setup_logging, get_config
from src.utilities.io import read_parquet, write_parquet
from src.utilities.transformations import clean_data


class BatchJobTemplate:
    """Template class for batch processing jobs."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the batch job.
        
        Args:
            config: Job configuration
        """
        self.config = config
        self.logger = setup_logging(f"{self.__class__.__name__}")
        self.spark = None
        
    def setup_spark(self) -> None:
        """Setup Spark session."""
        self.spark = create_spark_session(
            app_name=self.config.get('app_name', 'BatchJob'),
            config=self.config.get('spark_config', {})
        )
        
    def extract(self) -> Any:
        """
        Extract data from source.
        Override this method with your extraction logic.
        
        Returns:
            Extracted data
        """
        self.logger.info("Extracting data...")
        
        # Example extraction
        input_path = self.config['input_path']
        df = read_parquet(self.spark, input_path)
        
        self.logger.info(f"Extracted {df.count()} records")
        return df
        
    def transform(self, data: Any) -> Any:
        """
        Transform the extracted data.
        Override this method with your transformation logic.
        
        Args:
            data: Raw data from extraction
            
        Returns:
            Transformed data
        """
        self.logger.info("Transforming data...")
        
        # Example transformation
        cleaned_data = clean_data(data, remove_duplicates=True)
        
        self.logger.info(f"Transformation complete. Result: {cleaned_data.count()} records")
        return cleaned_data
        
    def load(self, data: Any) -> None:
        """
        Load transformed data to destination.
        Override this method with your loading logic.
        
        Args:
            data: Transformed data
        """
        self.logger.info("Loading data...")
        
        # Example loading
        output_path = self.config['output_path']
        write_parquet(data, output_path)
        
        self.logger.info(f"Data loaded to {output_path}")
        
    def validate(self) -> bool:
        """
        Validate job results.
        Override this method with your validation logic.
        
        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info("Validating results...")
        
        # Example validation
        output_path = self.config['output_path']
        result_df = read_parquet(self.spark, output_path)
        
        if result_df.count() > 0:
            self.logger.info("Validation passed")
            return True
        else:
            self.logger.error("Validation failed: No data in output")
            return False
            
    def run(self) -> bool:
        """
        Run the complete batch job.
        
        Returns:
            True if job succeeds, False otherwise
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Starting batch job: {self.__class__.__name__}")
            
            # Setup
            self.setup_spark()
            
            # ETL Process
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)
            
            # Validation
            if not self.validate():
                raise Exception("Job validation failed")
            
            duration = datetime.now() - start_time
            self.logger.info(f"Batch job completed successfully in {duration}")
            return True
            
        except Exception as e:
            duration = datetime.now() - start_time
            self.logger.error(f"Batch job failed after {duration}: {str(e)}")
            return False
            
        finally:
            if self.spark:
                stop_spark_session(self.spark)


def main():
    """Main function for command line execution."""
    parser = argparse.ArgumentParser(description='Batch Job Template')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--input', help='Input data path')
    parser.add_argument('--output', help='Output data path')
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        config = get_config(args.config)
    else:
        config = {
            'app_name': 'BatchJobTemplate',
            'input_path': args.input or 'assets/data/sample/employees.csv',
            'output_path': args.output or 'tmp/batch_job_output',
            'spark_config': {
                'spark.sql.shuffle.partitions': '200'
            }
        }
    
    # Run job
    job = BatchJobTemplate(config)
    success = job.run()
    
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
