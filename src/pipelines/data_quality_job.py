"""
Data Quality validation job
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull
import logging

from src.utilities.spark_utils.session import create_spark_session
from src.utilities.io.readers import DataReader
from src.utilities.transformations.data_cleaning import validate_data_quality
from src.configs.logging_config import setup_logging


class DataQualityJob:
    """Data Quality validation and reporting job"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.spark = None
        
    def initialize_spark(self) -> None:
        """Initialize Spark session"""
        self.spark = create_spark_session(app_name="DataQuality")
        self.reader = DataReader(self.spark)
        
    def validate_completeness(self, df: DataFrame, required_columns: list) -> dict:
        """Validate data completeness"""
        results = {}
        total_rows = df.count()
        
        for column in required_columns:
            if column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                completeness_rate = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
                results[column] = {
                    "completeness_rate": completeness_rate,
                    "null_count": null_count,
                    "status": "PASS" if completeness_rate >= 95 else "FAIL"
                }
        
        return results
    
    def validate_uniqueness(self, df: DataFrame, unique_columns: list) -> dict:
        """Validate data uniqueness"""
        results = {}
        total_rows = df.count()
        
        for column in unique_columns:
            if column in df.columns:
                unique_count = df.select(column).distinct().count()
                uniqueness_rate = (unique_count / total_rows * 100) if total_rows > 0 else 0
                results[column] = {
                    "uniqueness_rate": uniqueness_rate,
                    "duplicate_count": total_rows - unique_count,
                    "status": "PASS" if uniqueness_rate >= 99 else "FAIL"
                }
        
        return results
    
    def generate_quality_report(self, df: DataFrame, validation_rules: dict) -> dict:
        """Generate comprehensive data quality report"""
        report = {
            "dataset_info": {
                "total_rows": df.count(),
                "total_columns": len(df.columns),
                "columns": df.columns
            },
            "completeness": {},
            "uniqueness": {},
            "overall_status": "PASS"
        }
        
        # Completeness validation
        if "required_columns" in validation_rules:
            report["completeness"] = self.validate_completeness(
                df, validation_rules["required_columns"]
            )
        
        # Uniqueness validation
        if "unique_columns" in validation_rules:
            report["uniqueness"] = self.validate_uniqueness(
                df, validation_rules["unique_columns"]
            )
        
        # Overall status
        failed_tests = []
        for test_type, results in report.items():
            if isinstance(results, dict):
                for column, result in results.items():
                    if isinstance(result, dict) and result.get("status") == "FAIL":
                        failed_tests.append(f"{test_type}.{column}")
        
        if failed_tests:
            report["overall_status"] = "FAIL"
            report["failed_tests"] = failed_tests
        
        return report
    
    def run(self, input_path: str, validation_rules: dict) -> dict:
        """Run data quality validation"""
        try:
            self.initialize_spark()
            
            # Load data
            df = self.reader.read_parquet(input_path)
            
            # Generate quality report
            quality_report = self.generate_quality_report(df, validation_rules)
            
            # Log results
            self.logger.info("Data Quality Report:")
            self.logger.info(f"Overall Status: {quality_report['overall_status']}")
            
            if quality_report["overall_status"] == "FAIL":
                self.logger.warning(f"Failed tests: {quality_report.get('failed_tests', [])}")
            
            return quality_report
            
        except Exception as e:
            self.logger.error(f"Data quality job failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    # Example usage
    validation_rules = {
        "required_columns": ["id", "name", "email"],
        "unique_columns": ["id", "email"]
    }
    
    job = DataQualityJob()
    report = job.run("./assets/data/sample/users.parquet", validation_rules)
    print(report)
