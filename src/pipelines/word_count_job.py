"""
Simple Word Count Spark Job
Example of a basic PySpark application.
"""

import sys
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, split, lower, regexp_replace, col

# Add src to path for imports
sys.path.append('.')

from src.utilities.spark_utils import create_spark_session, stop_spark_session
from src.utilities.common import setup_logging, get_config


def word_count(spark, input_path: str, output_path: str) -> DataFrame:
    """
    Perform word count on text files.
    
    Args:
        spark: Spark session
        input_path: Path to input text files
        output_path: Path to save results
        
    Returns:
        DataFrame with word counts
    """
    # Read text files
    text_df = spark.read.text(input_path)
    
    # Clean and split text into words
    words_df = text_df.select(
        explode(
            split(
                regexp_replace(lower(col("value")), "[^a-zA-Z0-9\\s]", ""), 
                "\\s+"
            )
        ).alias("word")
    ).filter(col("word") != "")
    
    # Count words
    word_counts = words_df.groupBy("word").count().orderBy(col("count").desc())
    
    # Save results
    word_counts.write.mode("overwrite").parquet(output_path)
    
    return word_counts


def main():
    """Main function"""
    # Setup logging
    logger = setup_logging("WordCountJob")
    
    # Create Spark session
    spark = create_spark_session(
        app_name="WordCountJob",
        config={
            "spark.sql.shuffle.partitions": "200"
        }
    )
    
    try:
        # Default paths
        input_path = "assets/data/sample/sample_text.txt"
        output_path = "tmp/word_count_output"
        
        logger.info(f"Starting word count job")
        logger.info(f"Input: {input_path}")
        logger.info(f"Output: {output_path}")
        
        # Run word count
        result_df = word_count(spark, input_path, output_path)
        
        # Show results
        logger.info("Top 20 words:")
        result_df.show(20)
        
        logger.info(f"Total unique words: {result_df.count()}")
        logger.info("Word count job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
