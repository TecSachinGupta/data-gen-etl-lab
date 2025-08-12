"""
Spark configuration settings for different environments
"""

from typing import Dict, Any
import os


class SparkConfig:
    """Spark configuration for different environments"""
    
    @staticmethod
    def get_local_config() -> Dict[str, Any]:
        """Configuration for local development"""
        return {
            "spark.app.name": "PySpark Local App",
            "spark.master": "local[*]",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.warehouse.dir": "spark-warehouse",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g"
        }
    
    @staticmethod
    def get_cluster_config() -> Dict[str, Any]:
        """Configuration for cluster deployment"""
        return {
            "spark.app.name": "PySpark Cluster App",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "10",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2"
        }
    
    @staticmethod
    def get_config_for_env(env: str = None) -> Dict[str, Any]:
        """Get configuration based on environment"""
        if env is None:
            env = os.getenv("SPARK_ENV", "local")
        
        if env.lower() == "cluster":
            return SparkConfig.get_cluster_config()
        else:
            return SparkConfig.get_local_config()


# Environment-specific configurations
ENVIRONMENTS = {
    "dev": {
        "data_path": "./assets/data/sample",
        "output_path": "./output/dev",
        "log_level": "INFO"
    },
    "staging": {
        "data_path": "/data/staging",
        "output_path": "/output/staging", 
        "log_level": "WARN"
    },
    "prod": {
        "data_path": "/data/prod",
        "output_path": "/output/prod",
        "log_level": "ERROR"
    }
}


def get_env_config(env: str = "dev") -> Dict[str, Any]:
    """Get environment-specific configuration"""
    return ENVIRONMENTS.get(env, ENVIRONMENTS["dev"])
