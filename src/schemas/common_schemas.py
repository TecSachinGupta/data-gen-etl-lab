"""
Common data schemas for PySpark applications
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, DateType
)


class CommonSchemas:
    """Collection of commonly used schemas"""
    
    @staticmethod
    def user_schema() -> StructType:
        """Schema for user data"""
        return StructType([
            StructField("user_id", StringType(), False),
            StructField("username", StringType(), False),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("is_active", BooleanType(), False)
        ])
    
    @staticmethod
    def transaction_schema() -> StructType:
        """Schema for transaction data"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
            StructField("transaction_date", DateType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("status", StringType(), False)
        ])
    
    @staticmethod
    def log_schema() -> StructType:
        """Schema for log data"""
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("level", StringType(), False),
            StructField("logger", StringType(), False),
            StructField("message", StringType(), False),
            StructField("exception", StringType(), True)
        ])
    
    @staticmethod
    def product_schema() -> StructType:
        """Schema for product data"""
        return StructType([
            StructField("product_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), False),
            StructField("description", StringType(), True),
            StructField("in_stock", BooleanType(), False),
            StructField("created_at", TimestampType(), False)
        ])


def get_schema_by_name(schema_name: str) -> StructType:
    """
    Get schema by name
    
    Args:
        schema_name: Name of the schema
        
    Returns:
        StructType: The requested schema
    """
    schemas_map = {
        "user": CommonSchemas.user_schema(),
        "transaction": CommonSchemas.transaction_schema(),
        "log": CommonSchemas.log_schema(),
        "product": CommonSchemas.product_schema()
    }
    
    return schemas_map.get(schema_name.lower())
