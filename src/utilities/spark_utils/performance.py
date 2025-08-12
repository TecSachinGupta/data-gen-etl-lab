"""Spark performance optimization utilities"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def optimize_joins(df1: DataFrame, df2: DataFrame, join_key: str) -> DataFrame:
    """
    Optimize join operations with broadcast hints.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame (will be broadcast if small)
        join_key: Column to join on
    
    Returns:
        Joined DataFrame
    """
    # Simple heuristic: broadcast smaller DataFrame
    df1_count = df1.count()
    df2_count = df2.count() 
    
    if df2_count < df1_count * 0.1:  # df2 is less than 10% of df1
        from pyspark.sql.functions import broadcast
        return df1.join(broadcast(df2), join_key)
    else:
        return df1.join(df2, join_key)


def repartition_by_column(df: DataFrame, column: str, num_partitions: int = None) -> DataFrame:
    """
    Repartition DataFrame by column for better performance.
    
    Args:
        df: DataFrame to repartition
        column: Column to partition by
        num_partitions: Number of partitions (optional)
    
    Returns:
        Repartitioned DataFrame
    """
    if num_partitions:
        return df.repartition(num_partitions, col(column))
    else:
        return df.repartition(col(column))
