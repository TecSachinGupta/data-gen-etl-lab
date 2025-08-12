"""Feature engineering utilities"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, count, mean, stddev
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler


def create_features(
    df: DataFrame,
    numerical_cols: List[str],
    categorical_cols: List[str] = None
) -> DataFrame:
    """
    Create feature vector from numerical and categorical columns.
    
    Args:
        df: Input DataFrame
        numerical_cols: List of numerical column names
        categorical_cols: List of categorical column names
        
    Returns:
        DataFrame with features vector column
    """
    feature_cols = numerical_cols.copy()
    result_df = df
    
    # Handle categorical columns (simple one-hot encoding alternative)
    if categorical_cols:
        for col_name in categorical_cols:
            # Get unique values
            unique_values = [row[0] for row in df.select(col_name).distinct().collect()]
            
            # Create binary columns for each unique value
            for value in unique_values:
                new_col_name = f"{col_name}_{value}"
                result_df = result_df.withColumn(
                    new_col_name,
                    when(col(col_name) == value, 1.0).otherwise(0.0)
                )
                feature_cols.append(new_col_name)
    
    # Create feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    return assembler.transform(result_df)


def scale_features(
    df: DataFrame,
    input_col: str = "features",
    output_col: str = "scaled_features",
    scaler_type: str = "standard"
) -> DataFrame:
    """
    Scale feature vectors.
    
    Args:
        df: DataFrame with feature vectors
        input_col: Name of input vector column
        output_col: Name of output scaled vector column
        scaler_type: Type of scaler ("standard" or "minmax")
        
    Returns:
        DataFrame with scaled features
    """
    if scaler_type == "standard":
        scaler = StandardScaler(
            inputCol=input_col,
            outputCol=output_col,
            withStd=True,
            withMean=True
        )
    elif scaler_type == "minmax":
        scaler = MinMaxScaler(
            inputCol=input_col,
            outputCol=output_col
        )
    else:
        raise ValueError("scaler_type must be 'standard' or 'minmax'")
    
    # Fit and transform
    scaler_model = scaler.fit(df)
    return scaler_model.transform(df)
