# Utility Usage Examples

## Spark Session Management

```python
from src.utilities.spark_utils.session import create_spark_session

# Create session with default config
spark = create_spark_session(app_name="MyApp")

# Create session with custom config
custom_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.memory": "4g"
}
spark = create_spark_session(
    app_name="MyApp", 
    config_dict=custom_config
)
```

## Data Reading and Writing

```python
from src.utilities.io.readers import DataReader
from src.utilities.io.writers import DataWriter

# Reading data
reader = DataReader(spark)
df = reader.read_csv("path/to/file.csv")
df = reader.read_parquet("path/to/file.parquet")

# Writing data  
writer = DataWriter(df)
writer.write_parquet("output/path", partition_by=["date"])
writer.write_csv("output/path.csv", header=True)
```

## Data Cleaning

```python
from src.utilities.transformations.data_cleaning import (
    remove_duplicates, handle_null_values, validate_data_quality
)

# Remove duplicates
clean_df = remove_duplicates(df, subset=["id"])

# Handle nulls
filled_df = handle_null_values(df, strategy="fill", fill_value="UNKNOWN")

# Validate data quality
quality_metrics = validate_data_quality(df)
```

## Feature Engineering

```python
from src.utilities.ml.feature_engineering import (
    create_features_vector, scale_features, encode_categorical_columns
)

# Create feature vector
feature_df = create_features_vector(df, ["col1", "col2", "col3"])

# Scale features
scaled_df = scale_features(feature_df)

# Encode categorical columns
encoded_df = encode_categorical_columns(df, ["category", "status"])
```
