# Basic Data Transformations

This guide demonstrates common data transformation patterns using our utility functions.

## Data Cleaning

### Removing Duplicates
```python
from src.utilities.transformations import remove_duplicates

# Remove duplicates based on all columns
clean_df = remove_duplicates(df)

# Remove duplicates based on specific columns
clean_df = remove_duplicates(df, subset=["name", "email"])
```

### Handling Missing Values
```python
from src.utilities.transformations import handle_nulls

# Drop rows with any null values
clean_df = handle_nulls(df, strategy="drop")

# Fill nulls with specific values
clean_df = handle_nulls(df, strategy="fill", fill_values={
    "age": 0,
    "department": "Unknown"
})
```

## Feature Engineering

### Creating Feature Vectors
```python
from src.utilities.ml import create_features

# Create features from numerical and categorical columns
feature_df = create_features(
    df,
    numerical_cols=["age", "salary"],
    categorical_cols=["department"]
)
```

### Scaling Features
```python
from src.utilities.ml import scale_features

# Standard scaling
scaled_df = scale_features(df, scaler_type="standard")

# Min-Max scaling
scaled_df = scale_features(df, scaler_type="minmax")
```

## Advanced Examples

### Custom Transformations
```python
from pyspark.sql.functions import col, when, upper

def categorize_salary(df):
    return df.withColumn(
        "salary_category",
        when(col("salary") < 50000, "Low")
        .when(col("salary") < 80000, "Medium")
        .otherwise("High")
    )

# Apply custom transformation
result_df = categorize_salary(df)
```
