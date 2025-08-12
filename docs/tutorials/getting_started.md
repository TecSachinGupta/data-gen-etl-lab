# Getting Started with Pydataengineertemplate

This guide will help you get up and running with the PySpark repository.

## Prerequisites

- Python 3.8+
- Java 8 or 11 (for Spark)
- Git

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd PyDataEngineerTemplate
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

4. **Run setup script:**
   ```bash
   bash scripts/setup.sh
   ```

5. **Verify installation:**
   ```bash
   python -c "from src.utilities.spark_utils import create_spark_session; spark = create_spark_session('test'); print(f'Spark {spark.version} is working!'); spark.stop()"
   ```

## Quick Examples

### Basic Spark Session
```python
from src.utilities.spark_utils import create_spark_session

spark = create_spark_session("MyApp")
df = spark.range(1000)
print(f"Created DataFrame with {df.count()} rows")
spark.stop()
```

### Data Processing
```python
from src.utilities.io import read_csv
from src.utilities.transformations import clean_data

spark = create_spark_session("DataProcessing")

# Read data
df = read_csv(spark, "assets/data/sample/employees.csv")

# Clean data
clean_df = clean_data(df, columns_to_trim=["name"], remove_duplicates=True)

# Show results
clean_df.show()
spark.stop()
```

### Running Jobs
```bash
# Run word count example
python src/jobs/word_count_job.py

# Run ETL pipeline
python src/pipeline/etl_pipeline.py
```

## Next Steps

- Explore the utilities in `src/utilities/`
- Check out example jobs in `src/jobs/`
- Run the provided Jupyter notebooks
- Read the API documentation
