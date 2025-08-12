"""
Sample Airflow DAG for PySpark jobs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'sample_pyspark_pipeline',
    default_args=default_args,
    description='Sample PySpark data pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['pyspark', 'etl', 'sample']
)

# Data validation task
def validate_input_data(**context):
    """Validate input data exists and is readable"""
    import os
    from pathlib import Path
    
    input_path = "assets/data/sample/sample_data.csv"
    
    if not Path(input_path).exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Add more validation logic as needed
    print(f"Input data validation passed: {input_path}")
    return True

validate_data = PythonOperator(
    task_id='validate_input_data',
    python_callable=validate_input_data,
    dag=dag
)

# Word count job
word_count_job = SparkSubmitOperator(
    task_id='word_count_job',
    application='src/jobs/word_count_job.py',
    name='word_count_job',
    conn_id='spark_default',
    verbose=1,
    application_args=[],
    conf={
        'spark.master': 'local[*]',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g'
    },
    dag=dag
)

# Data quality check
quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python src/utilities/ml/data_quality_check.py tmp/word_count_output/',
    dag=dag
)

# Success notification
def send_success_notification(**context):
    """Send success notification"""
    print(f"Pipeline completed successfully at {datetime.now()}")
    # Add actual notification logic (email, Slack, etc.)

success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Define task dependencies
validate_data >> word_count_job >> quality_check >> success_notification
