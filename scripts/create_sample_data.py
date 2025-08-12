#!/usr/bin/env python3
"""
Script to create sample data files for testing and tutorials
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import random

# Add src to path
sys.path.append('.')

from src.utilities.spark_utils import create_spark_session
from src.utilities.io import write_csv, write_parquet


def create_employee_data(spark, num_records=1000):
    """Create sample employee data."""
    
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    
    data = []
    for i in range(num_records):
        employee_id = f"EMP_{i+1:04d}"
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        age = random.randint(22, 65)
        salary = random.randint(40000, 120000)
        department = random.choice(departments)
        
        # Random hire date in the last 3 years
        start_date = datetime.now() - timedelta(days=3*365)
        random_days = random.randint(0, 3*365)
        hire_date = start_date + timedelta(days=random_days)
        
        data.append((employee_id, name, age, salary, department, hire_date))
    
    schema = ["employee_id", "name", "age", "salary", "department", "hire_date"]
    return spark.createDataFrame(data, schema)


def main():
    """Create all sample data files."""
    
    print("Creating sample data files...")
    
    # Create directories
    sample_dir = Path("assets/data/sample")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session("CreateSampleData")
    
    try:
        # Create employee data
        print("Creating employee data...")
        employee_df = create_employee_data(spark, 1000)
        
        # Save in multiple formats
        write_csv(employee_df, str(sample_dir / "employees_large.csv"))
        write_parquet(employee_df, str(sample_dir / "employees_large.parquet"))
        
        print(f"Created employee data: {employee_df.count()} records")
        
        # Create a small dataset for quick tests
        print("Creating small test dataset...")
        small_employee_df = employee_df.limit(50)
        write_csv(small_employee_df, str(sample_dir / "employees_small.csv"))
        
        print("✓ All sample data files created successfully!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
