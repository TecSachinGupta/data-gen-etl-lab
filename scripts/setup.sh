#!/bin/bash

# Setup script for PyDataEngineerTemplate

echo "Setting up PyDataEngineerTemplate..."

# Create necessary directories
mkdir -p logs
mkdir -p tmp
mkdir -p spark-warehouse

# Create environment file from template
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env file from template. Please update with your settings."
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install package in development mode
echo "Installing package in development mode..."
pip install -e .

# Setup pre-commit hooks (if available)
if command -v pre-commit &> /dev/null; then
    echo "Setting up pre-commit hooks..."
    pre-commit install
fi

# Test installation
echo "Testing installation..."
python -c "
from src.utilities.spark_utils import create_spark_session
spark = create_spark_session('SetupTest')
print(f'✓ Spark {spark.version} is working!')
spark.stop()
print('✓ Setup completed successfully!')
"

echo ""
echo "Setup complete! You can now:"
echo "  - Run 'make test' to execute tests"
echo "  - Run 'make jupyter' to start Jupyter"
echo "  - Run 'python src/jobs/word_count_job.py' to test a sample job"
