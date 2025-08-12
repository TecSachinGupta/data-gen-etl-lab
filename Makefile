.PHONY: install test lint format clean jupyter docker-build

# Install dependencies
install:
	pip install -r requirements.txt
	pip install -e .

# Run tests
test:
	pytest tests/ -v

# Run specific test
test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

# Code formatting
format:
	black src/ tests/
	
# Linting
lint:
	flake8 src/ tests/
	black --check src/ tests/

# Start Jupyter
jupyter:
	jupyter lab src/notebooks/

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

# Build package
build:
	python -m build

# Run example job
run-example:
	python src/jobs/word_count_job.py

# Setup pre-commit hooks
setup-hooks:
	pre-commit install
