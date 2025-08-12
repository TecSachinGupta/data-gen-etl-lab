# Pydataengineertemplate

A comprehensive repository for PySpark jobs, utilities, and notebooks.

## Quick Start

```bash
# Clone the repository
git clone <your-repo-url>
cd PyDataEngineerTemplate

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install as editable package
pip install -e .

# Run tests
make test

# Start Jupyter
make jupyter
```

## Repository Structure

### Overview

- `src/` - Source code for all PySpark jobs and utilities
- `notebooks/` - Jupyter notebooks for exploration and tutorials
- `tests/` - Comprehensive test suite
- `docs/` - Documentation and examples
- `assets/` - Sample data and schemas
- `templates/` - Reusable templates

### Comprehensive

```
.
├── .gitignore
├── README.md                    # Overview, setup instructions, examples
├── requirements.txt             # Python dependencies
├── setup.py                     # For installing as a package
├── Makefile                     # Common commands (test, lint, build)
├── .env.example                 # Environment variables template
├── pyproject.toml               # Modern Python project config (alternative to setup.py)
│
├── tests/
│   ├── unit/                    # Unit tests for small functions
│   ├── integration/             # Spark/cluster tests
│   └── fixtures/                # Test data & reusable objects
│
├── docs/
│   ├── api/                     # API reference (auto-generated)
│   ├── tutorials/               # How-to guides
│   └── examples/                # Example scripts with explanations
│
├── assets/
│   ├── data/
│   │   ├── sample/              # Sample datasets
│   │   └── schemas/             # JSON/Avro/Parquet schema files
│   └── images/                  # Diagrams, charts, visual assets
│
├── src/
│   ├── __init__.py
│   ├── schemas/                 # Data schemas for jobs/utilities
│   ├── utilities/
│   │   ├── transformations/     # Data transformation snippets
│   │   ├── io/                  # I/O operation snippets
│   │   ├── ml/                  # ML utility snippets
│   │   ├── spark_utils/         # Spark-specific utilities
│   │   └── common/              # General utility snippets
│   ├── configs/                 # YAML/JSON configs for jobs
│   ├── orchestration/
│   │   ├── airflow/             # Airflow DAGs
│   │   ├── prefect/             # Prefect flows (if used)
│   │   └── schedulers/          # Custom scheduling logic
│   ├── pipeline/                # Jobs py files
│   └── notebooks/               # All Jupyter notebooks
│       ├── exploratory/         # Data exploration notebooks
│       ├── tutorials/           # Educational notebooks
│       ├── examples/            # Standalone runnable notebooks
│       └── archived/            # Old/deprecated notebooks
│
└── templates/                   # Project/job templates
    ├── notebook_templates/      # Pre-built Jupyter templates
    ├── job_templates/           # Spark job templates
    └── config_templates/        # Sample configs
```

#### Directory Descriptions

##### Root Level Files
- **`.gitignore`** - Git ignore patterns for Python, Jupyter, and data files
- **`README.md`** - Project overview, setup instructions, and usage examples
- **`requirements.txt`** - Python package dependencies
- **`setup.py`** - Package installation configuration (traditional approach)
- **`pyproject.toml`** - Modern Python project configuration (PEP 518)
- **`Makefile`** - Common development commands (test, lint, build, deploy)
- **`.env.example`** - Template for environment variables

##### Testing Structure (`/tests/`)
- **`unit/`** - Fast, isolated unit tests for individual functions and classes
- **`integration/`** - End-to-end tests including Spark jobs and data pipelines
- **`fixtures/`** - Shared test data, mock objects, and reusable test components

##### Documentation (`/docs/`)
- **`api/`** - Auto-generated API documentation from docstrings
- **`tutorials/`** - Step-by-step guides and how-to documentation
- **`examples/`** - Complete example scripts with detailed explanations

##### Assets (`/assets/`)
- **`data/sample/`** - Sample datasets for testing and examples
- **`data/schemas/`** - JSON Schema, Avro, and Parquet schema definitions
- **`images/`** - Architecture diagrams, charts, and visual documentation

##### Source Code (`/src/`)
- **`schemas/`** - Data schema definitions for validation and documentation
- **`utilities/`** - Reusable utility modules organized by function:
  - **`transformations/`** - Data transformation functions and classes
  - **`io/`** - Input/output operations (CSV, Parquet, databases, APIs)
  - **`ml/`** - Machine learning utilities and model helpers
  - **`spark_utils/`** - Spark session management and configuration helpers
  - **`common/`** - General-purpose utilities (logging, configuration, etc.)
- **`configs/`** - YAML/JSON configuration files for different environments
- **`orchestration/`** - Workflow orchestration code:
  - **`airflow/`** - Apache Airflow DAG definitions
  - **`prefect/`** - Prefect workflow definitions (if used)
  - **`schedulers/`** - Custom scheduling and orchestration logic
- **`pipeline/`** - Main pipeline and job Python files
- **`notebooks/`** - Jupyter notebooks organized by purpose:
  - **`exploratory/`** - Data exploration and analysis notebooks
  - **`tutorials/`** - Educational notebooks explaining concepts
  - **`examples/`** - Standalone runnable example notebooks
  - **`archived/`** - Deprecated or old notebooks kept for reference

##### Templates (`/templates/`)
- **`notebook_templates/`** - Standardized Jupyter notebook templates
- **`job_templates/`** - Spark job and pipeline templates
- **`config_templates/`** - Sample configuration files for different scenarios

## Build System Integration

This structure is fully supported by the comprehensive build system with the following capabilities:

### Automated Processing
- **Code Validation**: Syntax checking, linting, and type checking
- **Testing**: Unit and integration test execution with coverage reporting
- **Documentation**: Auto-generation of API docs and tutorial processing
- **Asset Management**: Schema validation and data file packaging
- **Notebook Processing**: Validation, execution, and format conversion

### Quality Assurance
- **Code Formatting**: Black, isort, and flake8 integration
- **Security Scanning**: Bandit and safety checks
- **Dependency Management**: Requirements validation and updates
- **Configuration Validation**: YAML/JSON syntax and schema checking

### Deployment Support
- **Package Building**: Python wheel and source distribution creation
- **Docker Integration**: Containerized deployment support
- **Spark Job Packaging**: Cluster-ready job packaging
- **Asset Bundling**: Complete project artifact creation

## Usage with Build System

```bash
# Full project build
python build.py full
make build

# Test specific directories
make test-unit          # tests/unit/
make test-integration   # tests/integration/

# Process specific components
python build.py assets  # assets/ directory
python build.py docs    # docs/ directory
python build.py package # src/ packaging
```

This structure provides a scalable foundation for data engineering projects with clear separation of concerns and comprehensive tooling support.

## Usage Examples

```python
from src.utilities.spark_utils import create_spark_session
from src.utilities.transformations import clean_data

# Create Spark session
spark = create_spark_session("MyApp")

# Load and clean data
df = spark.read.parquet("data/input.parquet")
clean_df = clean_data(df)
```
