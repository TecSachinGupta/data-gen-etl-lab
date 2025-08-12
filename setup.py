from setuptools import setup, find_packages

setup(
    name="PyDataEngineerTemplate",
    version="0.1.0",
    description="PySpark utilities and job repository",
    author="Your Name",
    author_email="your.email@company.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.4.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "pyarrow>=14.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-spark>=0.6.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
        "ml": [
            "scikit-learn>=1.3.0",
            "mlflow>=2.8.0",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
