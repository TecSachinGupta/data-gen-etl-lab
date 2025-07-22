# data-gen-etl-lab
> Repository to have the data genrator scripts as well as jyupter notbooks for python and pyspark.


### Structure
```
DATA-GEN-ETL-LAB
│   .gitignore
│   LICENSE
│   README.md
│   requirement.txt
│
├───assets
│   ├───data
│   │   └───raw
│   │           Army_01.json
│   │           Army_02.json
│   │           Orders_01.csv
│   │           Orders_02.csv
│   │           Orders_11.json
│   │           Orders_12.json
│   │           Orders_13.json
│   │           Persons_01.json
│   │           Persons_02.json
│   │           Products_01.json
│   │           Products_02.json
│   │           Sellers_01.csv
│   │           Sellers_02.csv
│   │
│   ├───images
│   │       generator output 01.png
│   │       generator output 02.png
│   │       generator output 03.png
│   │
│   └───libs
│           azure-storage-8.6.6.jar
│           hadoop-azure-3.4.1.jar
│           hadoop-bare-naked-local-fs-0.1.0.jar
│           mysql-connector-j-9.4.0.jar
│           postgresql-42.7.7.jar
│
├───docs
│       Azure Data Factory.ipynb
│       Exercise.md
│       Setup ADLS, Keyvault, Secret Scope.md
│       Spark Overview.ipynb
│
└───src
    ├───notebooks
    │   │   01. Python Tutorial.ipynb
    │   │   02. Python Exercises.ipynb
    │   │   03. PySpark Tutorial.ipynb
    │   │   04. PySpark Exercise.ipynb
    │   │   05. DeltaLake Table.ipynb
    │   │   11. students_etl.ipynb
    │   │   13. ecommerce_etl.ipynb
    │   │   15. postgres_etl.ipynb
    │   │
    │   └───artifacts
    │
    └───scripts
            army_etl.py
            configs.py
            data_analysis_pandas.py
            generate_fake_army_data.py
            generate_fake_ecommerce_data.py
            generate_fake_smart_city_data.py
            main.py
            utililties.py
```

