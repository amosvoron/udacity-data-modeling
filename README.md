# Data Modeling with PostgreSQL
This is a data modeling with PostgreSQL project for the Udacity Data Engineering Nanodegree program.

## Project Overview
The data modeling is the foundation of database quality and proper data handling in organized manner. In this project we'll define fact and dimensional tables for a star schema using data from the JSON data source. We'll build an Extract-Transform-Load (ETL) pipeline which will perform all necessary steps from extracting data from JSON files, process dimensional and fact data with corresponding relations, and finally load data into target tables.   

## Installation
### Clone
```sh
$ git clone https://github.com/amosvoron/udacity-data-modeling.git
```

### Instructions
To create database and tables run the following command:
```sh
$ python create_tables.py
```

To execute ETL process run the following command:
```sh
$ python etl.py <data_dir>
```

where \<data_dir\> is a path of the JSON data directory.

## Repository Description

```sh
- data/                              # directory with JSON data files                    
- create_tables.py                   # python script for database and table creation 
- etl.ipynb                          # jupyter notebook with ETL processing code 
- etl.py                             # python script for ETL processing
- sql_queries.py                     # python script with SQL code for table creation, inserts, and checks
- test.ipynb                         # python code for checking the database content
- README.md                          # README file
- LICENCE.md                         # LICENCE file
```

## License

MIT