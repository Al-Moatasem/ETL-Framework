# ETL Framework

This is an experimental project for building an ETL framework using **Python**. It uses data scraped from random courses published on **UDEMY**. The development process will be organized into different phases, the first phase is a Proof of Concept, its objective is to build a simple ETL pipeline, the next phases will build on this POC trying to improve the framework.



## Phase One - Proof of Concept

In this phase, I am aiming to build **simple Truncate and Load** ETL pipelines, which copy the data from multiple csv files into staging tables, then transform this data into a dimensional model.



### Data Sources

This project uses data scraped from random courses published on **UDEMY**, the data itself are categorized into three sections as follows:
1. courses basic info (single file).
2. course instructors (single file).
3. courses outlines (single file per course).



### How to Use?

1. Create the database objects -SQL Server at the moment- using either the batch file `create_db_objects_sql_server.bat` or by executing the python file `create_db_objects.py`, these objects are:
   1. The database itself `UDEMY` , and  multiple schemas `stg` and `dwh`.
   2. Staging tables, single table per data source section.
   3. Views which will mimic the structure of data warehouse tables, they act as the **transformation** layer (business logic, and data cleansing).
   4. Data Warehouse tables.
   5. Utilities.
   
2. Rename `config-sample.json` to `config.json`, and `etl_metadata_sample.csv` to `etl_metadata.csv`, then adjust the values in these files to match your environment.

3. Install required Python's modules

   ```bash
   pip install -r requirements.txt
   ```

3. Execute `main.py`, which will execute the ETL jobs.
   1. In `main.py` there are two Boolean variables `load_stg` and `load_dwh`, which control which part of the data pipeline is executed.



### Data Cleansing

1. There are **duplicate** courses in both basic info and lectures tables. Load the unique courses into the dimensional model.
   1. This process could be implemented either while loading the staging tables, or while loading the dimensional model; currently, the application uses the second option.
2. There is numeric data stored in the source files as string values e.g. `123,456.78`, `$123.45`, parse these data as numeric while loading into the dimensional model.