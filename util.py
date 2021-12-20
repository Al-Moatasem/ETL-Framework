import os
from fnmatch import fnmatch
import pandas as pd
from sqlalchemy import create_engine

def connect_to_sqlserver_db_sqlalchemy(connection_info):
    username = connection_info['username']
    password = connection_info['password']
    server = connection_info['server']
    database = connection_info['database']
    driver = connection_info['driver']
    
    cnxn_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
    engine = create_engine(cnxn_string, fast_executemany=True)
    cnxn = engine.connect()

    return cnxn

def sql_truncate_table(schema, table_name, connection):
    connection.execution_options(autocommit=True).execute(f'TRUNCATE TABLE {schema}.{table_name}')

def sql_select_from(schema, table_name, connection):
    qry = f'SELECT * FROM {schema}.{table_name}'
    df = pd.read_sql_query(qry, connection)
    return df

def sql_insert_into(dataframe, schema, table_name, connection, chunk_size = 10000):
    dataframe.to_sql(
            name = table_name, 
            con = connection, 
            schema=schema, 
            if_exists='append', 
            index=False,
            chunksize=chunk_size
            )

def list_directory_files(path, mask='*'):
    files = []

    if os.path.isfile(path):
        files.append(path)
        return files

    if os.path.isdir(path):
        for file in os.listdir(path):
            if fnmatch(file, mask):
                files.append(os.path.join(path,file))
        return files

def read_csv_pd(file_path):
    return pd.read_csv(file_path)
