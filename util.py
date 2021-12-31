import os
from datetime import datetime
from fnmatch import fnmatch
import pandas as pd
from sqlalchemy import create_engine


def connect_to_sqlserver_db_sqlalchemy(connection_info):
    username = connection_info["username"]
    password = connection_info["password"]
    server = connection_info["server"]
    database = connection_info["database"]
    driver = connection_info["driver"]

    cnxn_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
    )
    engine = create_engine(cnxn_string, fast_executemany=True)

    return engine


def sql_truncate_table(schema, table_name, connection):
    connection.execution_options(autocommit=True).execute(
        f"TRUNCATE TABLE {schema}.{table_name}"
    )


def sql_select_from(schema, table_name, connection):
    qry = f"SELECT * FROM {schema}.{table_name}"
    df = pd.read_sql_query(qry, connection)
    return df


def sql_insert_into(dataframe, schema, table_name, connection, chunk_size=10000):
    dataframe.to_sql(
        name=table_name,
        con=connection,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
    )


def list_directory_files(path, mask="*"):
    files = []

    if os.path.isfile(path):
        files.append(path)
        return files

    if os.path.isdir(path):
        for file in os.listdir(path):
            if fnmatch(file, mask):
                files.append(os.path.join(path, file))
        return files


def read_csv_pd(file_path):
    return pd.read_csv(file_path)


def get_file_info(file_path):
    file_info = {}
    if os.path.isfile(file_path):
        folder_path, file_name_ext = os.path.split(file_path)
        file_name, file_extension = os.path.splitext(file_name_ext)
        created_date_os = datetime.fromtimestamp(os.path.getctime(file_path))
        created_date_os = datetime.strftime(created_date_os, "%Y-%m-%d %H:%M:%S")
        created_date = datetime.strptime(file_name.split("_")[-1], "%Y%m%d%H%M%S")
        created_date = datetime.strftime(created_date, "%Y-%m-%d %H:%M:%S")

        file_info["folder_path"] = folder_path
        file_info["file_name_ext"] = file_name_ext
        file_info["file_name"] = file_name
        file_info["file_extension"] = file_extension
        file_info["created_date_os"] = created_date_os
        file_info["created_date"] = created_date

    return file_info
