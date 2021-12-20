import json
from util import connect_to_sqlserver_db_sqlalchemy, sql_truncate_table, sql_select_from, sql_insert_into

def dwh_truncate_and_load(table_name, schema_source, schema_target, connection_info_source, connection_info_target):
    
    print(f' {table_name} '.center(50,"-"))
    stg_cnxn = connect_to_sqlserver_db_sqlalchemy(connection_info_source)
    dwh_cnxn = connect_to_sqlserver_db_sqlalchemy(connection_info_target)

    print(f'Truncating {table_name}')
    # Truncating the destination table
    sql_truncate_table(schema_target, table_name, dwh_cnxn)
    
    # Selecting data from source table
    print(f'Selecting from {table_name}')
    src_dataframe = sql_select_from(schema_source, table_name, stg_cnxn)

    # Inserting data into destination table
    print(f'Inserting into {table_name}')
    sql_insert_into(src_dataframe, schema_target, table_name, dwh_cnxn)
    
    print(f' End of processing {table_name} '.center(50,"-"))

if __name__ == '__main__':
    
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
        stg_connection_info = config['database']['sqlserver_stg']
        dwh_connection_info = config['database']['sqlserver_dw']

    tables = ["DimCourse",  "DimInstructor","FactCourseLecture", "BridgeCourseInstructor"]

    for table_name in tables:
        dwh_truncate_and_load(table_name, 'stg', 'dwh', stg_connection_info, dwh_connection_info)