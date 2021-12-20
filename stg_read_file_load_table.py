from util import list_directory_files, read_csv_pd, connect_to_sqlserver_db_sqlalchemy, sql_truncate_table, sql_insert_into
import json


def read_file_load_db_table(path, schema_target, table_name, connection_info_target, mask = '*'):
    cnxn_target = connect_to_sqlserver_db_sqlalchemy(connection_info_target)

    print(f'Truncating {table_name}')
    # Truncating the destination table
    sql_truncate_table(schema_target, table_name, cnxn_target)
    
    for file in list_directory_files(path, mask):
        df = read_csv_pd(file)
        
        # Inserting data into destination table
        print(f'Inserting into {table_name}, {file}')
        sql_insert_into(df, schema_target, table_name, cnxn_target)


if __name__ == '__main__':
    
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
        connection_info_target = config['database']['sqlserver_stg']

    course_lectures = r'input_files\Course Outlines CSV'
    table_name = 'CourseLectures'

    read_file_load_db_table(course_lectures, 'stg', table_name, connection_info_target)
