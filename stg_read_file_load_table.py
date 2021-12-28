from util import list_directory_files, read_csv_pd, connect_to_sqlserver_db_sqlalchemy, sql_truncate_table, sql_insert_into
from etl_audit import insert_audit_record, Update_audit_record, count_table_records
import json


def read_file_load_db_table(path, schema_target, table_name, connection_info_target, connection_info_etl, mask = '*'):
    cnxn_target = connect_to_sqlserver_db_sqlalchemy(connection_info_target)
    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    audit_key = insert_audit_record(cnxn_etl, -1, f'stg loading {table_name}', path, table_name)

    initial_row_count = count_table_records(cnxn_target, schema_target, table_name)

    print(f'Truncating {table_name}')
    # Truncating the destination table
    sql_truncate_table(schema_target, table_name, cnxn_target)
    
    rows_extracted = 0
    rows_inserted = 0
    rows_rejected = 0

    for file in list_directory_files(path, mask):
        df = read_csv_pd(file)
        rows_extracted += df.shape[0]

        # Inserting data into destination table
        print(f'Inserting into {table_name}, {file}')
        try:
            sql_insert_into(df, schema_target, table_name, cnxn_target)
            rows_inserted += df.shape[0]
        except:
            rows_rejected += df.shape[0]

    final_row_count = count_table_records(cnxn_target, schema_target, table_name)

    Update_audit_record(
    cnxn_etl, audit_key,
    initial_row_count = initial_row_count,
    rows_extracted = rows_extracted,
    rows_inserted = rows_inserted, 
    # rows_updated = 0, 
    rows_rejected = rows_rejected, 
    final_row_count = final_row_count
    )


if __name__ == '__main__':
    
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
        connection_info_target = config['database']['sqlserver_stg']
        connection_info_etl = config['database']['sqlserver_etl']

    course_lectures = r'input_files\Course Outlines CSV'
    table_name = 'CourseLectures'

    read_file_load_db_table(course_lectures, 'stg', table_name, connection_info_target, connection_info_etl)
