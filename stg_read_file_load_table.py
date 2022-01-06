from util import (
    list_directory_files,
    read_csv_pd,
    connect_to_sqlserver_db_sqlalchemy,
    sql_truncate_table,
    sql_insert_into,
    get_file_info,
)
from etl_audit import insert_audit_record, Update_audit_record, count_table_records


def read_file_load_db_table(
    src_file_path,
    schema_target,
    table_name,
    connection_info_target,
    connection_info_etl,
    truncate_trg_table=True,
):
    cnxn_target = connect_to_sqlserver_db_sqlalchemy(connection_info_target)
    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    file_name = get_file_info(src_file_path).get("file_name_ext")
    audit_key = insert_audit_record(
        cnxn_etl, f"stg loading {table_name}", -1, table_name, file_name
    )

    initial_row_count = count_table_records(cnxn_target, schema_target, table_name)

    if truncate_trg_table:
        print(f"Truncating {table_name}")
        # Truncating the destination table
        sql_truncate_table(schema_target, table_name, cnxn_target)

    df = read_csv_pd(src_file_path)
    rows_extracted = df.shape[0]

    # Inserting data into destination table
    print(f"Inserting into {table_name}, {file_name}")

    sql_insert_into(df, schema_target, table_name, cnxn_target)
    rows_inserted = df.shape[0]

    rows_rejected = df.shape[0]

    final_row_count = count_table_records(cnxn_target, schema_target, table_name)

    Update_audit_record(
        cnxn_etl,
        audit_key,
        initial_row_count=initial_row_count,
        rows_extracted=rows_extracted,
        rows_inserted=rows_inserted,
        # rows_updated = 0,
        rows_rejected=rows_rejected,
        final_row_count=final_row_count,
    )


def loop_dir_files_load_db_table(
    path,
    schema_target,
    table_name,
    connection_info_target,
    connection_info_etl,
    files_mask="*",
):
    for idx, file in enumerate(list_directory_files(path, files_mask)):
        truncate_trg_table = True

        # Truncate target table with the first iteration only.
        if idx:
            truncate_trg_table = False

        read_file_load_db_table(
            file,
            schema_target,
            table_name,
            connection_info_target,
            connection_info_etl,
            truncate_trg_table,
        )


if __name__ == "__main__":
    import json

    with open("config.json", "r") as config_file:
        config = json.load(config_file)
        connection_info_target = config["database"]["sqlserver_stg"]
        connection_info_etl = config["database"]["sqlserver_etl"]

    course_lectures = r"input_files\Course Outlines CSV"
    table_name = "CourseLectures"

    loop_dir_files_load_db_table(
        course_lectures,
        "stg",
        table_name,
        connection_info_target,
        connection_info_etl,
    )