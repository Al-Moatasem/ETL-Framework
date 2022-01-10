from util.util import (
    list_directory_files,
    read_csv_pd,
    connect_to_sqlserver_db_sqlalchemy,
    sql_truncate_table,
    sql_insert_into,
    get_file_info,
)
from util.etl_audit import insert_audit_record, Update_audit_record, count_table_records
from util.log import log_msg


def read_file_load_db_table(
    src_file_path,
    schema_target,
    table_name,
    connection_info_target,
    connection_info_etl,
    parent_audit_key,
    truncate_trg_table=True,
):

    log_msg(
        f"[ETL Job Start] - STG extracting data from {src_file_path}, truncate and load {table_name}"
    )

    cnxn_target = connect_to_sqlserver_db_sqlalchemy(connection_info_target)
    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    file_name = get_file_info(src_file_path).get("file_name_ext")
    audit_key = insert_audit_record(
        cnxn_etl, f"stg loading {table_name}", parent_audit_key, table_name, file_name
    )

    log_msg(f"Get initial rows count for {table_name}")
    initial_row_count = count_table_records(cnxn_target, schema_target, table_name)

    if truncate_trg_table:
        # Truncating the destination table
        sql_truncate_table(schema_target, table_name, cnxn_target)

    df = read_csv_pd(src_file_path)
    rows_extracted = df.shape[0]

    # Inserting data into destination table
    sql_insert_into(df, schema_target, table_name, cnxn_target, audit_key)
    rows_inserted = df.shape[0]
    rows_rejected = df.shape[0]

    log_msg(f"Get final rows count for {table_name}")
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

    log_msg(
        f"[ETL Job End] - STG extracting data from {src_file_path}, truncate and load {table_name}"
    )


def loop_dir_files_load_db_table(
    path,
    schema_target,
    table_name,
    connection_info_target,
    connection_info_etl,
    parent_audit_key,
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
            parent_audit_key,
            truncate_trg_table,
        )
