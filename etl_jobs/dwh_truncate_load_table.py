from util.util import (
    connect_to_sqlserver_db_sqlalchemy,
    sql_truncate_table,
    sql_select_from,
    sql_insert_into,
)
from util.etl_audit import insert_audit_record, Update_audit_record, count_table_records
from util.log import log_msg


def dwh_truncate_and_load(
    table_name,
    schema_source,
    schema_target,
    connection_info_source,
    connection_info_target,
    connection_info_etl,
    parent_audit_key,
):

    log_msg(f"[ETL Job Start] - DWH truncate and load {table_name}")

    cnxn_stg = connect_to_sqlserver_db_sqlalchemy(connection_info_source)
    cnxn_target = connect_to_sqlserver_db_sqlalchemy(connection_info_target)
    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    audit_key = insert_audit_record(
        cnxn_etl, f"dwh loading {table_name}", parent_audit_key, table_name
    )

    initial_row_count = count_table_records(cnxn_target, schema_target, table_name)

    # Truncating the destination table
    sql_truncate_table(schema_target, table_name, cnxn_target)

    # Selecting data from source table
    src_dataframe = sql_select_from(schema_source, table_name, cnxn_stg)
    rows_extracted = src_dataframe.shape[0]

    # Inserting data into destination table
    rows_inserted = None
    rows_rejected = None

    try:
        sql_insert_into(
            src_dataframe, schema_target, table_name, cnxn_target, audit_key
        )
        rows_inserted = src_dataframe.shape[0]
    except:
        rows_rejected = src_dataframe.shape[0]

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

    log_msg(f"[ETL Job End] - DWH truncate and load {table_name}")
