from .dwh_truncate_load_table import dwh_truncate_and_load
from util.etl_audit import insert_audit_record, Update_audit_record
from util.util import connect_to_sqlserver_db_sqlalchemy
from util.log import log_msg


def execute_dwh_master(
    etl_metadata,
    connection_info_staging,
    connection_info_dwh,
    connection_info_etl,
    parent_audit_key,
):

    log_msg("[ETL Job Start] - Starting of data warehouse Master")

    etl_metadata_dwh = etl_metadata[
        (etl_metadata["enabled"] == 1) & (etl_metadata["layer"] == "data warehouse")
    ]

    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    audit_key = insert_audit_record(cnxn_etl, f"Master Load - DWH", parent_audit_key)

    for row in etl_metadata_dwh.itertuples():
        schema_stg, table_name = row.table_name_source.split(".")
        schema_dwh, table_name = row.table_name_target.split(".")

        dwh_truncate_and_load(
            table_name,
            schema_stg,
            schema_dwh,
            connection_info_staging,
            connection_info_dwh,
            connection_info_etl,
            parent_audit_key=audit_key,
        )

    Update_audit_record(
        cnxn_etl,
        audit_key,
    )

    log_msg("[ETL Job End] - Ending of data warehouse Master")
