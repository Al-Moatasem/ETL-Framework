from .master_staging import execute_staging_master
from .master_data_warehouse import execute_dwh_master

from util.etl_audit import insert_audit_record, Update_audit_record
from util.util import connect_to_sqlserver_db_sqlalchemy

from util.log import log_msg


def execute_master(
    etl_metadata,
    connection_info_staging,
    connection_info_dwh,
    connection_info_etl,
    execute_staging=True,
    execute_dwh=True,
):

    log_msg("[ETL Job Start] - Starting of Master Job")

    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    audit_key = insert_audit_record(cnxn_etl, f"Master Load")

    if execute_staging:
        execute_staging_master(
            etl_metadata,
            connection_info_staging,
            connection_info_etl,
            parent_audit_key=audit_key,
        )

    if execute_dwh:
        execute_dwh_master(
            etl_metadata,
            connection_info_staging,
            connection_info_dwh,
            connection_info_etl,
            parent_audit_key=audit_key,
        )

    Update_audit_record(
        cnxn_etl,
        audit_key,
    )

    log_msg("[ETL Job End] - Ending of Master Job")
