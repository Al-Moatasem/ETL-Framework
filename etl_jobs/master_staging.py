from .stg_read_csv_truncate_load_table import (
    read_file_load_db_table,
)

from util.etl_audit import insert_audit_record, Update_audit_record
from util.util import (
    archive_processed_file,
    read_csv_pd,
    connect_to_sqlserver_db_sqlalchemy,
    list_directory_files,
)
from util.log import log_msg


def execute_staging_master(
    etl_metadata, connection_info_staging, connection_info_etl, parent_audit_key
):

    log_msg("[ETL Job Start] - Starting of Staging Master")

    etl_metadata_stg = etl_metadata[
        (etl_metadata["enabled"] == 1) & (etl_metadata["layer"] == "staging")
    ]

    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    audit_key = insert_audit_record(
        cnxn_etl, f"Master Extract - Staging", parent_audit_key
    )

    for row in etl_metadata_stg.itertuples():
        path = row.path
        schema_target, table_name = row.table_name_target.split(".")
        files_mask = row.filter_files

        for idx, file in enumerate(list_directory_files(path, files_mask)):
            truncate_trg_table = True

            # Truncate target table with the first iteration only.
            if idx:
                truncate_trg_table = False

            read_file_load_db_table(
                file,
                schema_target,
                table_name,
                connection_info_staging,
                connection_info_etl,
                parent_audit_key,
                truncate_trg_table,
            )

            # Archiving processed files
            archiving_path = row.archive_path
            archive_processed_file(file, archiving_path, move_file=False)

    Update_audit_record(
        cnxn_etl,
        audit_key,
    )

    log_msg("[ETL Job End] - Ending of Staging Master")
