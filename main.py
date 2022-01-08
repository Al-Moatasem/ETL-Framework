from stg_read_file_load_table import loop_dir_files_load_db_table
from dwh_truncate_load_table import dwh_truncate_and_load
from etl_audit import insert_audit_record, Update_audit_record
from util import read_csv_pd, connect_to_sqlserver_db_sqlalchemy


def execute_staging_master(etl_metadata, connection_info_etl, parent_audit_key):

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

        loop_dir_files_load_db_table(
            path,
            schema_target,
            table_name,
            connection_info_staging,
            connection_info_etl,
            parent_audit_key=audit_key,
            files_mask=files_mask,
        )

    Update_audit_record(
        cnxn_etl,
        audit_key,
    )


def execute_dwh_master(etl_metadata, connection_info_etl, parent_audit_key):

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


def execute_master(
    etl_metadata,
    connection_info_etl,
    execute_staging=True,
    execute_dwh=True,
):

    cnxn_etl = connect_to_sqlserver_db_sqlalchemy(connection_info_etl)

    audit_key = insert_audit_record(cnxn_etl, f"Master Load")

    if execute_staging:
        execute_staging_master(
            etl_metadata, connection_info_etl, parent_audit_key=audit_key
        )

    if execute_dwh:
        execute_dwh_master(
            etl_metadata, connection_info_etl, parent_audit_key=audit_key
        )

    Update_audit_record(
        cnxn_etl,
        audit_key,
    )


if __name__ == "__main__":
    import json

    with open("config.json", "r") as config_file:
        config = json.load(config_file)
        connection_info_staging = config["database"]["sqlserver_stg"]
        connection_info_dwh = config["database"]["sqlserver_dw"]
        connection_info_etl = config["database"]["sqlserver_etl"]

    etl_metadata = read_csv_pd("etl_metadata.csv")

    load_stg = 1
    load_dwh = 1

    execute_master(
        etl_metadata,
        connection_info_etl,
        execute_staging=load_stg,
        execute_dwh=load_dwh,
    )
