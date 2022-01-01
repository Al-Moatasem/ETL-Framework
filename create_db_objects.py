import os
import json

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    src_connection_info = config["database"]["sqlserver_stg"]
    src_server = src_connection_info["server"]

    trg_connection_info = config["database"]["sqlserver_dw"]
    trg_server = trg_connection_info["server"]

    etl_connection_info = config["database"]["sqlserver_etl"]
    etl_server = etl_connection_info["server"]

# sqlcmd doc https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility?redirectedfrom=MSDN&view=sql-server-ver15

# same commands are stored on a batch file
# os.system(fr'create_db_objects_sql_server.bat')

# sqlcmd -S {src_server} -U {src_username} -P {src_password} -i sql_file.sql
os.system(
    rf"sqlcmd -S {src_server} -E -i sql_server_scripts\101_create_db_and_schemas.sql"
)
os.system(
    rf"sqlcmd -S {src_server} -E -i sql_server_scripts\102_create_stg_utilities.sql"
)
os.system(rf"sqlcmd -S {src_server} -E -i sql_server_scripts\201_create_stg_tables.sql")
os.system(
    rf"sqlcmd -S {src_server} -E -i sql_server_scripts\301_create_views_stg_to_dwh.sql"
)
os.system(rf"sqlcmd -S {trg_server} -E -i sql_server_scripts\401_create_dwh_tables.sql")
os.system(
    rf"sqlcmd -S {etl_server} -E -i sql_server_scripts\501_create_etl_audit_table.sql"
)
os.system(
    rf"sqlcmd -S {etl_server} -E -i sql_server_scripts\502_create_usp_audit_insert_update.sql"
)
os.system(
    rf"sqlcmd -S {etl_server} -E -i sql_server_scripts\503_Add_AuditKey_to_dwh_tables.sql"
)