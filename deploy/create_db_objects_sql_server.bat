:: While assigning values to the variables, don't add spaces before and after the equal '=' sign 
SET src_server=localhost
SET etl_server=localhost
SET trg_server=localhost

:: SET src_username=sa
:: SET src_password=P@ssword

:: Using Windows Authentication [-E] instead of SQL Server Authentication [-U -P]
:: sqlcmd -S %src_server% -U %src_username% -P %src_password% -i sql_server_scripts\101_create_db_and_schemas.sql

sqlcmd -S %src_server% -E -i deploy\sql_server_scripts\101_create_db_and_schemas.sql
sqlcmd -S %src_server% -E -i deploy\sql_server_scripts\102_create_stg_utilities.sql
sqlcmd -S %src_server% -E -i deploy\sql_server_scripts\201_create_stg_tables.sql
sqlcmd -S %src_server% -E -i deploy\sql_server_scripts\301_create_views_stg_to_dwh.sql
sqlcmd -S %trg_server% -E -i deploy\sql_server_scripts\401_create_dwh_tables.sql
sqlcmd -S %etl_server% -E -i deploy\sql_server_scripts\501_create_etl_audit_table.sql
sqlcmd -S %etl_server% -E -i deploy\sql_server_scripts\502_create_usp_audit_insert_update.sql
sqlcmd -S %trg_server% -E -i deploy\sql_server_scripts\402_add_audit_key_to_dwh_tables.sql
sqlcmd -S %src_server% -E -i deploy\sql_server_scripts\202_add_audit_key_to_stg_tables.sql

:: pause