:: While assigning values to the variables, don't add spaces before and after the equal '=' sign
:: Password is stored in pgpass.conf file.

SET src_server=localhost
SET src_server_port=5432
SET src_database=udemy
SET src_username=postgres

SET trg_server=localhost
SET trg_server_port=5432
SET trg_database=udemy
SET trg_username=postgres



psql -h %src_server% -U %src_username% -p %src_server_port% -d %src_database% -f .\postgres_scripts\102_create_schemas.sql
psql -h %src_server% -U %src_username% -p %src_server_port% -d %src_database% -f .\postgres_scripts\201_create_stg_tables.sql
pause