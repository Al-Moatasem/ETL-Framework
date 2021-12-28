from datetime import datetime

def insert_audit_record(connection, parent_audit_key, etl_job_name, src_file_name, table_name):
    
    cnxn = connection.raw_connection()
    crsr = cnxn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    crsr.execute(
			f"""INSERT INTO etl.Audit ( ParentAuditKey, ETLJobName, FileName, TableName, StartDate )
				OUTPUT INSERTED.AuditKey
				VALUES
				( {parent_audit_key}, N'{etl_job_name}', N'{src_file_name}', N'{table_name}', '{now}' )"""
	)

    AuditKey = crsr.fetchone()[0]
    
    cnxn.commit()
    crsr.close()
    cnxn.close()

    return AuditKey

def Update_audit_record(connection, audit_key, initial_row_count = 'NULL', rows_extracted = 'NULL', rows_inserted = 'NULL', rows_updated = 'NULL', rows_rejected = 'NULL', final_row_count = 'NULL'):

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    connection.execute(
			f"""UPDATE etl.Audit
                SET 
                    InitialRowCount	= {initial_row_count}, 
                    RowsExtracted = {rows_extracted},
                    RowsInserted = {rows_inserted}, 
                    RowsUpdated = {rows_updated}, 
                    RowsRejected = {rows_rejected}, 
                    FinalRowCount = {final_row_count}, 
                    EndDate = '{now}', 
                    CompletedSuccessfully = 'Y'
                WHERE AuditKey = {audit_key}
            """
	)

def count_table_records(connection, schema_name, table_name):
    sql = f'SELECT COUNT(*) AS cnt FROM {schema_name}.{table_name}'
    return connection.execute(sql).fetchone()[0]
