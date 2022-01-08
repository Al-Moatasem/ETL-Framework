def insert_audit_record(
    connection, etl_job_name, parent_audit_key=None, table_name=None, src_file_name=None
):

    cnxn = connection.connect()
    transaction = cnxn.begin()
    AuditKey = cnxn.execute(
        f"""
            DECLARE	@AuditKey int;
            EXEC	etl.InsertAuditRecord 
                @ParentAuditKey = ?, 
                @ETLJobName = ?,
                @FileName = ?, 
                @TableName = ?, 
                @AuditKey = @AuditKey OUTPUT
            """,
        (parent_audit_key, etl_job_name, src_file_name, table_name),
    ).fetchone()[0]
    transaction.commit()
    return AuditKey


def Update_audit_record(
    connection,
    audit_key,
    initial_row_count=None,
    rows_extracted=None,
    rows_inserted=None,
    rows_updated=None,
    rows_rejected=None,
    final_row_count=None,
):

    cnxn = connection.connect()
    transaction = cnxn.begin()
    cnxn.execute(
        f"""
        EXEC	[etl].[UpdateAuditRecord] 
            @AuditKey = ?, 
            @InitialRowCount = ?, 
            @RowsExtracted = ?,
		    @RowsInserted = ?, 
            @RowsUpdated = ?, 
            @RowsRejected = ?, 
            @FinalRowCount = ?
        """,
        (
            audit_key,
            initial_row_count,
            rows_extracted,
            rows_inserted,
            rows_updated,
            rows_rejected,
            final_row_count,
        ),
    )
    transaction.commit()


def count_table_records(connection, schema_name, table_name):
    sql = f"SELECT COUNT(*) AS cnt FROM {schema_name}.{table_name}"
    return connection.execute(sql).fetchone()[0]
