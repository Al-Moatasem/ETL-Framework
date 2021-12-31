USE [UDEMY]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE OBJECT_ID = OBJECT_ID(N'etl.InsertAuditRecord'))
DROP PROCEDURE etl.InsertAuditRecord;
GO

CREATE PROCEDURE [etl].[InsertAuditRecord] 
	@ParentAuditKey INT, 
	@ETLJobName NVARCHAR(255),
	@FileName NVARCHAR(255),
	@TableName NVARCHAR(255),
	@AuditKey INT OUTPUT
AS
/* Example:
DECLARE	@AuditKey int;
EXEC	etl.InsertAuditRecord @ParentAuditKey = -99, @ETLJobName = N'stg_extract_dim_xyz',
		@FileName = N'file_name_abc.csv', @TableName = N'DimXyz', @AuditKey = @AuditKey OUTPUT
*/
BEGIN
	DECLARE @StartDate DATETIME = GETDATE();

	INSERT INTO etl.Audit (
		ParentAuditKey, 
		ETLJobName, 
		FileName, 
		TableName, 
		StartDate
			)
	OUTPUT INSERTED.AuditKey
	VALUES (
		@ParentAuditKey,
		@ETLJobName,
		@FileName,
		@TableName,
		@StartDate
		)
END
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE OBJECT_ID = OBJECT_ID(N'etl.UpdateAuditRecord'))
DROP PROCEDURE etl.UpdateAuditRecord;
GO

CREATE PROCEDURE [etl].[UpdateAuditRecord]
	@AuditKey INT, 
	@InitialRowCount INT,
	@RowsExtracted INT,
	@RowsInserted INT, 
	@RowsUpdated INT,
	@RowsRejected INT,
	@FinalRowCount INT
AS
/* Example
EXEC	[etl].[UpdateAuditRecord] @AuditKey = 40, @InitialRowCount = 1, @RowsExtracted = 2,
		@RowsInserted = 3, @RowsUpdated = 4, @RowsRejected = 5, @FinalRowCount = 6
*/
BEGIN
	DECLARE @EndDate DATETIME = GETDATE();
	UPDATE etl.Audit
	SET
		EndDate = @EndDate, 
		InitialRowCount = @InitialRowCount, 
		RowsExtracted = @RowsExtracted, 
		RowsInserted = @RowsInserted, 
		RowsUpdated = @RowsUpdated, 
		RowsRejected = @RowsRejected, 
		FinalRowCount = @FinalRowCount, 
		CompletedSuccessfully = 'Y'
	WHERE AuditKey = @AuditKey
END
