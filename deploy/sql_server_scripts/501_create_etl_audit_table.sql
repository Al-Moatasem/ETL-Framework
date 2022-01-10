USE [UDEMY]
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('etl.Audit'))

-- DROP TABLE etl.Audit

CREATE TABLE etl.Audit(
	AuditKey INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
	ParentAuditKey INT NULL,
	ETLJobName NVARCHAR(255) NOT NULL,
	FileName NVARCHAR(255) NULL, -- While extracting data
	TableName NVARCHAR(255) NULL, -- target table
	StartDate DATETIME NOT NULL DEFAULT GETDATE(),
	EndDate DATETIME NULL,
	InitialRowCount INT NULL,
	RowsExtracted INT NULL,
	RowsInserted INT NULL,
	RowsUpdated INT NULL,
	RowsRejected INT NULL,
	FinalRowCount INT NULL,
	CompletedSuccessfully NCHAR(1) NOT NULL DEFAULT 'N'
);
GO