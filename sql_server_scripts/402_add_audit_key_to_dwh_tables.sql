USE UDEMY;
GO

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'dwh.DimCourse' ) and name = N'AuditKey'
	)
ALTER TABLE dwh.DimCourse ADD AuditKey INT NOT NULL;

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'dwh.DimInstructor' ) and name = N'AuditKey'
	)
ALTER TABLE dwh.DimInstructor ADD AuditKey INT NOT NULL;

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'dwh.BridgeCourseInstructor' ) and name = N'AuditKey'
	)
ALTER TABLE dwh.BridgeCourseInstructor ADD AuditKey INT NOT NULL;

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'dwh.FactCourseLecture' ) and name = N'AuditKey'
	)
ALTER TABLE dwh.FactCourseLecture ADD AuditKey INT NOT NULL;
