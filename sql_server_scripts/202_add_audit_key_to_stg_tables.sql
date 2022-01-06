USE UDEMY;
GO

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'stg.CourseLectures' ) and name = N'AuditKey'
	)
ALTER TABLE stg.CourseLectures ADD AuditKey INT NULL;

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'stg.CourseInstructors' ) and name = N'AuditKey'
	)
ALTER TABLE stg.CourseInstructors ADD AuditKey INT NULL;

IF NOT EXISTS (
	select name from sys.columns 
	where object_id = object_id(N'stg.CourseLectures' ) and name = N'AuditKey'
	)
ALTER TABLE stg.CourseLectures ADD AuditKey INT NULL;