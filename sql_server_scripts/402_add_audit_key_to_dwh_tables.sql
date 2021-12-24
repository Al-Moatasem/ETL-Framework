USE [UDEMY]
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.DimCourse'))
ALTER TABLE dwh.DimCourse ADD AuditKey INT NOT NULL
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.DimInstructor'))
ALTER TABLE dwh.DimInstructor ADD AuditKey INT NOT NULL
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.FactCourseLecture'))
ALTER TABLE dwh.FactCourseLecture ADD AuditKey INT NOT NULL
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.BridgeCourseInstructor'))
ALTER TABLE dwh.BridgeCourseInstructor ADD AuditKey INT NOT NULL
GO
