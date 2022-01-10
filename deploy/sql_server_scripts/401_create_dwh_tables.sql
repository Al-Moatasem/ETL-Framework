USE [UDEMY]
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.DimCourse'))

-- DROP TABLE [dwh].[DimCourse]

CREATE TABLE [dwh].[DimCourse](
	[CourseKey] [int] NOT NULL PRIMARY KEY,
	[CourseID] [nvarchar](10) NOT NULL,
	[InstructorID] [nvarchar](75) NULL,
	[CourseName] [nvarchar](250) NULL,
	[Price] [numeric](6, 2) NULL,
	[CourseDescription] [nvarchar](250) NULL,
	[Language] [nvarchar](25) NULL,
	[LastUpdateDate] [date] NULL,
	[CourseCategory] [nvarchar](25) NULL,
	[CourseSubcategory] [nvarchar](75) NULL,
	[Topic] [nvarchar](75) NULL,
	[SectionsCount] [int] NULL,
	[LecturesCount] [int] NULL,
	[DurationInMinutes] [int] NULL
);
GO

-- ==========================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.DimInstructor'))

-- DROP TABLE [dwh].[DimInstructor]

CREATE TABLE [dwh].DimInstructor(
	[InstructorKey] [int] NOT NULL PRIMARY KEY,
	[InstructorID] [nvarchar](75) NULL,
	[InstructorName] [nvarchar](100) NULL,
	[InstructorRating] [decimal](4, 2) NULL,
	[CoursesCount] [int] NULL,
	[EnrolledStudents] [int] NULL,
	[ReviewsCount] [int] NULL
);
GO

-- ==========================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.FactCourseLecture'))

-- DROP TABLE [dwh].[FactCourseLecture]

CREATE TABLE [dwh].FactCourseLecture(
	[CourseLectureKey] [bigint] NOT NULL PRIMARY KEY,
	[CourseKey] [int] NULL,
	[LectureID] [int] NULL,
	[LectureNumber] [int] NULL,
	[SectionNumber] [int] NULL,
	[SectionTitle] [nvarchar](100) NULL,
	[SectionLectureNumber] [int] NULL,
	[LectureTitle] [nvarchar](100) NULL,
	[LectureType] [nvarchar](25) NULL,
	[LectureDuration] [nvarchar](15) NULL,
	[QuestionsCount] [int] NULL
);
GO

-- ==========================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE OBJECT_ID = OBJECT_ID('dwh.BridgeCourseInstructor'))

-- DROP TABLE [dwh].[BridgeCourseInstructor]

CREATE TABLE [dwh].BridgeCourseInstructor(
	[CourseInstructorKey] [bigint] NOT NULL PRIMARY KEY,
	[CourseKey] [int] NOT NULL,
	[InstructorKey] [int] NOT NULL,
	[CourseInstructorOrder] [int] NULL,
	[InstructorRank] [nvarchar](25) NULL
);
GO