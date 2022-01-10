USE UDEMY;
go

-- ==========================================================

IF EXISTS (SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(N'stg.DimInstructor') )
DROP VIEW stg.DimInstructor;
go

CREATE VIEW stg.DimInstructor AS
SELECT
	CAST(ROW_NUMBER() OVER(ORDER BY InstructorID) as INT) AS InstructorKey,
	*
FROM (
	SELECT 
	
		CAST(instractor_id AS NVARCHAR(75)) AS InstructorID,
		-- max length in the sample data is 52
		CAST(instractor_name AS NVARCHAR(100)) AS InstructorName,
		-- TODO: These columns should be stored in a separate table, as they are considered fast changing dimensions
		CAST( MAX(
			CASE WHEN instractor_name <> N'Anonymized User' THEN [instractor_rating] END) 
			AS DECIMAL(4,2)) AS InstructorRating,
		CAST( MAX( 
			CASE WHEN instractor_name <> N'Anonymized User' THEN [instractor_courses_count] END 
			) AS INT) AS CoursesCount,
		-- Sample value 566,079
		MAX( 
			CASE WHEN instractor_name <> N'Anonymized User' THEN TRY_PARSE([instractor_students] AS INT) END
			) AS EnrolledStudents,
		MAX(
			CASE WHEN instractor_name <> N'Anonymized User' THEN TRY_PARSE([instractor_reviews] AS INT) END
			) AS ReviewsCount
	FROM stg.CourseInstructors
	GROUP BY instractor_id, instractor_name
) AS qry;
go


-- ==========================================================

IF EXISTS (SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(N'stg.DimCourse') )
DROP VIEW stg.DimCourse;
go

CREATE VIEW stg.DimCourse AS
SELECT
	CAST(ROW_NUMBER() OVER(ORDER BY CourseID ) as INT) AS CourseKey,
	*
FROM (
	SELECT DISTINCT 
		CAST([course_id] AS NVARCHAR(10)) AS CourseID,
		CAST([instractor] AS NVARCHAR(75)) AS InstructorID,
		CAST([name] AS NVARCHAR(250))as CourseName,
		CASE WHEN [original_price] = N'Free' THEN 0
		ELSE TRY_PARSE(
				REPLACE([original_price], N'$', N'' ) AS DECIMAL(6,2) USING 'en-US')
		END as Price,
		-- as per the sample data, the max length is 120
		CAST([short_description] AS NVARCHAR(250)) AS CourseDescription,
		CAST([language] AS NVARCHAR(25)) AS [Language],
		TRY_PARSE('01/'+[last_update_date] AS DATE USING 'en-GB') AS LastUpdateDate,
		CAST([position_1] AS NVARCHAR(25)) AS CourseCategory,
		CAST([position_2] AS NVARCHAR(75)) AS CourseSubcategory,
		CAST([position_3] AS NVARCHAR(75)) AS Topic,
		CAST([sections_count] AS INT) AS SectionsCount,
		CAST([lectures_count] AS INT) AS LecturesCount,
		stg.ConvertDurationToMinutes([duration_str]) as DurationInMinutes
	FROM stg.CourseBasicInfo
) AS qry;
go

-- ==========================================================

IF EXISTS (SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(N'stg.FactCourseLecture') )
DROP VIEW stg.FactCourseLecture;
go

CREATE VIEW stg.FactCourseLecture AS
SELECT
	CAST(ROW_NUMBER() OVER(ORDER BY CourseKey, LectureID) as INT) AS CourseLectureKey,
	*
FROM (
	SELECT DISTINCT
		COALESCE(dc.CourseKey, -1) AS CourseKey,
		CAST([lecture_id] AS INT) AS LectureID,
		CAST([lecture_index_global] AS INT) AS LectureNumber,
		CAST([section_index] AS INT) AS SectionNumber,
		-- max length in the sample data is 80
		CAST([section_title] AS NVARCHAR(100)) AS SectionTitle,
		CAST([lecture_index_section] AS INT) AS SectionLectureNumber,
		-- max length in the sample data is 80
		CAST([lecture_title] AS NVARCHAR(100)) AS LectureTitle,
		CAST([lecture_type] AS NVARCHAR(25)) AS LectureType,
		-- TODO: Needs to be converted to number of seconds
		-- max length in the sample data is 13
		CAST(CASE WHEN [lecture_type] = N'lecture' THEN [lecture_duration] END AS NVARCHAR(15)) AS LectureDuration,
		CASE WHEN [lecture_type] IN ( N'quiz', N'practice')
			 THEN TRY_PARSE(LEFT( [lecture_duration],  CHARINDEX(N' ', [lecture_duration]) - 1) as INT)
		--ELSE 0
		END AS QuestionsCount
	FROM stg.CourseLectures AS fct
		LEFT JOIN stg.DimCourse AS dc ON fct.course_id = dc.CourseID
) AS qry;
GO

-- ==========================================================

IF EXISTS (SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(N'stg.BridgeCourseInstructor') )
DROP VIEW stg.BridgeCourseInstructor;
go


CREATE VIEW stg.BridgeCourseInstructor AS
SELECT
	ROW_NUMBER() OVER(ORDER BY CourseKey, InstructorKey ) AS CourseInstructorKey,
	*
FROM (
	SELECT DISTINCT 
		COALESCE(dc.CourseKey, -1) AS CourseKey,
		COALESCE(di.InstructorKey, -1) AS InstructorKey,
		-- order = 1 means this is the main primary instructor
		CAST( [instractor_order] AS INT )AS CourseInstructorOrder,
		CAST(
			CASE 
				WHEN [instractor_order] = 1 THEN N'Primary' 
			ELSE N'Secondary' 
			END 
			AS NVARCHAR(25)) AS InstructorRank
	FROM [stg].[CourseInstructors] AS fct
		LEFT JOIN stg.DimCourse AS dc ON fct.course_id = dc.CourseID
		LEFT JOIN stg.DimInstructor AS di ON fct.instractor_id = di.InstructorID
) AS qry;
GO
