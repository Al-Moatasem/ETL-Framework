use UDEMY;
go

if exists (select 1 from sys.tables where object_id = object_id(N'stg.CourseBasicInfo') and type = 'U' )
drop table stg.CourseBasicInfo;

create table stg.CourseBasicInfo(
	course_id nvarchar(4000),
	name nvarchar(4000),
	active_course nvarchar(4000),
	category nvarchar(4000),
	instractor nvarchar(4000),
	original_price nvarchar(4000),
	url nvarchar(4000),
	short_description nvarchar(4000),
	sections_count nvarchar(4000),
	lectures_count nvarchar(4000),
	duration_str nvarchar(4000),
	language nvarchar(4000),
	last_update_date nvarchar(4000),
	caourse_rating nvarchar(4000),
	rating_count nvarchar(4000),
	best_rating nvarchar(4000),
	worst_rating nvarchar(4000),
	position_1 nvarchar(4000),
	position_2 nvarchar(4000),
	position_3 nvarchar(4000),
	enrollment nvarchar(4000)
);
go

if exists (select 1 from sys.tables where object_id = object_id(N'stg.CourseInstructors') and type = 'U' )
drop table stg.CourseInstructors;

create table stg.CourseInstructors(
	course_id nvarchar(4000),
	instractor_name nvarchar(4000),
	instractor_id nvarchar(4000),
	instractor_order nvarchar(4000),
	instractor_rating nvarchar(4000),
	instractor_reviews nvarchar(4000),
	instractor_students nvarchar(4000),
	instractor_courses_count nvarchar(4000)
);
go


if exists (select 1 from sys.tables where object_id = object_id(N'stg.CourseLectures') and type = 'U' )
drop table stg.CourseLectures;

create table stg.CourseLectures(
	course_id nvarchar(4000),
	lecture_id nvarchar(4000),
	lecture_index_global nvarchar(4000),
	lecture_title nvarchar(4000),
	lecture_duration nvarchar(4000),
	lecture_type nvarchar(4000),
	lecture_index_section nvarchar(4000),
	section_title nvarchar(4000),
	section_index nvarchar(4000)
);
go