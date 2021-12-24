CREATE TABLE IF NOT EXISTS stg.CourseBasicInfo(
	course_id character varying(4000),
	name character varying(4000),
	active_course character varying(4000),
	category character varying(4000),
	instractor character varying(4000),
	original_price character varying(4000),
	url character varying(4000),
	short_description character varying(4000),
	sections_count character varying(4000),
	lectures_count character varying(4000),
	duration_str character varying(4000),
	language character varying(4000),
	last_update_date character varying(4000),
	caourse_rating character varying(4000),
	rating_count character varying(4000),
	best_rating character varying(4000),
	worst_rating character varying(4000),
	position_1 character varying(4000),
	position_2 character varying(4000),
	position_3 character varying(4000),
	enrollment character varying(4000)
);

CREATE TABLE IF NOT EXISTS stg.CourseInstructors(
	course_id character varying(4000),
	instractor_name character varying(4000),
	instractor_id character varying(4000),
	instractor_order character varying(4000),
	instractor_rating character varying(4000),
	instractor_reviews character varying(4000),
	instractor_students character varying(4000),
	instractor_courses_count character varying(4000)
);

CREATE TABLE IF NOT EXISTS stg.CourseLectures(
	course_id character varying(4000),
	lecture_id character varying(4000),
	lecture_index_global character varying(4000),
	lecture_title character varying(4000),
	lecture_duration character varying(4000),
	lecture_type character varying(4000),
	lecture_index_section character varying(4000),
	section_title character varying(4000),
	section_index character varying(4000)
);
