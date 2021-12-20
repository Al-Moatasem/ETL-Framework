# ETL Framework | TO DO

List of improvements/fixes to be implemented as the next steps, -no particular order-

1. Databases, and schemas should be defined in a config file or so, then the script will create them.
2. Error handling.
3. Logging the execution of the python script.
4. Auditing the ETL jobs
   1. Store the file names per execution
   2. Use a naming pattern for job names
      1. `stg_loading_courses_basic_info` for loading staging tables, where `courses_basic_info` is the table name.
      2. `dw_loading_dimensions_DimCourse` for dimension tables.
      3. `dw_loading_facts_FactCourseLectures` for fact tables.
      4. `dw_master` for master jobs, if exists.
5. Check if the file doesn't exists -in case of archiving the file after the loading into staging table-, skip the ETL process
   1. same goes with the folder, if there are no files that match a certain pattern
6. Using Incremental Load instead of Truncate and Load.