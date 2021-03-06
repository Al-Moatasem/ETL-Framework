# ETL Framework | TO DO

List of improvements/fixes to be implemented as the next steps, -no particular order-

1. ~~Databases, and schemas should be defined in a config file or so, then the script will create them.~~
2. Error handling.
3. Logging the execution of the python script.
4. Auditing the ETL jobs
   1. ✔ Store the file names per execution
   2.  ✔ Use a naming pattern for job names
      1. ✔`stg_loading_courses_basic_info` for loading staging tables, where `courses_basic_info` is the table name.
      2. ✔`dw_loading_dimensions_DimCourse` for dimension tables.
      3. ✔`dw_loading_facts_FactCourseLectures` for fact tables.
      4. ✔ `dw_master` for master jobs, if exists.
   3. Review `Update_audit_record` in `etl_audit.py`, replace the counters with a dict or two lists, one for column names, and the other list for values.
   4. ✔ **Fixed** ParentAuditKey, currently, it is usinge a contstant value (-1).
   6. Store the number of inserted records while processing dimensional model.
      1. The engine/connection/cursor has an option for the number of affected records after executing the sql statement.
5. Check if the file doesn't exists -in case of archiving the file after the loading into staging table-, skip the ETL process
   1. same goes with the folder, if there are no files that match a certain pattern
6. Building separate ETL jobs for Incremental Load.
7. Creating a new ETL job for Extracting (STG) data from a table in a database
   1. Currently, there is no business need for this ETL job, but adding it to the framework will be a good addition.