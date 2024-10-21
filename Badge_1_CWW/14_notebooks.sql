create or replace TABLE FLOWER_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);


use database util_db;
use schema public;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DWW08' as step
   ,( select iff(count(*)=0, 0, count(*)/count(*))
      from table(information_schema.query_history())
      where query_text like 'execute notebook%Uncle Yer%') as actual
   , 1 as expected
   , 'Notebook success!' as description
);


create or replace TABLE GARDEN_PLANTS.FRUITS.FRUIT_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW09' as step
 ,( select iff(count(*)=0, 0, count(*)/count(*))
    from snowflake.account_usage.query_history
    where query_text like 'execute streamlit "GARDEN_PLANTS"."FRUITS".%'
   ) as actual
 , 1 as expected
 ,'SiS App Works' as description
);


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW10' as step
  ,( select count(*)
    from UTIL_DB.INFORMATION_SCHEMA.stages
    where stage_name='MY_INTERNAL_STAGE'
    and stage_type is null) as actual
  , 1 as expected
  , 'Internal stage created' as description
 );

