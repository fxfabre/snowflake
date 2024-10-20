-- Badge 1 : Data Warehousing Workshop


use role accountadmin;

create or replace api integration dora_api_integration
    api_provider = aws_api_gateway
    api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
    enabled = true
    api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

create or replace external function util_db.public.grader(
      step varchar, passed boolean, actual integer, expected integer, description varchar
) returns variant
    api_integration = dora_api_integration
    context_headers = (current_timestamp, current_account, current_statement, current_account_name)
    as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
;


use database util_db;
use schema public;

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT
    'DORA_IS_WORKING' as step
    ,(select 123) as actual
    ,123 as expected
    ,'Dora is working!' as description
);


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW01' as step
 ,( select count(*)
   from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
   where schema_name in ('FLOWERS','VEGGIES','FRUITS')) as actual
  ,3 as expected
  ,'Created 3 Garden Plant schemas' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW02' as step
 ,( select count(*)
   from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
   where schema_name = 'PUBLIC') as actual
 , 0 as expected
 ,'Deleted PUBLIC schema.' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW03' as step
 ,( select count(*)
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
   where table_name = 'ROOT_DEPTH') as actual
 , 1 as expected
 ,'ROOT_DEPTH Table Exists' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW04' as step
 ,( select count(*) as SCHEMAS_FOUND
   from UTIL_DB.INFORMATION_SCHEMA.SCHEMATA) as actual
 , 2 as expected
 , 'UTIL_DB Schemas' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW05' as step
,( select row_count
  from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
  where table_name = 'ROOT_DEPTH') as actual
, 3 as expected
,'ROOT_DEPTH row count' as description
);


-- Import CSV manuel
use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW06' as step
 ,( select count(*)
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 1 as expected
 ,'VEGETABLE_DETAILS Table' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW07' as step
 ,( select row_count
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 41 as expected
 , 'VEG_DETAILS row count' as description
);


-- Import CSV notebook
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DWW08' as step
   ,( select iff(count(*)=0, 0, count(*)/count(*))
      from table(information_schema.query_history())
      where query_text like 'execute notebook%Uncle Yer%') as actual
   , 1 as expected
   , 'Notebook success!' as description
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


-- Import CSV SQL
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW10' as step
  ,( select count(*)
    from UTIL_DB.INFORMATION_SCHEMA.stages
    where stage_name='MY_INTERNAL_STAGE'
    and stage_type is null) as actual
  , 1 as expected
  , 'Internal stage created' as description
 );

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DWW11' as step
  ,( select row_count
    from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
    where table_name = 'VEGETABLE_DETAILS_SOIL_TYPE') as actual
  , 42 as expected
  , 'Veg Det Soil Type Count' as description
 );

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
      SELECT 'DWW12' as step
      ,( select row_count
        from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
        where table_name = 'VEGETABLE_DETAILS_PLANT_HEIGHT') as actual
      , 41 as expected
      , 'Veg Detail Plant Height Count' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
     SELECT 'DWW13' as step
     ,( select row_count
       from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
       where table_name = 'LU_SOIL_TYPE') as actual
     , 8 as expected
     ,'Soil Type Look Up Table' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
     SELECT 'DWW14' as step
     ,( select count(*)
       from GARDEN_PLANTS.INFORMATION_SCHEMA.FILE_FORMATS
       where FILE_FORMAT_NAME='L9_CHALLENGE_FF'
       and FIELD_DELIMITER = '\t') as actual
     , 1 as expected
     ,'Challenge File Format Created' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
     SELECT 'DWW15' as step
     ,( select count(*)
      from LIBRARY_CARD_CATALOG.PUBLIC.Book_to_Author ba
      join LIBRARY_CARD_CATALOG.PUBLIC.author a
      on ba.author_uid = a.author_uid
      join LIBRARY_CARD_CATALOG.PUBLIC.book b
      on b.book_uid=ba.book_uid) as actual
     , 6 as expected
     , '3NF DB was Created.' as description
);


-- Import JSON
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW16' as step
  ,( select row_count
    from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES
    where table_name = 'AUTHOR_INGEST_JSON') as actual
  ,6 as expected
  ,'Check number of rows' as description
 );

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
     SELECT 'DWW17' as step
      ,( select row_count
        from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES
        where table_name = 'NESTED_INGEST_JSON') as actual
      , 5 as expected
      ,'Check number of rows' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
   SELECT 'DWW18' as step
  ,( select row_count
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.TABLES
    where table_name = 'TWEET_INGEST') as actual
  , 9 as expected
  ,'Check number of rows' as description
 );

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW19' as step
  ,( select count(*)
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.VIEWS
    where table_name = 'HASHTAGS_NORMALIZED') as actual
  , 1 as expected
  ,'Check number of rows' as description
 );
