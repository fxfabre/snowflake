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


select * from garden_plants.information_schema.schemata;
SELECT *
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT
 'DWW01' as step
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
