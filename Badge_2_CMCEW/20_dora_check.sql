-- Badge 2: Collaboration, Marketplace & Cost Estimation Workshop

use database util_db;
use schema public;

select GRADER(step,(actual = expected), actual, expected, description) as graded_results from (
SELECT 'DORA_IS_WORKING' as step
 ,(select 223 ) as actual
 ,223 as expected
 ,'Dora is working!' as description
);

-- Import CSV from S3
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
 SELECT 'CMCW01' as step
 ,( select count(*)
   from snowflake.account_usage.databases
   where database_name = 'INTL_DB'
   and deleted is null) as actual
 , 1 as expected
 ,'Created INTL_DB' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW02' as step
 ,( select count(*)
   from INTL_DB.INFORMATION_SCHEMA.TABLES
   where table_schema = 'PUBLIC'
   and table_name = 'INT_STDS_ORG_3166') as actual
 , 1 as expected
 ,'ISO table created' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW03' as step
 ,(select row_count
   from INTL_DB.INFORMATION_SCHEMA.TABLES
   where table_name = 'INT_STDS_ORG_3166') as actual
 , 249 as expected
 ,'ISO Table Loaded' as description
);

-- Create view
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW04' as step
 ,( select count(*)
   from INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO) as actual
 , 249 as expected
 ,'Nations Sample Plus Iso' as description
);

-- Import currencies
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW05' as step
 ,(select row_count
  from INTL_DB.INFORMATION_SCHEMA.TABLES
  where table_schema = 'PUBLIC'
  and table_name = 'COUNTRY_CODE_TO_CURRENCY_CODE') as actual
 , 265 as expected
 ,'CCTCC Table Loaded' as description
);
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW06' as step
 ,(select row_count
  from INTL_DB.INFORMATION_SCHEMA.TABLES
  where table_schema = 'PUBLIC'
  and table_name = 'CURRENCIES') as actual
 , 151 as expected
 ,'Currencies table loaded' as description
);

select grader(step, (actual = expected), actual, expected, description) as graded_results from(
 SELECT 'CMCW07' as step
,( select count(*)
  from INTL_DB.PUBLIC.SIMPLE_CURRENCY ) as actual
, 265 as expected
,'Simple Currency Looks Good' as description
);
