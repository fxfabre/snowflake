ALTER DATABASE SNOWFLAKE_SHARE RENAME TO SNOWFLAKE_SAMPLE_DATA;

grant imported privileges               -- Droit de lecture sur la DB
on database SNOWFLAKE_SAMPLE_DATA
to role SYSADMIN;


--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);


SELECT N_NATIONKEY, N_NAME, N_REGIONKEY FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;     -- Nations Table
SELECT R_REGIONKEY, R_NAME FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;                  -- Regions Table

-- Join the Tables and Sort
SELECT R_NAME as Region, N_NAME as Nation
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION
    ON N_REGIONKEY = R_REGIONKEY
ORDER BY R_NAME, N_NAME ASC;

--Group and Count Rows Per Region
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION
    ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;



SELECT count(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;


-- where did you put the function?
show user functions in account;

-- did you put it here?
select *
from util_db.information_schema.functions
where function_name = 'GRADER'
and function_catalog = 'UTIL_DB'
and function_owner = 'ACCOUNTADMIN';



use role SYSADMIN;

create database INTL_DB;
use schema INTL_DB.PUBLIC;


create warehouse INTL_WH with
    warehouse_size = 'XSMALL'
    warehouse_type = 'STANDARD'
    auto_suspend = 600 --600 seconds/10 mins
    auto_resume = TRUE;

use warehouse INTL_WH;
create or replace table intl_db.public.INT_STDS_ORG_3166 (
    iso_country_name varchar(100),
    country_name_official varchar(200),
    sovreignty varchar(40),
    alpha_code_2digit varchar(2),
    alpha_code_3digit varchar(3),
    numeric_country_code integer,
    iso_subdivision varchar(15),
    internet_domain_code varchar(10)
);
create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR
    type = 'CSV' --use CSV for any flat file
    compression = 'AUTO'
    field_delimiter = '|' --pipe or vertical bar
    record_delimiter = '\r' --carriage return
    skip_header = 1  --1 header row
    field_optionally_enclosed_by = '\042'  --double quotes
    trim_space = FALSE;
show stages in account;
create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';
list @util_db.public.aws_s3_bucket;

copy into intl_db.public.INT_STDS_ORG_3166
from @util_db.public.aws_s3_bucket
    files = ( 'ISO_Countries_UTF8_pipe.csv')
    file_format = ( format_name=util_db.public.PIPE_DBLQUOTE_HEADER_CR );

select count(*) as found, '249' as expected
from INTL_DB.PUBLIC.INT_STDS_ORG_3166;

select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';

select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';



create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
(iso_country_name, country_name_official, alpha_code_2digit, region) AS (
    select
         iso_country_name
        ,country_name_official,alpha_code_2digit
        ,r_name as region
    from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
        on upper(i.iso_country_name)= n.n_name
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
        on n_regionkey = r_regionkey
);

create table intl_db.public.CURRENCIES
(
    currency_ID integer,
    currency_char_code varchar(3),
    currency_symbol varchar(4),
    currency_digital_code varchar(3),
    currency_digital_name varchar(30)
)
comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE
(
    country_char_code varchar(3),
    country_numeric_code integer,
    country_name varchar(100),
    currency_name varchar(100),
    currency_char_code varchar(3),
    currency_numeric_code integer
)
comment = 'Mapping table currencies to countries';

create file format util_db.public.CSV_COMMA_LF_HEADER
    type = 'CSV'
    field_delimiter = ','
    record_delimiter = '\n' -- the n represents a Line Feed character
    skip_header = 1;

copy into intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE
from @util_db.public.aws_s3_bucket
    files = ( 'country_code_to_currency_code.csv')
    file_format = ( format_name=util_db.public.CSV_COMMA_LF_HEADER );

copy into intl_db.public.CURRENCIES
from @util_db.public.aws_s3_bucket
    files = ( 'currencies.csv')
    file_format = ( format_name=util_db.public.CSV_COMMA_LF_HEADER );


SELECT * FROM COUNTRY_CODE_TO_CURRENCY_CODE;
SELECT * FROM CURRENCIES;

CREATE OR REPLACE VIEW simple_currency AS (
    SELECT DISTINCT
        COUNTRY_CHAR_CODE AS CTY_CODE,
        CURRENCY_CHAR_CODE AS CUR_CODE
    FROM intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE
);
