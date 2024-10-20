use database util_db;
use schema public;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW10' as step
  ,( select count(*)
    from UTIL_DB.INFORMATION_SCHEMA.stages
    where stage_name='MY_INTERNAL_STAGE'
    and stage_type is null) as actual
  , 1 as expected
  , 'Internal stage created' as description
 );

use database GARDEN_PLANTS;
use schema VEGGIES;
create or replace table vegetable_details_soil_type
(plant_name varchar(25), soil_type number(1,0));


create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = '|' --pipes as column separators
    skip_header = 1 --one header row to skip
    ;

copy into vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );


use database util_db;
use schema public;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DWW11' as step
  ,( select row_count
    from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES
    where table_name = 'VEGETABLE_DETAILS_SOIL_TYPE') as actual
  , 42 as expected
  , 'Veg Det Soil Type Count' as description
 );


use database GARDEN_PLANTS;
use schema VEGGIES;
create OR REPLACE file format garden_plants.veggies.L9_CHALLENGE_FF
    TYPE = 'CSV'--csv for comma separated files
    FILE_EXTENSION = 'TSV'
    FIELD_DELIMITER = '\t' --commas as column separators
    SKIP_HEADER = 1 --one header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L9_CHALLENGE_FF );

create or replace table LU_SOIL_TYPE(
    SOIL_TYPE_ID number,
    SOIL_TYPE varchar(15),
    SOIL_DESCRIPTION varchar(75)
);

copy into LU_SOIL_TYPE
from @util_db.public.my_internal_stage
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.CSV_TAB_ONEHEADROW );

SELECT * from LU_SOIL_TYPE;


create or replace table GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_PLANT_HEIGHT(
    plant_name varchar(75),
    UOM varchar(1),
    Low_End_of_Range number,
    High_End_of_Range number
);

create OR REPLACE file format garden_plants.veggies.CSV_COMMA_ONEHEADROW
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = ',' --pipes as column separators
    skip_header = 1 --one header row to skip
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ;

copy into GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ( 'veg_plant_height.csv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.CSV_COMMA_ONEHEADROW );


use database util_db;
use schema public;
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















