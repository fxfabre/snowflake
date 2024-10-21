USE ROLE SYSADMIN;
USE DATABASE ZENAS_ATHLEISURE_DB;
CREATE SCHEMA PRODUCTS;
USE SCHEMA PRODUCTS;

create or replace table util_db.public.my_data_types
(
      my_number number
    , my_text varchar(10)
    , my_bool boolean
    , my_float float
    , my_date date
    , my_timestamp timestamp_tz
    , my_variant variant
    , my_array array
    , my_object object
    , my_geography geography
    , my_geometry geometry
    , my_vector vector(int,16)
);


CREATE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS
	DIRECTORY = ( ENABLE = true )
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
-- Load the 10 pictures inside stage, from zip file
list @zenas_athleisure_db.products.SWEATSUITS;

CREATE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_METADATA
	DIRECTORY = ( ENABLE = true );
-- Load the 3 metadata txt files
list @zenas_athleisure_db.products.product_metadata;


-- File sweatsuit_sizes
SELECT $1, $2 FROM @zenas_athleisure_db.products.product_metadata/sweatsuit_sizes.txt;
create or replace file format zmd_file_format_1
    RECORD_DELIMITER = ';'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE;
create view zenas_athleisure_db.products.sweatsuit_sizes as
select REPLACE($1, '\r\n') AS sizes_available
from @product_metadata/sweatsuit_sizes.txt(file_format => zmd_file_format_1)
WHERE sizes_available != '';

select sizes_available from zenas_athleisure_db.products.sweatsuit_sizes;

-- File swt_product_line
SELECT $1, $2 FROM @zenas_athleisure_db.products.product_metadata/swt_product_line.txt;
create or replace file format zmd_file_format_2
    RECORD_DELIMITER = ';'
    FIELD_DELIMITER = '|'
    SKIP_BLANK_LINES = TRUE
    TRIM_SPACE = TRUE;
CREATE OR REPLACE VIEW zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE AS
select REPLACE($1, '\r\n') AS product_code, $2 AS headband_description, $3 AS wristband_description
from @product_metadata/swt_product_line.txt(file_format => zmd_file_format_2);

select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;

-- File product_coordination_suggestions
SELECT $1, $2 FROM @zenas_athleisure_db.products.product_metadata/product_coordination_suggestions.txt;
create or replace file format zmd_file_format_3
    RECORD_DELIMITER = '^'
    FIELD_DELIMITER = '=';
CREATE OR REPLACE VIEW zenas_athleisure_db.products.sweatband_coordination AS
select $1 as product_code, $2 AS has_matching_sweatsuit
from @product_metadata/product_coordination_suggestions.txt (file_format => zmd_file_format_3);

select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;



select metadata$filename, metadata$file_row_number
from @sweatsuits/purple_sweatsuit.png;

select metadata$filename, count(metadata$file_row_number) as number_of_rows
from @sweatsuits
group by 1;


select * from directory(@sweatsuits);
list @sweatsuits;

