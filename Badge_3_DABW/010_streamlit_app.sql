USE DATABASE SMOOTHIES;

CREATE OR REPLACE TABLE FRUIT_OPTIONS (
    FRUIT_ID INTEGER,
    FRUIT_NAME VARCHAR(25)
);

create or replace file format smoothies.public.two_headerrow_pct_delim
   type = CSV,
   skip_header = 2,
   field_delimiter = '%',
   trim_space = TRUE
;

CREATE OR REPLACE FILE FORMAT smoothies.public.tst_two_headerrow_pct_delim
    TYPE = 'CSV',
    RECORD_DELIMITER = '\n',
    FIELD_DELIMITER = '%',
    PARSE_HEADER = True,
    TRIM_SPACE = True,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;

CREATE STAGE smoothies.public.my_uploaded_files
	DIRECTORY = ( ENABLE = true )
	COMMENT = 'tmp storage to fill tables';

-- TODO : upload file "fruits available for smoothies" to stage

SELECT $1, $2, $3, $4, $5
FROM @smoothies.public.my_uploaded_files(
    FILE_FORMAT => smoothies.public.two_headerrow_pct_delim
);

COPY INTO smoothies.public.fruit_options
from @smoothies.public.my_uploaded_files
    files = ('fruits_available_for_smoothies.txt')
    file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
    on_error = abort_statement
    validation_mode = return_errors
    purge = false;



COPY INTO smoothies.public.fruit_options
FROM (
    SELECT $2 AS FRUIT_ID, $1 AS FRUIT_NAME
    FROM @smoothies.public.my_uploaded_files(
        FILE_FORMAT => smoothies.public.two_headerrow_pct_delim
    )
);


Use role SYSADMIN;
CREATE OR REPLACE TABLE smoothies.public.orders (
    ingredients varchar(200)
);

Insert into smoothies.public.orders(ingredients) values ('Elderberries Figs Guava');
select distinct * from smoothies.public.orders;

ALTER TABLE smoothies.public.orders ADD COLUMN NAME_ON_ORDER varchar(100);
ALTER TABLE smoothies.public.orders ADD COLUMN ORDER_FILLED BOOLEAN DEFAULT FALSE;

insert into smoothies.public.orders values ('Blueberries Dragon Fruit Honeydew', 'FX')


create sequence order_seq
    start = 1
    increment = 2
    ORDER
    comment = 'Provide a unique ID for each order';

truncate table SMOOTHIES.PUBLIC.ORDERS;

alter table SMOOTHIES.PUBLIC.ORDERS
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column

create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,    -- autoincrement
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);


ALTER TABLE smoothies.public.fruit_options ADD COLUMN SEARCH_ON varchar(25);
UPDATE smoothies.public.fruit_options SET search_on = fruit_name;

select * from smoothies.public.fruit_options;

UPDATE smoothies.public.fruit_options SET search_on = 'Apple' WHERE search_on = 'Apples';
UPDATE smoothies.public.fruit_options SET search_on = 'Blueberry' WHERE search_on = 'Blueberries';
UPDATE smoothies.public.fruit_options SET search_on = 'Elderberry' WHERE search_on = 'Elderberries';
UPDATE smoothies.public.fruit_options SET search_on = 'Fig' WHERE search_on = 'Figs';
UPDATE smoothies.public.fruit_options SET search_on = 'Jackfruit' WHERE search_on = 'Strawberries';
UPDATE smoothies.public.fruit_options SET search_on = 'Raspberry' WHERE search_on = 'Raspberries';
UPDATE smoothies.public.fruit_options SET search_on = 'Strawberry' WHERE search_on = 'Strawberries';
