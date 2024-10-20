use role accountadmin;

use database util_db;
use schema public;

select * from garden_plants.information_schema.schemata;

SELECT *
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');
