USE ROLE SYSADMIN;

CREATE DATABASE MELS_SMOOTHIE_CHALLENGE_DB;
DROP SCHEMA public;
CREATE SCHEMA TRAILS;

CREATE STAGE TRAILS_GEOJSON
    DIRECTORY = ( ENABLE = true );
-- Upload files to stage
CREATE STAGE TRAILS_PARQUET
    DIRECTORY = ( ENABLE = true );
-- TODO : upload parquet file to stage

CREATE FILE FORMAT FF_JSON
    type = JSON;
CREATE FILE FORMAT FF_PARQUET
    type = PARQUET;


---------- Read Parquet
CREATE OR REPLACE VIEW CHERRY_CREEK_TRAIL AS
SELECT
    $1:sequence_1 AS point_id,
    $1:trail_name::varchar AS trail_name,
    $1:latitude::number(11, 8) AS lng,
    $1:longitude::number(11, 8) AS lat,
    lng||' '||lat as coord_pair
    --$1:sequence_2 AS sequence_2,
    --$1:elevation::float AS elevation,
FROM @TRAILS_PARQUET (
    FILE_FORMAT => FF_PARQUET
)
order by point_id;

SELECT
    point_id,
    'POINT(' || coord_pair || ')' AS trail_point
FROM CHERRY_CREEK_TRAIL
LIMIT 10;


select
    'LINESTRING(' || listagg(coord_pair, ',') within group (order by point_id) ||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

SHOW TRAILS_GEOJSON;


---------- Read JSON
SELECT * FROM @TRAILS_GEOJSON (
    FILE_FORMAT => FF_JSON
);

CREATE OR REPLACE VIEW DENVER_AREA_TRAILS AS
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry::string as feature_coordinates -- geometry:coordinates
,$1:features[0]:geometry::string as geometry
, st_length(TO_GEOGRAPHY(geometry)) AS trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);


--Remember this code?
select
    'LINESTRING('|| listagg(coord_pair, ',') within group (order by point_id) || ')' as my_linestring,
    st_length(TO_GEOGRAPHY(my_linestring)) as length_of_trail
from cherry_creek_trail
group by trail_name;



SELECT
    feature_name,
    feature_coordinates,
    geometry,
    trail_length,
    --st_length(TO_GEOGRAPHY(feature_coordinates)) AS wo_length,
    --st_length(TO_GEOGRAPHY(geometry)) AS geom_length,
FROM DENVER_AREA_TRAILS;

select get_ddl('view', 'DENVER_AREA_TRAILS');



-- merge data from parquer & json
--Create a view that will have similar columns to DENVER_AREA_TRAILS
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select
    trail_name as feature_name,
    '{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry,
    st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;


--Create a view that will have similar columns to DENVER_AREA_TRAILS
select feature_name, to_geography(geometry), trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, to_geography(geometry), trail_length
from DENVER_AREA_TRAILS_2;

--Add more GeoSpatial Calculations to get more GeoSpecial Information!
create view trails_and_boundaries AS
select feature_name
    , to_geography(geometry) as my_linestring
    , st_xmin(my_linestring) as min_eastwest
    , st_xmax(my_linestring) as max_eastwest
    , st_ymin(my_linestring) as min_northsouth
    , st_ymax(my_linestring) as max_northsouth
    , trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
    , to_geography(geometry) as my_linestring
    , st_xmin(my_linestring) as min_eastwest
    , st_xmax(my_linestring) as max_eastwest
    , st_ymin(my_linestring) as min_northsouth
    , st_ymax(my_linestring) as max_northsouth
    , trail_length
from DENVER_AREA_TRAILS_2;




select 'POLYGON(('||
    min(min_eastwest)||' '||max(max_northsouth)||','||
    max(max_eastwest)||' '||max(max_northsouth)||','||
    max(max_eastwest)||' '||min(min_northsouth)||','||
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;
