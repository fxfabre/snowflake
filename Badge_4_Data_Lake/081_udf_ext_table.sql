-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615';
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from
--Melanie's Cafe to Confluent Park
select st_distance(
    st_makepoint($mc_lng,$mc_lat), st_makepoint($loc_lng,$loc_lat)
) as mc_to_cp;


-- Start create / use UDF
USE DATABASE MELS_SMOOTHIE_CHALLENGE_DB;
CREATE SCHEMA LOCATIONS;

CREATE OR REPLACE FUNCTION distance_to_mc(loc_lng number(38, 32), loc_lat number(38, 32))
RETURNS FLOAT AS
$$
st_distance(
    st_makepoint('-104.97300245114094', '39.76471253574085'),
    st_makepoint(loc_lng, loc_lat)
)
$$;


--Tivoli Center into the variables
set tc_lng='-105.00532059763648';
set tc_lat='39.74548137398218';

select distance_to_mc($tc_lng, $tc_lat);


CREATE OR REPLACE VIEW COMPETITION AS
select *
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where (
    (amenity in ('fast_food','cafe','restaurant','juice_bar')) and
    (name ilike '%jamba%' or name ilike '%juice%' or name ilike '%superfruit%')
) or (
    cuisine like '%smoothie%' or cuisine like '%juice%'
);


SELECT name, cuisine, ST_DISTANCE(
        st_makepoint('-104.97300245114094','39.76471253574085'), coordinates
    ) AS distance_to_melanies, *
FROM  competition
ORDER by distance_to_melanies;


CREATE OR REPLACE FUNCTION distance_to_mc(lng_and_lat GEOGRAPHY)
  RETURNS FLOAT AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085'), lng_and_lat
    )
  $$;

SELECT
 name
 ,cuisine
 , coordinates
 ,distance_to_mc(coordinates) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;


-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203';
set tcb_lat='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lng,$tcb_lat);

--this will run the second version of the UDF, bc it converts the coords
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lng,$tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books'
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';


select name
, distance_to_mc(coordinates) as distance_to_melanies
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='bicycle'
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

CREATE OR REPLACE VIEW denver_bike_shops AS
SELECT
    name,
    distance_to_mc(to_geography(coordinates)) AS distance_to_melanies,
    coordinates
FROM OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
WHERE shop = 'bicycle'
ORDER BY 2;



USE DATABASE MELS_SMOOTHIE_CHALLENGE_DB;
USE SCHEMA TRAILS;

-- Will fail, trails_parquet is an internal stage
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(100) as (metadata$filename::varchar(100))
)
    location= @trails_parquet
    auto_refresh = true
    file_format = (type = parquet);

-- Workaround : use External stage + extarnal table + materialized view
CREATE STAGE EXTERNAL_AWS_DLKW
	URL = 's3://uni-dlkw'
	DIRECTORY = ( ENABLE = true );
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_file_name varchar(100) as (metadata$filename::varchar(100))
)
    location= @external_aws_dlkw
    auto_refresh = true
    file_format = (type = parquet);

create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select
 value:sequence_1 as point_id,
 value:trail_name::varchar as trail_name,
 value:latitude::number(11,8) as lng,
 value:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair,
 locations.distance_to_mc(lng,lat) as distance_to_melanies
from t_cherry_creek_trail;
