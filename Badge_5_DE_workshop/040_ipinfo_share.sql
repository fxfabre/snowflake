USE ROLE SYSADMIN;
USE SCHEMA IPINFO_GEOLOC.PUBLIC;

select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;


--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4
BETWEEN start_ip_int AND end_ip_int;


--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
    , logs.user_login
    , logs.user_event
    , logs.datetime_iso8601
    , city
    , region
    , country
    , timezone,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS dow_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address)
BETWEEN start_ip_int AND end_ip_int;



-- Add early-morning, late evening ...
create table ags_game_audience.raw.time_of_day_lu (
    hour number,
    tod_name varchar(25)
);
--insert statement to add all 24 rows to the table
insert into ags_game_audience.raw.time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',')
from ags_game_audience.raw.time_of_day_lu
group by tod_name;

--Wrap any Select in a CTAS statement
create table ags_game_audience.enhanced.logs_enhanced as(
    SELECT
        logs.ip_address,
        logs.user_login         AS GAMER_NAME,
        logs.user_event         AS GAME_EVENT_NAME,
        logs.datetime_iso8601   AS GAME_EVENT_UTC,
        city, region, country,
        timezone                AS GAMER_LTZ_NAME,
        CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        hour(game_event_ltz) as loc_hour,
        tod.tod_name
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int
    LEFT JOIN ags_game_audience.raw.time_of_day_lu AS tod
        ON tod.hour = hour(game_event_ltz)
);
