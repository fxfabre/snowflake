USE ROLE SYSADMIN;
USE SCHEMA AGS_GAME_AUDIENCE.RAW;


CREATE OR REPLACE STAGE UNI_KISHORE_PIPELINE
    url='s3://uni-kishore-pipeline'
    file_format=FF_JSON_LOGS
    DIRECTORY = ( ENABLE = true );

list @UNI_KISHORE_PIPELINE;

SELECT *
FROM @UNI_KISHORE_PIPELINE/logs_331_340_0_0_0.json;

CREATE TABLE PL_GAME_LOGS (
    raw_log VARIANT
);
COPY INTO PL_GAME_LOGS
FROM @UNI_KISHORE_PIPELINE
    file_format = (format_name = FF_JSON_LOGS);

select * from PL_GAME_LOGS LIMIT 10;


create task GET_NEW_FILES
    warehouse = 'COMPUTE_WH'
    schedule = '10 minute'
AS
    COPY INTO PL_GAME_LOGS
    FROM @UNI_KISHORE_PIPELINE
        file_format = (format_name = FF_JSON_LOGS);

CREATE OR REPLACE VIEW pl_logs AS
SELECT
    raw_log:datetime_iso8601::text AS datetime_iso8601,
    raw_log:ip_address::text AS ip_address,
    raw_log:user_event::text AS user_event,
    raw_log:user_login::text AS user_login,
    raw_log
FROM PL_GAME_LOGS;

SELECT * FROM pl_logs LIMIT 10;
select * from logs limit 10;


-- insert merge
create or replace task ENHANCED.load_logs_enhanced
    warehouse = 'WAREHOUSE_SMALL'
    schedule = '10 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
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
    from (
        --SELECT * FROM AGS_GAME_AUDIENCE.RAW.LOGS
        --UNION ALL
        SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS
    ) logs
    JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int
    LEFT JOIN ags_game_audience.raw.time_of_day_lu AS tod
        ON tod.hour = hour(game_event_ltz)
) r
    ON r.GAMER_NAME = e.GAMER_NAME
    and r.GAME_EVENT_UTC = e.game_event_utc
    and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
    insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, LOC_HOUR, TOD_NAME)
    values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, LOC_HOUR, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;


execute task GET_NEW_FILES;
EXECUTE TASK ENHANCED.load_logs_enhanced;


--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN; -- Enable serverless
use role sysadmin;


-- Same tasks using serverless
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule = '10 minute'
AS
    COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    FROM @UNI_KISHORE_PIPELINE
        file_format = (format_name = FF_JSON_LOGS);


-- insert merge
create or replace task RAW.load_logs_enhanced
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
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
    from (
        --SELECT * FROM AGS_GAME_AUDIENCE.RAW.LOGS
        --UNION ALL
        SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS
    ) logs
    JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int
    LEFT JOIN ags_game_audience.raw.time_of_day_lu AS tod
        ON tod.hour = hour(game_event_ltz)
) r
    ON r.GAMER_NAME = e.GAMER_NAME
    and r.GAME_EVENT_UTC = e.game_event_utc
    and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
    insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, LOC_HOUR, TOD_NAME)
    values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, LOC_HOUR, TOD_NAME) --list of columns (but we can mark as coming from the r select)
;


TRUNCATE TABLE ENHANCED.LOGS_ENHANCED;
TRUNCATE TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;

SELECT count(*) FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;
SELECT count(*) FROM ENHANCED.LOGS_ENHANCED;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;
