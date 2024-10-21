USE ROLE SYSADMIN;
USE SCHEMA AGS_GAME_AUDIENCE.RAW;

CREATE OR REPLACE TABLE ED_PIPELINE_LOGS (
    log_file_name VARCHAR(100),
    log_file_row_id VARCHAR(100),
    load_ltz timestamp_ltz,
    datetime_iso8601 timestamp_ntz,
    user_event VARCHAR(25),
    user_login VARCHAR(100),
    ip_address VARCHAR(100)
) AS (
    SELECT
        METADATA$FILENAME as log_file_name --new metadata column
      , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
      , current_timestamp(0) as load_ltz --new local time of load
      , get($1,'datetime_iso8601')::timestamp_ntz as datetime_iso8601
      , get($1,'user_event')::text as USER_EVENT
      , get($1,'user_login')::text as USER_LOGIN
      , get($1,'ip_address')::text as IP_ADDRESS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
      (file_format => 'ff_json_logs')
);

--truncate the table rows that were input during the CTAS, if that's what you did
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT
    METADATA$FILENAME as log_file_name
  , METADATA$FILE_ROW_NUMBER as log_file_row_id
  , current_timestamp(0) as load_ltz
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
    auto_ingest=true
    aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT
    METADATA$FILENAME as log_file_name
  , METADATA$FILE_ROW_NUMBER as log_file_row_id
  , current_timestamp(0) as load_ltz
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


TRUNCATE TABLE ENHANCED.LOGS_ENHANCED;

-- insert merge
create or replace task RAW.load_logs_enhanced
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    --after AGS_GAME_AUDIENCE.RAW.PIPE_GET_NEW_FILES
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
        SELECT * FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
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


alter task load_logs_enhanced resume;
SELECT * FROM ED_PIPELINE_LOGS;
SELECT * FROM ENHANCED.LOGS_ENHANCED;
select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;


--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');


--query the stream
select *
from ags_game_audience.raw.ed_cdc_stream;

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;





--make a note of how many rows are in the stream
select *
from ags_game_audience.raw.ed_cdc_stream;


--process the stream by using the rows in a merge
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
WHEN NOT MATCHED THEN
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

--Did all the rows from the stream disappear?
select *
from ags_game_audience.raw.ed_cdc_stream;






-- Task time schedule + stream dependent
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
WHEN
    system$stream_has_data('ed_cdc_stream')
AS
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
WHEN NOT MATCHED THEN
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED suspend;
alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
