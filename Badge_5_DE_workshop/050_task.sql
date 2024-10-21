create task load_logs_enhanced
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
    -- <session_parameter> = <value> [ , <session_parameter> = <value> ... ]
    -- user_task_timeout_ms = <num>
    -- copy grants
    -- comment = '<comment>'
    -- after <string>
  -- when <boolean_expr>
  as
    select 'hello';

use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin;

-- Run once, no auto trigger
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

USE SCHEMA AGS_GAME_AUDIENCE.ENHANCED;
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF
clone ags_game_audience.enhanced.LOGS_ENHANCED;


--- update merge
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
    ON r.user_login = e.GAMER_NAME
    AND r.datetime_iso8601 = e.GAME_EVENT_UTC
    AND r.user_event = e.GAME_EVENT_NAME
WHEN MATCHED THEN
    UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

select * from LOGS_ENHANCED;


--let's truncate so we can start the load over again
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- insert merge
create or replace task load_logs_enhanced
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
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
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
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

select count(*) from ENHANCED.LOGS_ENHANCED;
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;




--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table
--You can change the user_event field each time to create "new" records
--editing the ip_address or datetime_iso8601 can complicate things more than they need to
--editing the user_login will make it harder to remove the fake records after you finish testing
INSERT INTO ags_game_audience.raw.game_logs
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
