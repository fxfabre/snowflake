-- DNGW Data Engineering Workshop

alter user FXFABRE set default_role = 'SYSADMIN';
alter user FXFABRE set default_warehouse = 'COMPUTE_WH';
alter user FXFABRE set default_namespace = 'UTIL_DB.PUBLIC';


CREATE DATABASE AGS_GAME_AUDIENCE;
DROP SCHEMA PUBLIC;
CREATE SCHEMA RAW;

CREATE TABLE GAME_LOGS (
    raw_log VARIANT
);
list @UNI_KISHORE/kickoff;
CREATE OR REPLACE FILE FORMAT FF_JSON_LOGS
    type = 'JSON',
    strip_outer_array = true;

SELECT *
FROM @UNI_KISHORE/kickoff (
    file_format => FF_JSON_LOGS
);
COPY INTO AGS_GAME_AUDIENCE.raw.game_logs
FROM @UNI_KISHORE/kickoff
    file_format = (format_name = FF_JSON_LOGS);
-- By default, Snowflake load all files from the stage. More options possibles

CREATE VIEW logs AS
SELECT
    raw_log:agent::text AS agent,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    *
FROM AGS_GAME_AUDIENCE.raw.game_logs;

select * from logs;


--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';


-- log file V2 avec adresse IP

list @UNI_KISHORE/updated_feed;
SELECT *
FROM @UNI_KISHORE/updated_feed (
    file_format => FF_JSON_LOGS
);
COPY INTO AGS_GAME_AUDIENCE.raw.game_logs
FROM @UNI_KISHORE/updated_feed
    file_format = (format_name = FF_JSON_LOGS);

CREATE OR REPLACE VIEW logs AS
SELECT
    --raw_log:agent::text AS agent,
    raw_log:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:ip_address::text as ip_address,
    *
FROM AGS_GAME_AUDIENCE.raw.game_logs
WHERE ip_address is not null;

select count(*) from GAME_LOGS;
select * from logs
--WHERE ip_address = '100.41.16.160'
WHERE USER_LOGIN ilike '%kishore%'
order by 1;


select parse_ip('100.41.16.160', 'inet');
