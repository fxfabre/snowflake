USE ROLE SYSADMIN;
CREATE SCHEMA AGS_GAME_AUDIENCE.CURATED;


--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
group by gamer_name;

select GAMER_NAME
       ,game_event_ltz as login
       ,lead(game_event_ltz)
                OVER (
                    partition by GAMER_NAME
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;



--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins'
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login
       ,lead(game_event_ltz)
                OVER (
                    partition by GAMER_NAME
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;
