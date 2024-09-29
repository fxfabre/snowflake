USE DATABASE LIBRARY_CARD_CATALOG;
USE SCHEMA PUBLIC;

-- Nested JSON
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON (
    "RAW_NESTED_BOOK" VARIANT
);
copy into LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
from @util_db.public.my_internal_stage
    files = ( 'json_book_author_nested.txt')
    file_format = library_card_catalog.public.json_file_format;

//a few simple queries
SELECT RAW_NESTED_BOOK FROM NESTED_INGEST_JSON;
SELECT RAW_NESTED_BOOK:year_published FROM NESTED_INGEST_JSON;
SELECT RAW_NESTED_BOOK:authors FROM NESTED_INGEST_JSON;

//Use these example flatten commands to explore flattening the nested book and author data
SELECT value:first_name
FROM NESTED_INGEST_JSON,
LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

SELECT value:first_name
FROM NESTED_INGEST_JSON,
table(flatten(RAW_NESTED_BOOK:authors));

//Add a CAST command to the fields returned
SELECT value:first_name::VARCHAR, value:last_name::VARCHAR
FROM NESTED_INGEST_JSON,
LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

//Assign new column  names to the columns using "AS"
SELECT
    value:first_name::VARCHAR AS FIRST_NM,
    value:last_name::VARCHAR AS LAST_NM
FROM NESTED_INGEST_JSON,
LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);


//Create a new database to hold the Twitter file
create database SOCIAL_MEDIA_FLOODGATES
    comment = 'There\'s so much data from social media - flood warning';

use database SOCIAL_MEDIA_FLOODGATES;

//Create a table in the new database
create table SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST ("RAW_STATUS" VARIANT)
    comment = 'Bring in tweets, one row per tweet or status entity';

//Create a JSON file format in the new database
create file format SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT
    type = 'JSON'
    strip_outer_array = true;

copy into SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
from @util_db.public.my_internal_stage
    files = ( 'nutrition_tweets.json')
    file_format = SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT;




//select statements as seen in the video
SELECT RAW_STATUS FROM TWEET_INGEST;
SELECT RAW_STATUS:entities FROM TWEET_INGEST;
SELECT RAW_STATUS:entities:hashtags FROM TWEET_INGEST;
SELECT RAW_STATUS:entities:hashtags[0].text FROM TWEET_INGEST;      -- Add array index
SELECT RAW_STATUS:created_at::DATE FROM TWEET_INGEST ORDER BY 1;    -- Cast to DATE

SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST
WHERE RAW_STATUS:entities:hashtags[0].text is not null;

SELECT value                                                        -- Lateral flatten
FROM TWEET_INGEST,
LATERAL FLATTEN(input => RAW_STATUS:entities:hashtags);

SELECT value                                                        -- Table flatten
FROM TWEET_INGEST,
TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

SELECT value:text                                                   -- Extract text field
FROM TWEET_INGEST,
LATERAL FLATTEN(input => RAW_STATUS:entities:hashtags);

SELECT
    RAW_STATUS:user:id  AS USER_ID,
    RAW_STATUS:id       AS TWEET_ID,
    value:text::VARCHAR AS HASHTAG_TEXT                             -- Cast json data -> String to remove the "quotes"
FROM TWEET_INGEST,
LATERAL FLATTEN(input => RAW_STATUS:entities:hashtags);


-- Vizw with Normalized data
create or replace view SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED as (
    SELECT
        RAW_STATUS:user:id  AS USER_ID,
        RAW_STATUS:id       AS TWEET_ID,
        value:text::VARCHAR AS HASHTAG_TEXT
    FROM TWEET_INGEST,
    LATERAL FLATTEN(input => RAW_STATUS:entities:hashtags)
);
