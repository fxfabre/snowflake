use role sysadmin;

// Create a new database and set the context to use the new database
create database library_card_catalog comment = 'DWW Lesson 10 ';

//Set the worksheet context to use the new database
use database library_card_catalog;

// Create Author table
create or replace table author (
   author_uid number
  ,first_name varchar(50)
  ,middle_name varchar(50)
  ,last_name varchar(50)
);

// Insert the first two authors into the Author table
insert into author(author_uid, first_name, middle_name, last_name)
values
(1, 'Fiona', '','Macdonald'), (2, 'Gian','Paulo','Faleschini');

// Look at your table with it's new rows
select * from author;


CREATE OR REPLACE SEQUENCE SEQ_AUTHOR_UID
    start = 1
    increment = 1
    ORDER comment = 'Use this to fill in AUTHOR_UID';

//See how the nextval function works
select seq_author_uid.nextval;

show sequences;

//Drop and recreate the counter (sequence) so that it starts at 3
create or replace sequence library_card_catalog.public.seq_author_uid
    start = 3 increment = 1 ORDER
    comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

insert into author(author_uid,first_name, middle_name, last_name) values
    (seq_author_uid.nextval, 'Laura', 'K','Egendorf')
    ,(seq_author_uid.nextval, 'Jan', '','Grover')
    ,(seq_author_uid.nextval, 'Jennifer', '','Clapp')
    ,(seq_author_uid.nextval, 'Kathleen', '','Petelinsek');

SELECT * FROM author;

use database library_card_catalog;
use role sysadmin;

// Create a new sequence, this one will be a counter for the book table
create or replace sequence library_card_catalog.public.seq_book_uid
  start = 1
  increment = 1
  ORDER
  comment = 'Use this to fill in the BOOK_UID every time you add a new row';

create or replace table book(
    book_uid number default library_card_catalog.public.seq_book_uid.nextval,
    title varchar(50),
    year_published number(4,0)
);

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the default setting
// will take care of it for you

insert into book(title, year_published)
values
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

// Create the relationships table
// this is sometimes called a "Many-to-Many table"
create table book_to_author
( book_uid number
  ,author_uid number
);

//Insert rows of the known relationships
insert into book_to_author(book_uid, author_uid)
values
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek


//Check your work by joining the 3 tables together
//You should get 1 row for every author
select *
from book_to_author ba
join author a
    on ba.author_uid = a.author_uid
join book b
    on b.book_uid=ba.book_uid;


-- Handle JSON files
// JSON DDL Scripts
use database library_card_catalog;
use role sysadmin;

// Create an Ingestion Table for JSON Data
create table library_card_catalog.public.author_ingest_json(
    raw_author variant
);

//Create File Format for JSON Data
create file format library_card_catalog.public.json_file_format
    type = 'JSON'
    compression = 'AUTO'
    enable_octal = FALSE
    allow_duplicate = FALSE
    strip_outer_array = TRUE
    strip_null_values = FALSE
    ignore_utf8_errors = FALSE;


select $1
from @util_db.public.my_internal_stage/author_with_header.json (
    file_format => library_card_catalog.public.json_file_format
);

USE DATABASE library_card_catalog;
USE SCHEMA public;
copy into author_ingest_json
from @util_db.public.my_internal_stage
    files = ( 'author_with_header.json')
    file_format = library_card_catalog.public.json_file_format;

SELECT * FROM author_ingest_json;

select raw_author:AUTHOR_UID from author_ingest_json;

SELECT
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;
