USE DATABASE UTIL_DB;

create function sum_mystery_bag_vars(var1 number, var2 number, var3 number) returns number AS
    'select var1 + var2 + var3';

CREATE OR REPLACE FUNCTION  UTIL_DB.PUBLIC.NEUTRALIZE_WHINING (s text) returns text AS
    'SELECT INITCAP(s)';
