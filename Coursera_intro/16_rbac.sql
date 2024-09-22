SELECT CURRENT_USER;

USE ROLE accountadmin;

---> create a role
CREATE ROLE tasty_de;

---> see what privileges this new role has
SHOW GRANTS TO ROLE tasty_de;

---> see what privileges an auto-generated role has
SHOW GRANTS TO ROLE accountadmin;

---> grant a role to a specific user
GRANT ROLE tasty_de TO USER [username];


---> use a role
USE ROLE tasty_de;

---> try creating a warehouse with this new role
CREATE WAREHOUSE tasty_de_test;

USE ROLE accountadmin;

---> grant the create warehouse privilege to the tasty_de role
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tasty_de;

---> show all of the privileges the tasty_de role has
SHOW GRANTS TO ROLE tasty_de;

USE ROLE tasty_de;

---> test to see whether tasty_de can create a warehouse
CREATE WAREHOUSE tasty_de_test;

---> learn more about the privileges each of the following auto-generated roles has

SHOW GRANTS TO ROLE securityadmin;

SHOW GRANTS TO ROLE useradmin;

SHOW GRANTS TO ROLE sysadmin;

SHOW GRANTS TO ROLE public;


------

CREATE ROLE tasty_role;
SHOW GRANTS TO ROLE tasty_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tasty_role;
SELECT CURRENT_USER;
GRANT ROLE tasty_role TO USER FXFABRE;
USE ROLE tasty_role;
CREATE WAREHOUSE tasty_test_wh;

USE ROLE accountadmin;
SHOW GRANTS TO USER FXFABRE;
SHOW GRANTS TO ROLE USERADMIN;
