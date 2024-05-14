
-- ----------------------------------------------------------------------------
--Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE EV_ROLE;
GRANT ROLE EV_ROLE TO ROLE SYSADMIN;
GRANT ROLE EV_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE EV_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE EV_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE EV_ROLE;

-- Databases
CREATE OR REPLACE DATABASE EV_DB;
GRANT OWNERSHIP ON DATABASE EV_DB TO ROLE EV_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE EV_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE EV_WH TO ROLE EV_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE EV_ROLE;
USE WAREHOUSE EV_WH;
USE DATABASE EV_DB;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA RAW_EV;
CREATE OR REPLACE SCHEMA PROCESSED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- External EV objects
USE SCHEMA EXTERNAL;

CREATE OR REPLACE STAGE EV_RAW_STAGE
    URL = 's3://snowflake-ev-data/raw_data/'
;


