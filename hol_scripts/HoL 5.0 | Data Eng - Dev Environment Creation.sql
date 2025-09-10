--### HoL 5.0: Data Engineering - Development Environment Creation ###--
--### Creating Zero Copy Clone & Setting up Development Environment ###--

---Set Up Context---
USE ROLE sysadmin;
USE WAREHOUSE data_transformation_wh;

---PART 1: CREATE DEVELOPMENT ENVIRONMENT WITH ZERO COPY CLONE---
--Zero copy clones are instantaneous copies that share the same underlying data
--Perfect for creating isolated development environments without storage duplication

-- Create parameterized database name using current user for multi-user environments
SET current_username = CURRENT_USER();
SET user_db_name = (SELECT 'finserv_demo_' || REPLACE($current_username, '.', '_') || '_dev_clone');

SELECT $user_db_name;

--Create a development clone of the entire finserv_demo database with user-specific name
CREATE OR REPLACE DATABASE IDENTIFIER($user_db_name) 
CLONE finserv_demo;

--Set context to the new development environment
USE DATABASE IDENTIFIER($user_db_name);
USE SCHEMA raw_data;

--Verify all objects were cloned
SHOW SCHEMAS;
SHOW TABLES IN SCHEMA raw_data;


---PART 2: VERIFY SOURCE DATA IN DEVELOPMENT ENVIRONMENT---
--Check that our source data is available in the development clone
SELECT COUNT(*),MIN(transaction_date),MAX(transaction_date) as cash_deposits_count FROM cash_deposits;
SELECT COUNT(*),MIN(transaction_date),MAX(transaction_date) as security_trades_count FROM security_trades;

--Sample the data to ensure it looks correct
SELECT * FROM raw_data.cash_deposits   LIMIT 5;
SELECT * FROM raw_data.security_trades LIMIT 5;

---PART 3: SNOWPIPE MANAGEMENT IN DEVELOPMENT ENVIRONMENT---
--Important: Pipes are paused when databases are cloned
--We need to manually check and resume them in the development environment

--First let's check all pipe objects
SHOW PIPES;

--Check the current status of our pipes
SELECT SYSTEM$PIPE_STATUS('raw_data.cash_deposits_pipe');
SELECT SYSTEM$PIPE_STATUS('raw_data.security_trades_pipe');

--Resume the pipes if they exist and are paused
--Note: You may need to adjust these based on your actual pipe names
SELECT SYSTEM$PIPE_FORCE_RESUME('cash_deposits_pipe');
SELECT SYSTEM$PIPE_FORCE_RESUME('security_trades_pipe');

--Verify pipes are now running
SELECT SYSTEM$PIPE_STATUS('raw_data.cash_deposits_pipe');
SELECT SYSTEM$PIPE_STATUS('raw_data.security_trades_pipe');

--Alternative: If you prefer to keep pipes paused in development
--You can manually pause them to prevent unwanted data ingestion
-- ALTER PIPE cash_deposits_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
-- ALTER PIPE security_trades_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

---PART 4: DEVELOPMENT ENVIRONMENT VALIDATION---
--Verify that all necessary objects exist in the development environment

--Check raw_data schema objects
USE SCHEMA raw_data;
SHOW TABLES;
SHOW VIEWS;
SHOW PIPES;

---RESET---
drop database identifier($user_db_name) ;


