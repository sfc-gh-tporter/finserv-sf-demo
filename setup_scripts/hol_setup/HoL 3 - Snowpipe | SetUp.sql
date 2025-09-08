----THIS SCRIPT CREATES THE FILES LOADED BY SNOWPIPE-----
use role accountadmin;
use schema data_unload.transaction_unload;

----
select count(*)from finserv_demo.raw_data.retirement_contributions;
--All ready have all retirement_contributions

----****************SECURITY TRANSACTION UNLOAD***************----
CREATE OR REPLACE TABLE security_trades_unload AS 

SELECT 
 TRANSACTION_DATE,
 md5(concat(TRANSACTION_DATE,CUSTOMER_ID)) as transaction_id,
  OBJECT_CONSTRUCT(
    'customer_id', CUSTOMER_ID,
    'transaction_date', TRANSACTION_DATE,
    'transaction_category', TRANSACTION_CATEGORY,
    'account_type', ACCOUNT_TYPE_NAME,
    'trades', ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'ticker', TICKER,
        'shares', SHARE_AMOUNT,
        'trade_price', TRADE_PRICE,
        'total_trade_value', TOTAL_TRADE_AMOUNT,
        'action', 'Buy'
      )
    )
  ) as transaction_payload
FROM all_transactions
WHERE TRANSACTION_CATEGORY = 'Securities Trade'
GROUP BY CUSTOMER_ID, TRANSACTION_DATE, ACCOUNT_TYPE_NAME, TRANSACTION_CATEGORY
ORDER BY TRANSACTION_DATE;

--ADDING COLs FOR UNLOAD TRACKING--
ALTER TABLE security_trades_unload ADD COLUMN unload_status string ;
ALTER TABLE security_trades_unload ADD COLUMN unload_time timestamp ;

 --CSV UNLOAD FILE FORMAT---
CREATE OR REPLACE FILE FORMAT data_unload.transaction_unload.csv_unload_file_format 
    TYPE = CSV
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = 'n'
    ESCAPE_UNENCLOSED_FIELD = NONE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FILE_EXTENSION = 'csv';

--Security Trade Unload Stage--
CREATE OR REPLACE STAGE data_unload.transaction_unload.security_trade_unload_stage
 url= 's3://tporter-demo-root/finserv-demo-stage/security_trades/'
 credentials = (AWS_KEY_ID='YOUR_AWS_KEY_ID' AWS_SECRET_KEY='YOUR_AWS_SECRET_KEY')
 encryption = (type = 'none');

--Copy into location Unload Command--
COPY INTO @data_unload.transaction_unload.security_trade_unload_stage/
FROM (SELECT transaction_date
            ,transaction_id
            ,to_json(transaction_payload) as transaction_payload
      FROM data_unload.transaction_unload.security_trades_unload
      WHERE transaction_date = '')
FILE_FORMAT = (FORMAT_NAME = 'csv_unload_file_format')
OVERWRITE = true
SINGLE = true ;

----****************CASH TRANSACTION UNLOAD***************----
CREATE OR REPLACE TABLE cash_deposit_unload AS 

SELECT *
FROM all_transactions
WHERE TRANSACTION_CATEGORY = 'Cash Deposit'
ORDER BY transaction_date ;

--ADDING COLs FOR UNLOAD TRACKING--
ALTER TABLE cash_deposit_unload ADD COLUMN unload_status string ;
ALTER TABLE cash_deposit_unload ADD COLUMN unload_time timestamp ;

---CASH UNLOAD STAGE---
CREATE OR REPLACE STAGE data_unload.transaction_unload.cash_deposit_unload_stage
 url= 's3://tporter-demo-root/finserv-demo-stage/cash_deposits/'
 credentials = (AWS_KEY_ID='YOUR_AWS_KEY_ID' AWS_SECRET_KEY='YOUR_AWS_SECRET_KEY')
 encryption = (type = 'none');


--TEST UNLOAD--
COPY INTO @data_unload.transaction_unload.cash_deposit_unload_stage 
FROM (SELECT transaction_id,customer_id,transaction_date,ticker,deposit_amount,account_type,transaction_category
      FROM data_unload.transaction_unload.cash_deposit_unload
      WHERE transaction_date = '2019-01-02')
FILE_FORMAT = (FORMAT_NAME = 'csv_unload_file_format')
OVERWRITE = true
SINGLE = true ;

----****************PROC SCRIPTING***************----
CREATE OR REPLACE PROCEDURE security_trades_unload_proc()
RETURNS varchar
LANGUAGE sql
AS 
DECLARE
    date_to_unload date := (SELECT MIN(transaction_date) FROM transaction_unload.security_trades_unload WHERE unload_time IS NULL );
    file_name_string string := 'security_transaction_unload_' || :date_to_unload || '.csv';
    copy_command STRING;
BEGIN 

    copy_command := 
        'COPY INTO @data_unload.transaction_unload.security_trade_unload_stage/' || :file_name_string || 
        ' FROM ( ' ||
            'SELECT ' ||
                'transaction_date '  ||
                ',transaction_id  ' ||
                ',to_json(transaction_payload) as transaction_payload ' ||
            'FROM data_unload.transaction_unload.security_trades_unload ' ||
            'WHERE transaction_date = ''' || :date_to_unload || ''' ' ||
        ') ' ||
        'FILE_FORMAT = (format_name = ''csv_unload_file_format'') ' ||
        'OVERWRITE = true ' ||
        'SINGLE = true';

    EXECUTE IMMEDIATE :copy_command;

    UPDATE data_unload.transaction_unload.security_trades_unload 
           SET unload_status = 'SUCCESS' , unload_time = current_timestamp() 
    WHERE transaction_date = :date_to_unload;

RETURN 'transaction_date =' || :date_to_unload ||' file_name_string = ' || :file_name_string ;
END;

CALL security_trades_unload_proc();

    SELECT * FROM data_unload.transaction_unload.security_trades_unload ORDER BY transaction_date;

---CASH DEPOSIT PROC_---
CREATE OR REPLACE PROCEDURE cash_deposits_unload_proc()
RETURNS varchar
LANGUAGE sql
AS 
DECLARE
    date_to_unload date := (SELECT MIN(transaction_date) FROM cash_deposit_unload WHERE unload_time IS NULL );
    file_name_string string := 'cash_deposits_unload' || :date_to_unload || '.csv';
    copy_command STRING;
BEGIN 

    copy_command := 
        'COPY INTO @cash_deposit_unload_stage/' || :file_name_string ||
        ' FROM ( ' ||
            'SELECT transaction_id,customer_id,transaction_date,ticker,deposit_amount,account_type,transaction_category'  ||
            ' FROM cash_deposit_unload ' ||
            'WHERE transaction_date = ''' || :date_to_unload || ''' ' ||
        ') ' ||
        'FILE_FORMAT = (format_name = ''csv_unload_file_format'') ' ||
        'OVERWRITE = true ' ||
        'SINGLE = true';

    EXECUTE IMMEDIATE :copy_command;

    UPDATE cash_deposit_unload 
           SET unload_status = 'SUCCESS' , unload_time = current_timestamp() 
    WHERE transaction_date = :date_to_unload;

RETURN 'transaction_date =' || :date_to_unload ||' file_name_string = ' || :file_name_string ;
END;

-- Parent task that handles scheduling - does nothing but coordinate child tasks
CREATE OR REPLACE TASK data_unload_coordinator_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '2 minutes'
COMMENT = 'Parent task that coordinates data unload operations every 2 minutes'
AS 
  SELECT 'Data unload coordinator started' as status;

-- Child task for security trades unload
CREATE OR REPLACE TASK security_trades_unload_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Unloads security trade files after coordinator task completes'
AFTER data_unload_coordinator_task
AS 
  CALL security_trades_unload_proc();

-- Child task for cash deposits unload  
CREATE OR REPLACE TASK cash_deposit_unload_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Unloads cash deposit files after coordinator task completes'
AFTER data_unload_coordinator_task
AS 
  CALL cash_deposits_unload_proc();

-- Resume child tasks first, then parent task
ALTER TASK security_trades_unload_task RESUME;
ALTER TASK cash_deposit_unload_task RESUME;
ALTER TASK data_unload_coordinator_task RESUME;

show tasks;
---RESET--
ALTER TASK data_unload_coordinator_task SUSPEND;
ALTER TASK security_trades_unload_task SUSPEND;
ALTER TASK cash_deposit_unload_task SUSPEND;

truncate table finserv_demo.raw_data.cash_deposits;
truncate table finserv_demo.raw_data.security_trades;

update cash_deposit_unload set unload_status = null ,unload_time = null;
update security_trades_unload set unload_status = null , unload_time = null;

select * from cash_deposit_unload where unload_status is not null;
select * from security_trades_unload where unload_status is not null;

REMOVE @cash_deposit_unload_stage ;
REMOVE @security_trade_unload_Stage ;

-----New Date Coordination System that Cursor Suggested----
----****************CENTRALIZED DATE CONTROL***************----
-- Control table to manage synchronized historical data processing starting from 2019-01-02
CREATE OR REPLACE TABLE data_unload.transaction_unload.unload_date_control (
    control_id INTEGER DEFAULT 1,
    current_processing_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    processing_status STRING DEFAULT 'READY'
);

-- Initialize with 2019-01-02 as the starting date for historical data processing
INSERT INTO data_unload.transaction_unload.unload_date_control (current_processing_date)
VALUES ('2019-01-02');

----****************PROC SCRIPTING***************----
CREATE OR REPLACE PROCEDURE security_trades_unload_proc()
RETURNS varchar
LANGUAGE sql
AS 
DECLARE
    date_to_unload date := (SELECT current_processing_date FROM data_unload.transaction_unload.unload_date_control WHERE control_id = 1);
    file_name_string string := 'security_transaction_unload_' || :date_to_unload || '.csv';
    copy_command STRING;
    record_count INTEGER;
BEGIN 
    -- Check if there are any records for this date
    SELECT COUNT(*) INTO :record_count 
    FROM data_unload.transaction_unload.security_trades_unload 
    WHERE transaction_date = :date_to_unload;

    IF (:record_count > 0) THEN
        copy_command := 
            'COPY INTO @data_unload.transaction_unload.security_trade_unload_stage/' || :file_name_string || 
            ' FROM ( ' ||
                'SELECT ' ||
                    'transaction_date '  ||
                    ',transaction_id  ' ||
                    ',to_json(transaction_payload) as transaction_payload ' ||
                'FROM data_unload.transaction_unload.security_trades_unload ' ||
                'WHERE transaction_date = ''' || :date_to_unload || ''' ' ||
            ') ' ||
            'FILE_FORMAT = (format_name = ''csv_unload_file_format'') ' ||
            'OVERWRITE = true ' ||
            'SINGLE = true';

        EXECUTE IMMEDIATE :copy_command;

        UPDATE data_unload.transaction_unload.security_trades_unload 
               SET unload_status = 'SUCCESS' , unload_time = current_timestamp() 
        WHERE transaction_date = :date_to_unload;

        RETURN 'PROCESSED: transaction_date=' || :date_to_unload || ' records=' || :record_count || ' file=' || :file_name_string;
    ELSE
        RETURN 'SKIPPED: transaction_date=' || :date_to_unload || ' - No security trade records for this date';
    END IF;
END;

CALL security_trades_unload_proc();

---CASH DEPOSIT PROC_---
CREATE OR REPLACE PROCEDURE cash_deposits_unload_proc()
RETURNS varchar
LANGUAGE sql
AS 
DECLARE
    date_to_unload date := (SELECT current_processing_date FROM data_unload.transaction_unload.unload_date_control WHERE control_id = 1);
    file_name_string string := 'cash_deposits_unload' || :date_to_unload || '.csv';
    copy_command STRING;
    record_count INTEGER;
BEGIN 
    -- Check if there are any records for this date
    SELECT COUNT(*) INTO :record_count 
    FROM cash_deposit_unload 
    WHERE transaction_date = :date_to_unload;

    IF (:record_count > 0) THEN
        copy_command := 
            'COPY INTO @cash_deposit_unload_stage/' || :file_name_string ||
            ' FROM ( ' ||
                'SELECT transaction_id,customer_id,transaction_date,ticker,deposit_amount,account_type,transaction_category'  ||
                ' FROM cash_deposit_unload ' ||
                ' WHERE transaction_date = ''' || :date_to_unload || ''' ' ||
            ') ' ||
            'FILE_FORMAT = (format_name = ''csv_unload_file_format'') ' ||
            'OVERWRITE = true ' ||
            'SINGLE = true';

        EXECUTE IMMEDIATE :copy_command;

        UPDATE cash_deposit_unload 
               SET unload_status = 'SUCCESS' , unload_time = current_timestamp() 
        WHERE transaction_date = :date_to_unload;

        RETURN 'PROCESSED: transaction_date=' || :date_to_unload || ' records=' || :record_count || ' file=' || :file_name_string;
    ELSE
        RETURN 'SKIPPED: transaction_date=' || :date_to_unload || ' - No cash deposit records for this date';
    END IF;
END;

CALL cash_deposits_unload_proc();

---DATE COORDINATOR PROC---
CREATE OR REPLACE PROCEDURE advance_unload_date_proc()
RETURNS varchar
LANGUAGE sql
AS 
DECLARE
    current_date DATE;  -- Current date being processed in historical timeline (not today's date)
    next_date DATE;
    max_security_date DATE;
    max_cash_date DATE;
BEGIN 
    -- Get current processing date from control table (historical date, not today's date)
    SELECT current_processing_date INTO :current_date 
    FROM data_unload.transaction_unload.unload_date_control 
    WHERE control_id = 1;

    -- Find the maximum available dates to ensure we don't go beyond available data
    SELECT COALESCE(MAX(transaction_date), '1900-01-01') INTO :max_security_date 
    FROM data_unload.transaction_unload.security_trades_unload 
    WHERE unload_time IS NULL;

    SELECT COALESCE(MAX(transaction_date), '1900-01-01') INTO :max_cash_date 
    FROM cash_deposit_unload 
    WHERE unload_time IS NULL;

    -- Calculate next date (advance by 1 day)
    SELECT DATEADD(day, 1, :current_date) INTO :next_date;

    -- Only advance if there's still data to process
    IF (:next_date <= GREATEST(:max_security_date, :max_cash_date)) THEN
        UPDATE data_unload.transaction_unload.unload_date_control 
        SET current_processing_date = :next_date,
            last_updated = current_timestamp()
        WHERE control_id = 1;
        
        RETURN 'Date advanced from ' || :current_date || ' to ' || :next_date;
    ELSE
        RETURN 'No more historical data to process. Current processing date: ' || :current_date || ', Max available: ' || GREATEST(:max_security_date, :max_cash_date);
    END IF;
END;

-- Parent task that handles scheduling - starts the processing chain
CREATE OR REPLACE TASK data_unload_coordinator_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '2 minutes'
COMMENT = 'Parent task that triggers historical data unload processing chain every 2 minutes'
AS 
  SELECT 'Starting data unload processing for current date' as status;

-- Child task for security trades unload (runs first - processes current date)
CREATE OR REPLACE TASK security_trades_unload_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Unloads security trade files for current processing date'
AFTER data_unload_coordinator_task
AS 
  CALL security_trades_unload_proc();

-- Child task for cash deposits unload (runs second - processes current date)
CREATE OR REPLACE TASK cash_deposit_unload_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Unloads cash deposit files for current processing date'
AFTER security_trades_unload_task
AS 
  CALL cash_deposits_unload_proc();

-- Final task that advances the date for next cycle (runs last)
CREATE OR REPLACE TASK advance_date_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Advances processing date to next day after both unload tasks complete'
AFTER cash_deposit_unload_task
AS 
  CALL advance_unload_date_proc();

-- Resume tasks in dependency order (children first, then parent)
ALTER TASK advance_date_task RESUME;
ALTER TASK cash_deposit_unload_task RESUME;
ALTER TASK security_trades_unload_task RESUME;
ALTER TASK data_unload_coordinator_task RESUME;


----****************HELPER PROCEDURES***************----
-- Procedure to check current historical data processing status
CREATE OR REPLACE PROCEDURE check_unload_status()
RETURNS varchar
LANGUAGE sql
AS
BEGIN
    RETURN (
        SELECT 'Historical Processing Date: ' || current_processing_date || 
               ', Last Updated: ' || last_updated || 
               ', Status: ' || processing_status
        FROM data_unload.transaction_unload.unload_date_control 
        WHERE control_id = 1
    );
END;

-- Procedure to reset processing date to a specific date
CREATE OR REPLACE PROCEDURE reset_unload_date(target_date DATE)
RETURNS varchar
LANGUAGE sql
AS
BEGIN
    UPDATE data_unload.transaction_unload.unload_date_control 
    SET current_processing_date = target_date,
        last_updated = current_timestamp(),
        processing_status = 'READY'
    WHERE control_id = 1;
    
    RETURN 'Processing date reset to: ' || target_date;
END;

---RESET EVERYTHING--
ALTER TASK data_unload_coordinator_task SUSPEND;
ALTER TASK security_trades_unload_task SUSPEND;
ALTER TASK cash_deposit_unload_task SUSPEND;
ALTER TASK advance_date_task SUSPEND;

truncate table finserv_demo.raw_data.cash_deposits;
truncate table finserv_demo.raw_data.security_trades;

update cash_deposit_unload set unload_status = null ,unload_time = null;
update security_trades_unload set unload_status = null , unload_time = null;

-- Reset the control table to start from 2019-01-02
UPDATE data_unload.transaction_unload.unload_date_control 
SET current_processing_date = '2019-01-02',
    last_updated = current_timestamp(),
    processing_status = 'READY'
WHERE control_id = 1;

REMOVE @cash_deposit_unload_stage ;
REMOVE @security_trade_unload_Stage ;



