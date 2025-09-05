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
 credentials = (AWS_KEY_ID='****************' AWS_SECRET_KEY='*****************')
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
 credentials = (AWS_KEY_ID='********************' AWS_SECRET_KEY='********************')
 encryption = (type = 'none');


--TEST UNLOAD--
COPY INTO @data_unload.transaction_unload.cash_deposit_unload/
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
            'FROM cash_deposit_unload ' ||
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

CALL cash_deposits_unload_proc();


CREATE OR REPLACE TASK security_trades_unload_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '2 minutes'
COMMENT = 'Unloads files every 2 minutes for ingestion in our other pipeline'
AS 
  CALL security_trades_unload_proc();

CREATE OR REPLACE TASK cash_deposit_unload_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '2 minutes'
COMMENT = 'Unloads files every 2 minutes for ingestion in our other pipeline'
AS 
  CALL cash_deposits_unload_proc();

ALTER TASK security_trades_unload_task RESUME;
ALTER TASK cash_deposit_unload_task RESUME;


---RESET--
ALTER TASK security_trades_unload_task SUSPEND;
ALTER TASK cash_deposit_unload_task SUSPEND;

truncate table finserv_demo.raw_data.cash_deposits;
truncate table finserv_demo.raw_data.security_trades;

update cash_deposit_unload set unload_status = null ,unload_time = null;
update security_trades_unload set unload_status = null , unload_time = null;


REMOVE @cash_deposit_unload_stage ;
REMOVE @security_trade_unload_Stage ;

-------

