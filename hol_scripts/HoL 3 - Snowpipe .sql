----Ingestion Objcts--
USE ROLE sysadmin;
USE SCHEMA finserv_demo.raw_data;

--FILE FORMAT---
CREATE OR REPLACE FILE FORMAT finserv_demo.raw_data.csv_ingest_ff
    TYPE = CSV
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = 'n'
    ESCAPE_UNENCLOSED_FIELD = NONE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FILE_EXTENSION = 'csv';
    

--SECURITY TRADE INGEST OBJECTS
--STAGE---
CREATE OR REPLACE STAGE finserv_demo.raw_data.security_trades_inbound_stage
 url= 's3://tporter-demo-root/finserv-demo-stage/security_trades/'
 credentials = (AWS_KEY_ID='******************' AWS_SECRET_KEY='*****************')
 encryption = (type = 'none')
 file_format = (format_name = finserv_demo.raw_data.csv_ingest_ff);

LS @finserv_demo.raw_data.security_trades_inbound_stage;

SELECT $1::date    as transaction_date
     , $2::varchar as transaction_id
     , parse_json($3) as transaction_payload
 FROM @security_trades_inbound_stage;

create or replace table finserv_demo.raw_data.security_trades 
  (transaction_date date
  ,transaction_id varchar
  ,transaction_payload variant);
  

COPY INTO finserv_demo.raw_data.security_trades (transaction_date,transaction_id,transaction_payload) 
    FROM (SELECT stg.$1::date
                ,stg.$2::string 
                ,parse_json(stg.$3)
          FROM @finserv_demo.raw_data.security_trades_inbound_stage stg);

SELECT * FROM security_trades LIMIT 100;

---Pipe--
CREATE OR REPLACE PIPE raw_data.security_trades_pipe
AUTO_INGEST = true
AS 
COPY INTO finserv_demo.raw_data.security_trades (transaction_date,transaction_id,transaction_payload) 
    FROM (SELECT stg.$1::date
                ,stg.$2::string 
                ,parse_json(stg.$3)
          FROM @finserv_demo.raw_data.security_trades_inbound_stage stg);

desc pipe raw_data.security_trades_pipe;
---arn:aws:sqs:us-east-1:016311861695:sf-snowpipe-AIDAQHTCDIG7VSKM626H2-WQjBb19YOBGOHJ17pbYTIQ

------CASH DEPOSIT OBJECTS-----
---STAGE--
CREATE OR REPLACE STAGE finserv_demo.raw_data.cash_deposits_inbound_stage
 url= 's3://tporter-demo-root/finserv-demo-stage/cash_deposits/'
 credentials = (AWS_KEY_ID='******************' AWS_SECRET_KEY='******************')
 encryption = (type = 'none')
 file_format = (format_name = csv_ingest_ff);


CREATE OR REPLACE TABLE cash_deposits (
    deposit_id  varchar(16777216),
	customer_id number(38,0),
	transaction_date timestamp_ntz(9),
	ticker varchar(16777216),
	deposit_amount number(38,2),
	account_type_name varchar(25),
	transaction_category varchar(16777216)
);

SELECT $1,$2,$3,$4,$5,$6,$7,$8
FROM @cash_deposits_inbound_stage; 

COPY INTO cash_deposits FROM @cash_deposits_inbound_stage;

--Pipe--
CREATE OR REPLACE PIPE raw_data.cash_deposits_pipe
AUTO_INGEST = true
AS 
COPY INTO cash_deposits FROM @cash_deposits_inbound_stage;

SELECT transaction_date,count(*) FROM security_trades GROUP BY ALL;

SELECT transaction_date,count(*) FROM cash_deposits GROUP BY ALL;

SELECT SYSTEM$PIPE_STATUS('raw_data.cash_deposits_pipe');
SELECT SYSTEM$PIPE_STATUS('raw_data.security_trades_pipe');