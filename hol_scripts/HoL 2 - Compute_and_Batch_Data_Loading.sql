--### HOL Session 2: Compute Management & Data Loading--

---Set Up Context--
---Where are we? What compute are we using?
use role sysadmin;
use schema finserv_demo.raw_data;
use warehouse snowflake_se_wh;

--Exploring our schema
show tables in schema;

desc table customers;

select * from customers;
--Empty! 

---Let's load some data!
---First we need some compute

show warehouses;
desc warehouse data_ingestion_wh;

create warehouse if exists data_ingestion_wh
    with warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true ;

--We can apply a TAG to help attribute cost to a business unit
ALTER WAREHOUSE data_ingestion_wh SET TAG tags.public.business_unit = 'Central_IT';

--We will review this usage in the UI in a later sessions--

---DATA LOADING--
use warehouse data_ingestion_wh;

--Where do we load batch data from? 
--External Stages that live in S3 (or other cloud object storage)
show stages like '%customer%';

--Listing Files in a Stage--
ls @raw_data.customer_data_stage;

--Querying Data in a Stage

--First we must tell Snowflake what format of data we are working with--
create or replace file format raw_data.parquet_format 
    type = 'PARQUET';

SELECT $1 
FROM @raw_data.customer_data_stage
    (FILE_FORMAT => 'raw_data.parquet_format')
LIMIT 100;

--Transform Data From Stage--
SELECT $1:CUSTOMER_ID::INT as customer_id
      ,$1:FIRST_NAME::STRING as first_name
      ,$1:SALARY::NUMBER(12,2) as salary
FROM @raw_data.customer_data_stage
    (FILE_FORMAT => 'raw_data.parquet_format')
LIMIT 100;

-- We can query the INFER_SCHEMA function to get a full picture of the parquet schema
select * 
from table (
    infer_schema(
     location => '@raw_data.customer_data_stage'
     ,file_format =>'parquet_format'
     )
    );

---Lets copy data into this table-
--We already have a table, so we'll just run a standard copy into
copy into raw_data.customers
from @raw_data.customer_data_stage
file_format = (format_name = 'parquet_format')
match_by_column_name = 'CASE_INSENSITIVE'; -- automatically matches the infered schema to colums in our table 

--Done!
--Querying the data
select * from raw_data.customers;

--What other data do we have to load?
ls @retirement_contributions_data_stage;

--We can see that the data is broken up into folders with "new_schema" and "orginal_schema" prefixes
SELECT CASE WHEN $1 like '%new_schema%' THEN 'new_schema'
            WHEN $1 like '%original_schema%' THEN 'original_schema'
            END AS SCHEMA_TYPE
           ,SUM($2)   as total_bytes
           ,COUNT($1) as num_of_files
FROM table(result_scan(last_query_id()))
GROUP BY ALL;

--Let's create a table using schema inference for the original schema--
CREATE OR REPLACE TABLE raw_data.retirement_contributions
  USING TEMPLATE (
    SELECT ARRAY_AGG(object_construct(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@retirement_contributions_data_stage/original_schema/',
          FILE_FORMAT=>'parquet_format'
        )
      ));

--Copy Into
copy into raw_data.retirement_contributions
from @retirement_contributions_data_stage/original_schema/
file_format = (format_name = 'parquet_format')
match_by_column_name = 'CASE_INSENSITIVE';

--Check the Data
SELECT * 
FROM raw_data.retirement_contributions
LIMIT 100;

--What is the different in schema compared to the new schema staged files?
select * 
from table (
    infer_schema(
     location => '@retirement_contributions_data_stage/new_schema/'
     ,file_format =>'parquet_format'
     )
    );

---There is a new column: CONTRIBUTION_SCHEDULE
---We can natively handle changing schemas using EVOLVE SCHEMA

alter table retirement_contributions set enable_schema_evolution = true;

--Copy into--
copy into raw_data.retirement_contributions
from @retirement_contributions_data_stage/new_schema/
file_format = (format_name = 'parquet_format')
match_by_column_name = 'CASE_INSENSITIVE';

--The new column will be added
select * 
from retirement_contributions 
limit 100;

----

--Reset: 
truncate table customers;
drop table retirement_contributions;
