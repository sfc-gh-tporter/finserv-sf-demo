--### HOL Session 4: Data Governance and Security--
--### Automatic Data Classification and Tag-Based Policies--

---PART 1: VIEWING OUR SENSITIVE DATA---
use role fiserv_admin;
use schema finserv_demo.raw_data;
use warehouse data_classification_wh;

---Let's check out our customers table--
select * from customers limit 100;

--We have a couple sensitive columns we'd like to mask!
----SSN: High Sensitivity
----First Name, Last Name, Address, Phone Number: Moderate Sensitivity
----Age, Retirement Age: Low Sensitivity

---PART 2: CREATE CUSTOM TAGS---
--Let's swap to a data_steward role that has the proper permissions for managing data goverance objects--
use role data_steward;

--Snowflake has some out of the box tags that we can apply automatically using data classification methods
show tags in schema snowflake.core;

--Let's create our own tags to help mask this data according to our requirements
create tag if not exists finserv_demo.raw_data.pii_high
    comment = 'Tag for HIGHLY SENSITIVE PII - SSNs';
    
show tags in schema raw_data;

---PART 3: CREATE A CLASSIFICATION PROFILE---
--A classification profle governs how often auto classification runs and how it can tag our objects

create or replace snowflake.data_privacy.classification_profile
    finserv_demo.raw_data.finserv_classification_profile(
        {
            'minimum_object_age_for_classification_days': 0,
            'maximum_classification_validity_days': 30,
            'auto_tag': true
        });

--Add tag map to automatically apply custom tags based on classification results
call finserv_demo.raw_data.finserv_classification_profile!set_tag_map(
    {'column_tag_map':[
        {
            'tag_name':'finserv_demo.raw_data.pii_low',
            'tag_value':'age',
            'semantic_categories':['AGE']
        },
        {
            'tag_name':'finserv_demo.raw_data.pii_low',
            'tag_value':'date',
            'semantic_categories':['DATE_OF_BIRTH']
        },
        {
            'tag_name':'finserv_demo.raw_data.pii_moderate',
            'tag_value':'name',
            'semantic_categories':['NAME']
        },
        {
            'tag_name':'finserv_demo.raw_data.pii_moderate',
            'tag_value':'address',
            'semantic_categories':['STREET_ADDRESS']
        },
        {
            'tag_name':'finserv_demo.raw_data.pii_moderate',
            'tag_value':'phone',
            'semantic_categories':['PHONE_NUMBER']
        },
        {
            'tag_name':'finserv_demo.raw_data.pii_high',
            'tag_value':'social_security_number',
            'semantic_categories':['NATIONAL_IDENTIFIER']
        }
    ]});

--Inspect the map to ensure the tag map and other parameters are applied correctly
SELECT finserv_demo.raw_data.finserv_classification_profile!DESCRIBE();

--Assign to the schema to use the finserv_classification_profile when running auto classification
alter schema finserv_demo.raw_data set classification_profile = 'finserv_classification_profile';

---PART 4: MANUAL CLASSIFICATION ---
--For demo purposes, manually trigger classification 
--Classification runs automatically within 1 hour for new tables
--Can be scheduled faster with tasks

--Manually classify existing tables for immediate results
call system$classify('finserv_demo.raw_data.customers', 'finserv_demo.raw_data.finserv_classification_profile');
call system$classify('finserv_demo.raw_data.advisors', 'finserv_demo.raw_data.finserv_classification_profile');

---PART 5: VIEW CLASSIFICATION RESULTS---
--View classification results for CUSTOMERS table
call system$get_classification_result('finserv_demo.raw_data.customers');

--View classification results for ADVISORS table  
call system$get_classification_result('finserv_demo.raw_data.advisors');


SELECT object_name, column_name,tag_name, tag_value, apply_method
FROM (SELECT *
        FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
      'finserv_demo.raw_data.advisors','table'
    )) 
UNION 
SELECT *
    FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
      'finserv_demo.raw_data.customers','table'
    )))
where tag_name LIKE 'PII%'
order by object_name, column_name,tag_name;

---PART 6: CREATE MASKING POLICYs & ASSIGN TAGs---
--Masking policy for SSN (different masking per role)
create or replace masking policy ssn_mask as (val string) returns string ->
    case 
        when current_role() in ('DATA_ENGINEER_RL', 'BUSINESS_ANALYST_RL','DATA_STEWARD','FISERV_ADMIN') then '*** Redacted ***'
        when current_role() = 'CLIENT_ADVISOR_RL' then regexp_replace(val, '^.{3}-.{2}', '***-**')
        else val
    end;

alter tag finserv_demo.raw_data.pii_high 
    set masking policy finserv_demo.raw_data.ssn_mask ;

show masking policies;

--Query Acccount usage to check existins policies and their tag associations---
--Delay of ~ 2 hours-- (won't see recent applications)
SELECT policy_name
      ,policy_kind
      ,policy_status
      ,tag_name
      ,ref_entity_name
      ,ref_entity_domain
FROM snowflake.account_usage.policy_references
WHERE policy_schema = 'RAW_DATA';

--Check the masking policy--
select first_name,last_name,phone_number,ssn,zip_code,state 
from customers limit 100;

select first_name,last_name,street_address,zip_code,state 
from advisors limit 100;

--Swap roles--
use role sysadmin;
select first_name,last_name,phone_number,ssn,zip_code,state 
from customers limit 100;


---PART 7: ROW ACCESS POLICY---
--Create row access policy for CLIENT_ADVISOR_RL to only see their customers
use role data_steward;
use schema finserv_demo.raw_data;

--Create row access policy for customers table using a mapping table--

--this is an acutal test user I created that would represent an advisor logging into Snowflake
use role accountadmin;
show users like '%DFLORES%';

--Check mapping table--
select * from map.advisor_users;

use role data_steward;
create row access policy if not exists customer_advisor_policy as (customer_id number) returns boolean ->
    case
        when current_role() in ('DATA_STEWARD', 'DATA_ENGINEER_RL', 'BUSINESS_ANALYST_RL', 'FINSERV_ADMIN') then true
        when current_role() = 'CLIENT_ADVISOR_RL' then 
            exists (
                select 1 from finserv_demo.map.customer_advisor ca
                inner join finserv_demo.map.advisor_users au on ca.advisor_id = au.advisor_id
                where ca.customer_id = customer_id 
                and au.snowflake_username = current_user()
            )
        else true
    end;

--Apply row access policy to customers table
alter table finserv_demo.raw_data.customers add row access policy customer_advisor_policy on (customer_id);


select * from customers limit 100;
--As expected, we can see all rows with the coluimn masking applied
--To see the row access policy in action, we'll need to swap to a user that 

--Luckily, snowflake has a handy POLICY_CONTEXT function that will let us simiulate a user---
use role sysadmin;
execute using POLICY_CONTEXT(CURRENT_USER => 'DFLORES'
                            ,CURRENT_ROLE => 'CLIENT_ADVISOR_RL')
  AS select first_name, last_name
          , age
          , ssn
          , phone_number
          , state
          , customer_type
          , retirement_stage
          , personal_wealth_portfolio
          , retirement_fund
    from advisor_workspace.customer_overview 
    order by age ;




---RESET
use role data_steward;

alter tag finserv_demo.raw_data.pii_high unset masking policy finserv_demo.raw_data.ssn_mask ;
drop masking policy ssn_mask;

alter schema finserv_demo.raw_data unset classification_profile;
drop  snowflake.data_privacy.classification_profile finserv_classification_profile;

alter table finserv_demo.raw_data.customers drop row access policy customer_advisor_policy;
drop row access policy customer_advisor_policy;

drop tag finserv_demo.raw_data.pii_high;

----UNSET TAGs---
--Advisor Core Tags--
alter table advisors alter column ADVISOR_ID unset tag snowflake.core.privacy_category;
alter table advisors alter column ADVISOR_ID unset tag snowflake.core.semantic_category;
alter table advisors alter column ADVISOR_START_DATE unset tag snowflake.core.privacy_category;
alter table advisors alter column ADVISOR_START_DATE unset tag snowflake.core.semantic_category;
alter table advisors alter column AGE unset tag snowflake.core.privacy_category;
alter table advisors alter column AGE unset tag snowflake.core.semantic_category;
alter table advisors alter column COUNTRY unset tag snowflake.core.privacy_category;
alter table advisors alter column COUNTRY unset tag snowflake.core.semantic_category;
alter table advisors alter column FIRST_NAME unset tag snowflake.core.privacy_category;
alter table advisors alter column FIRST_NAME unset tag snowflake.core.semantic_category;
alter table advisors alter column LAST_NAME unset tag snowflake.core.privacy_category;
alter table advisors alter column LAST_NAME unset tag snowflake.core.semantic_category;
alter table advisors alter column PHONE_NUMBER unset tag snowflake.core.privacy_category;
alter table advisors alter column PHONE_NUMBER unset tag snowflake.core.semantic_category;
alter table advisors alter column STATE unset tag snowflake.core.privacy_category;
alter table advisors alter column STATE unset tag snowflake.core.semantic_category;
alter table advisors alter column STREET_ADDRESS unset tag snowflake.core.privacy_category;
alter table advisors alter column STREET_ADDRESS unset tag snowflake.core.semantic_category;
alter table advisors alter column ZIP_CODE unset tag snowflake.core.privacy_category;
alter table advisors alter column ZIP_CODE unset tag snowflake.core.semantic_category;

--Advisor Custom Tags--
alter table advisors alter column  first_name unset tag finserv_demo.raw_data.pii_moderate;
alter table advisors alter column  last_name unset tag finserv_demo.raw_data.pii_moderate;
alter table advisors alter column  phone_number unset tag finserv_demo.raw_data.pii_moderate;
alter table advisors alter column  street_address unset tag finserv_demo.raw_data.pii_moderate;
alter table advisors alter column  age unset tag finserv_demo.raw_data.pii_low;

--Customers Core Tags--
alter table customers alter column AGE unset tag snowflake.core.privacy_category;
alter table customers alter column AGE unset tag snowflake.core.semantic_category;
alter table customers alter column BIRTHDATE unset tag snowflake.core.privacy_category;
alter table customers alter column BIRTHDATE unset tag snowflake.core.semantic_category;
alter table customers alter column COUNTRY unset tag snowflake.core.privacy_category;
alter table customers alter column COUNTRY unset tag snowflake.core.semantic_category;
alter table customers alter column CUSTOMER_ID unset tag snowflake.core.privacy_category;
alter table customers alter column CUSTOMER_ID unset tag snowflake.core.semantic_category;
alter table customers alter column CUSTOMER_TYPE unset tag snowflake.core.privacy_category;
alter table customers alter column CUSTOMER_TYPE unset tag snowflake.core.semantic_category;
alter table customers alter column DEPARTMENT unset tag snowflake.core.privacy_category;
alter table customers alter column DEPARTMENT unset tag snowflake.core.semantic_category;
alter table customers alter column EMPLOYEE_ID unset tag snowflake.core.privacy_category;
alter table customers alter column EMPLOYEE_ID unset tag snowflake.core.semantic_category;
alter table customers alter column EMPLOYER_ID unset tag snowflake.core.privacy_category;
alter table customers alter column EMPLOYER_ID unset tag snowflake.core.semantic_category;
alter table customers alter column FIRST_NAME unset tag snowflake.core.privacy_category;
alter table customers alter column FIRST_NAME unset tag snowflake.core.semantic_category;
alter table customers alter column HIRE_DATE unset tag snowflake.core.privacy_category;
alter table customers alter column HIRE_DATE unset tag snowflake.core.semantic_category;
alter table customers alter column JOB_TITLE unset tag snowflake.core.privacy_category;
alter table customers alter column JOB_TITLE unset tag snowflake.core.semantic_category;
alter table customers alter column LAST_NAME unset tag snowflake.core.privacy_category;
alter table customers alter column LAST_NAME unset tag snowflake.core.semantic_category;
alter table customers alter column PHONE_NUMBER unset tag snowflake.core.privacy_category;
alter table customers alter column PHONE_NUMBER unset tag snowflake.core.semantic_category;
alter table customers alter column REPORTED_RISK_LEVEL unset tag snowflake.core.privacy_category;
alter table customers alter column REPORTED_RISK_LEVEL unset tag snowflake.core.semantic_category;
alter table customers alter column RETIRED unset tag snowflake.core.privacy_category;
alter table customers alter column RETIRED unset tag snowflake.core.semantic_category;
alter table customers alter column RETIREMENT_AGE unset tag snowflake.core.privacy_category;
alter table customers alter column RETIREMENT_AGE unset tag snowflake.core.semantic_category;
alter table customers alter column RETIREMENT_DATE unset tag snowflake.core.privacy_category;
alter table customers alter column RETIREMENT_DATE unset tag snowflake.core.semantic_category;
alter table customers alter column RISK_LEVEL_DESCRIPTION unset tag snowflake.core.privacy_category;
alter table customers alter column RISK_LEVEL_DESCRIPTION unset tag snowflake.core.semantic_category;
alter table customers alter column SALARY unset tag snowflake.core.privacy_category;
alter table customers alter column SALARY unset tag snowflake.core.semantic_category;
alter table customers alter column SSN unset tag snowflake.core.privacy_category;
alter table customers alter column SSN unset tag snowflake.core.semantic_category;
alter table customers alter column START_DATE unset tag snowflake.core.privacy_category;
alter table customers alter column START_DATE unset tag snowflake.core.semantic_category;
alter table customers alter column STATE unset tag snowflake.core.privacy_category;
alter table customers alter column STATE unset tag snowflake.core.semantic_category;
alter table customers alter column STREET_ADDRESS unset tag snowflake.core.privacy_category;
alter table customers alter column STREET_ADDRESS unset tag snowflake.core.semantic_category;
alter table customers alter column ZIP_CODE unset tag snowflake.core.privacy_category;
alter table customers alter column ZIP_CODE unset tag snowflake.core.semantic_category;

--Customer Custom Tags--
alter table customers alter column retirement_age unset tag finserv_demo.raw_data.pii_low;
alter table customers alter column age unset tag finserv_demo.raw_data.pii_low;
alter table customers alter column birthdate unset tag finserv_demo.raw_data.pii_low;
alter table customers alter column first_name unset tag finserv_demo.raw_data.pii_moderate;
alter table customers alter column last_name unset tag finserv_demo.raw_data.pii_moderate;
alter table customers alter column phone_number unset tag finserv_demo.raw_data.pii_moderate;
alter table customers alter column ssn unset tag finserv_demo.raw_data.pii_high;


--Validate
SELECT object_name, column_name, tag_name, tag_value
FROM (
SELECT *
FROM
  TABLE(
    INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
      'finserv_demo.raw_data.advisors',
      'table'
    )) 
UNION 
SELECT *
FROM
  TABLE(
    INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
      'finserv_demo.raw_data.customers',
      'table'
    )))
where tag_name <> 'BUSINESS_UNIT'    
order by object_name;




