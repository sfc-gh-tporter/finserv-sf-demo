---Setup for HOL Demo---

use role sysadmin;

--Create advisor-to-user mapping table for row access policy
create or replace table finserv_demo.map.advisor_users (
    snowflake_username varchar(100),
    advisor_id number(38,0),
    advisor_first_name varchar(100),
    advisor_last_name varchar(100)
);

--Insert mapping for demo user (extend this for additional advisors)
insert into finserv_demo.map.advisor_users values 
    ('DFLORES', 1, 'David', 'Flores');

--Create WH for classification runs
create or replace warehouse data_classification_wh with 
    warehouse_size = 'small' 
    auto_suspend = 60 
    auto_resume = true
    comment = 'Warehouse for data governance operations';

alter tag tags.public.business_unit add allowed_values 'Data_Governance', 'Data_Quality ';

alter warehouse data_classification_wh set tag tags.public.business_unit = 'Data_Governance';

--Create WH for client aanaylst  runs
create or replace warehouse client_analyst_wh with 
    warehouse_size = 'xsmall' 
    auto_suspend = 60 
    auto_resume = true
    comment = 'Warehouse for client-advisor and analyst operations';
    
alter warehouse client_analyst_wh set tag tags.public.business_unit = 'Analytics';

create or replace warehouse it_dev_wh with 
    warehouse_size = 'xsmall' 
    auto_suspend = 60 
    auto_resume = true
    comment = 'Warehouse for IT and Developer Operations';
    
alter warehouse it_dev_wh set tag tags.public.business_unit = 'CentralIt';

use role accountadmin;

create or replace role data_steward;
grant role data_steward to role sysadmin;
grant role data_steward to user tporter_sfc;

GRANT EXECUTE AUTO CLASSIFICATION ON SCHEMA finserv_demo.raw_data TO ROLE data_steward;
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE data_steward;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE ON SCHEMA finserv_demo.raw_data TO ROLE data_steward;

--Create business roles
create or replace role data_engineer_rl;
create or replace role business_analyst_rl;
create or replace role client_advisor_rl;
grant role data_engineer_rl to role sysadmin;
grant role business_analyst_rl to role sysadmin;
grant role client_advisor_rl to role sysadmin;

grant role data_engineer_rl to user tporter_sfc;
grant role business_analyst_rl to user tporter_sfc;
grant role client_advisor_rl to user tporter_sfc;

--Allow our data steward to apply tags globally
grant apply tag on account to role data_steward;
--Allow our data steward to apply maksing policies globally
grant apply masking policy on account to role data_steward;

--Allow for querying for useful objects in snowflake.account_usage
grant imported privileges on database snowflake to role data_steward;


use role sysadmin;
--Grant warehouse access
grant usage on warehouse data_classification_wh to role data_steward;
grant usage on warehouse it_dev_wh to role data_engineer_rl;
grant usage on warehouse client_analyst_wh to role business_analyst_rl;
grant usage on warehouse client_analyst_wh to role client_advisor_rl;

--Grant database and schema access
grant usage on database finserv_demo to role data_steward;
grant usage on database finserv_demo to role data_engineer_rl;
grant usage on database finserv_demo to role business_analyst_rl;
grant usage on database finserv_demo to role client_advisor_rl;


grant usage on all schemas in database finserv_demo to role data_steward;
revoke usage on schema finserv_demo.gh_util from role data_steward;

grant all on schema finserv_demo.raw_data to role data_steward;
grant create masking policy on schema finserv_demo.raw_data to role data_steward;

grant all on all functions in schema finserv_demo.information_schema to role data_steward;

grant create snowflake.data_privacy.classificiation on schema finserv_demo.raw_data to role data_steward;
grant execute auto classification on schema finserv_demo.raw_data to role data_steward;

grant all on schema finserv_demo.information_schema to role data_steward;

grant usage on schema finserv_demo.raw_data to role data_engineer_rl;
--grant usage on schema finserv_demo.raw_data to role business_analyst_rl;
--grant usage on schema finserv_demo.raw_data to role client_advisor_rl;
--grant usage on schema finserv_demo.map to role client_advisor_rl;

--Grant table access
grant select on all tables in schema finserv_demo.raw_data to role data_engineer_rl; 

--Grant comprehensive data governance permissions to DATA_STEWARD
--grant create table on schema finserv_demo.raw_data to role data_steward;
grant create tag on schema finserv_demo.raw_data to role data_steward;
grant all on all tables in schema finserv_demo.raw_data to role data_steward;
grant all on all views in schema finserv_demo.raw_data to role data_steward;


----


use role data_steward;

--Tag Pre Creation----
create tag if not exists finserv_demo.raw_data.pii_moderate
    comment = 'Tag for MODERATELY SENSITIVE PII - Address, Name, Phone Number';

create tag if not exists finserv_demo.raw_data.pii_low 
    comment = 'Tag for LOW SENSITIVE PII data type- Age, Birthdate';


use schema finserv_demo.raw_data;
show masking policies;

--ACCOUNTADMIN
--SYSADMIN
--BUSINESS_ANALYST_RL
--CLIENT_ADVISOR_RL
--DATA_ENGINEER_RL
--DATA_STEWARD
--FISERV_ADMIN

--Masking Policies and Tags---
/* SET DURING DEMO EXECUTION 
--Masking policy for SSN (different masking per role)
create or replace masking policy ssn_mask as (val string) returns string ->
    case 
        when current_role() in ('DATA_ENGINEER_RL', 'BUSINESS_ANALYST_RL','DATA_STEWARD','FISERV_ADMIN') then '*** Redacted ***'
        when current_role() = 'CLIENT_ADVISOR_RL' then regexp_replace(val, '^.{3}-.{2}', '***-**')
        else val
    end;

alter tag finserv_demo.raw_data.pii_sensitive 
    set masking policy finserv_demo.raw_data.ssn_mask ;
    
*/

---Set during set up to reduce SQL to run in DEMO ---
use role   data_steward;
use schema finserv_demo.raw_data;

show tags;
--Masking | Varchar Moderate--
create or replace masking policy varchar_moderate as (val string) returns string ->
    case 
        when current_role() in ('CLIENT_ADVISOR_RL') then val
        when current_role() in ('DATA_ENGINEER_RL', 'BUSINESS_ANALYST_RL','FISERV_ADMIN','DATA_STEWARD') then '*** Redacted ***'
        else val -- Else handels system roles (Accountadmin, Sysadmin) --For demo purposes we are not masking them
    end
    comment = 'VarChar Masking Policy for moderately senstive data.';
    
    --Apply to proper tag--
alter tag finserv_demo.raw_data.pii_moderate 
    set masking policy finserv_demo.raw_data.varchar_moderate ;


--Masking policy for age data 
create or replace masking policy age_mask as (val number) returns number ->
    case 
        when current_role() in ('CLIENT_ADVISOR_RL','BUSINESS_ANALYST_RL') then val --useful for advisors and analysts
        when current_role() in ('DATA_ENGINEER_RL','FISERV_ADMIN','DATA_STEWARD') then null
        else val -- Else handles system roles (Accountadmin, Sysadmin) --For demo purposes we are not masking them
    end;

    --Apply to proper tag--
alter tag finserv_demo.raw_data.pii_low
    set masking policy finserv_demo.raw_data.age_mask ;


--Masking policy for date date--
create or replace masking policy date_mask as (val date) returns date ->
    case 
        when current_role() in ('CLIENT_ADVISOR_RL','BUSINESS_ANALYST_RL') then val --useful for advisors and analysts
        when current_role() in ('DATA_ENGINEER_RL','FISERV_ADMIN','DATA_STEWARD') then null
        else val -- Else handles system roles (Accountadmin, Sysadmin) --For demo purposes we are not masking them
    end;

    --Apply to proper tag--
alter tag finserv_demo.raw_data.pii_low
    set masking policy finserv_demo.raw_data.date_mask ;

create or replace schema advisor_workspace;

create or replace view finserv_demo.advisor_workspace.customer_overview as
select 
    -- Customer Information (automatically masked based on role and user)
    c.customer_type,
    c.first_name,
    c.last_name,
    c.age,
    c.birthdate,
    c.retirement_date,
    c.retired,
    c.state,
    c.phone_number,
    c.ssn,
    c.job_title,
    c.salary,
        -- Calculated Fields for Advisor Insights
    case 
        when c.retired = true then 'Retired'
        when rp.years_to_fund_target <= 5 then 'Near Retirement'
        when rp.years_to_fund_target <= 15 then 'Mid Career'
        else 'Early Career'
    end as retirement_stage,
    -- Retirement Plan Details
    rp.retirement_year,
    rp.assigned_fund_name as retirement_fund,
    
    -- Risk and Portfolio Information
    c.risk_level_description as risk_level,
    pc.portfolio_description as personal_wealth_portfolio
 
        
    
from finserv_demo.raw_data.customers c
left join finserv_demo.map.customer_portfolio cp on c.customer_id = cp.customer_id and cp.active = true
left join finserv_demo.dims.dim_portfolio_composition pc on cp.portfolio_id = pc.portfolio_id
left join finserv_demo.map.customer_retirement_plans rp on c.customer_id = rp.customer_id
left join finserv_demo.dims.dim_retirement_plan_funds rpf on rp.assigned_fund_ticker = rpf.ticker;




--Grants--
grant usage on schema advisor_workspace to role client_advisor_rl;
grant usage on schema advisor_workspace to role fiserv_admin; 
grant select on all views in schema advisor_workspace to role client_advisor_rl;
grant select on all views in schema advisor_workspace to role fiserv_admin;

use role accountadmin;
grant select on future views in schema advisor_workspace to role client_advisor_rl;
grant select on future views in schema advisor_workspace to role fiserv_admin;

alter user dflores set default_warehouse = 'CLIENT_ANALYST_WH';
alter user dflores set default_namespace = 'FINSERV_DEMO.ADVISOR_WORKSPACE';
alter user dflores set default_role = 'CLIENT_ADVISOR_RL';

grant usage on schema map to role data_steward;
grant select on table map.customer_advisor to role data_steward;
grant select on table map.advisor_users to role data_steward;

GRANT APPLY ROW ACCESS POLICY ON ACCOUNT to role data_steward;