--### HOL Session 1: Snowflake 101 & Intro to User & Role Management--

---Set Up Context--
---Where are we? What compute are we using?

use schema finserv_demo.raw_data;
use warehouse snowflake_se_wh;

---System Roles---
use role sysadmin;
show roles;

---ACCOUNTADMIN: Highest permissioned role in each Snowflake Account
---SYSADMIN: De Facto "DBA" or "IT" role. Can create data & compute objects
---SECURITYADMIN:  Can grant permissions to other roles 
---USERADMIN: Creates users
---PUBLIC: Default role for all users. Has no permissions. Grant something to public if you want it to be globally available

use role accountadmin;
--View Role Hierarchy Chart
--Goverment & Security --> Users & Roles -->  Roles --> Click "Graph" Tab

--Custome Role Creation
create or replace role fiserv_admin;
grant role fiserv_admin to user tporter_sfc;
grant role fiserv_admin to role sysadmin;

use role sysadmin;

--Warehouse Permissions
grant all on warehouse data_ingestion_wh to role fiserv_admin;
grant all on warehouse data_transformation_wh to role fiserv_admin;

--Database Permissions--
grant usage on database finserv_demo;

--Schema Permissions--
grant all on all schemas in database finserv_demo;

--Schema Object Permissions--
grant all on all tables in schema finserv_demo.raw_data;
grant all on all tables in schema finserv_demo.dims;
grant all on all tables in schema finserv_demo.map;

--Future Grants--
---useful for preemptively appling permissions 
---simplifies object permissions management
grant all on future schemas in database finserv_demo to role fiserv_admin;

grant all on future tables in schema finserv_demo.raw_data to role fiserv_admin;
grant all on future views  in schema finserv_demo.raw_data to role fiserv_admin;

grant all on future tables in schema finserv_demo.dims to role fiserv_admin;
grant all on future views  in schema finserv_demo.dims to role fiserv_admin;

grant all on future tables in schema finserv_demo.map to role fiserv_admin;
grant all on future views  in schema finserv_demo.map to role fiserv_admin;

--Users--
show users in account;

use role accountadmin;

--Just Me! Let's add someone
create or replace user ANEEL_SFC
    display_name = 'Adam Neel'
    email = 'XXXXXXXXXXXX'
    password = 'XXXXXXXX'
    default_role = 'SYSADMIN'
    default_warehouse = 'snowflake_se_wh'
    must_change_password = TRUE;

-- Create Additonal Users Via the UI
-- Get list of users to create before next session


--Reset--
use role accountadmin;
drop role fiserv_admin;
drop user aneel_sfc;




