--### HoL 5.1: Data Engineering - Streams & Tasks Pipeline ###--
--### Building Real-time Holdings Tables using Streams and Tasks ###--

---Prerequisites---
--1. Complete HoL 5.0 to set up your user-specific development environment
--2. Ensure you have data in cash_deposits and security_trades tables
--3. Each user will work in their own database: finserv_demo_USERNAME

---Set Up Context---
USE ROLE sysadmin;
USE WAREHOUSE data_transformation_wh;

-- Let's make sure we are in our user specific cloned database
SET current_username = CURRENT_USER();
SET user_db_name = (SELECT 'finserv_demo_' || REPLACE($current_username, '.', '_') || '_dev_clone');

USE DATABASE IDENTIFIER($user_db_name);
USE SCHEMA raw_data;

---PART 1: CREATE STREAMS FOR CHANGE DATA CAPTURE---
--Streams capture changes (inserts, updates, deletes) from source tables
--SHOW_INITIAL_ROWS = TRUE includes existing data in the first stream read
--This is important for initial processing of historical data

USE SCHEMA raw_data;

CREATE OR REPLACE STREAM cash_deposits_stream 
ON TABLE cash_deposits
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM security_trades_stream 
ON TABLE security_trades  
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE;

--Verify streams are created and show initial data availability
SHOW STREAMS;
SELECT SYSTEM$STREAM_HAS_DATA('cash_deposits_stream') as cash_stream_has_data;
SELECT SYSTEM$STREAM_HAS_DATA('security_trades_stream') as trades_stream_has_data;

---PART 2: PARSING JSON---
--Let's take a look at this json schema in raw_data.security_trades

SELECT * 
FROM raw_data.security_trades
LIMIT 100;

--Parse out some values in the payload-
SELECT  st_str.transaction_date,
        st_str.transaction_id,
        st_str.transaction_payload:customer_id::INT as customer_id,
        st_str.transaction_payload:account_type::STRING as account_type,
        st_str.transaction_payload as payload,
        st_str.metadata$action,
        st_str.metadata$row_id
FROM raw_data.security_trades_stream st_str -- Selected from the stream this time!
WHERE transaction_id = '5a1b2dba6a198a23b47c1079a6e68852';

--Flattening our rows into trades
SELECT  st_str.transaction_date,
        st_str.transaction_id,
        st_str.transaction_payload:customer_id::INT as customer_id,
        st_str.transaction_payload:account_type::STRING as account_type,
        t.value:action::STRING as trade_action,
        t.value:ticker::STRING as ticker,
        t.value:shares::INT as shares,
        t.value:trade_price::DECIMAL(12,2) as trade_price,
        t.value:total_trade_value::DECIMAL(12,2) as total_trade_value,
        st_str.transaction_payload as payload,
        st_str.metadata$action,
        st_str.metadata$row_id
FROM raw_data.security_trades_stream st_str -- Selected from the stream this time!
        ,LATERAL FLATTEN(input => st_str.transaction_payload:trades) t
WHERE transaction_id = '5a1b2dba6a198a23b47c1079a6e68852';
     
---Let's materialize this data in a table that lives in a downstream schema--
CREATE SCHEMA IF NOT EXISTS conformed_data;

CREATE OR REPLACE TABLE conformed_data.security_trades_flattened (
    transaction_date DATE,
    transaction_id VARCHAR(50),
    customer_id INT,
    account_type VARCHAR(25),
    trade_action VARCHAR(10),
    ticker VARCHAR(10),
    shares INT,
    trade_price DECIMAL(10,2),
    total_trade_value DECIMAL(10,2),
    share_change INT,
    cash_flow_impact DECIMAL(10,2),
    processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Normalized security trade transactions with flattened JSON structure. Contains individual trade details including calculated share changes (positive for buys, negative for sells) and cash flow impacts (negative for buys, positive for sells). Source: raw_data.security_trades';

---PART 2.5: CREATE DATA PIPELINES SCHEMA FOR TASK MANAGEMENT---
--Best practice: Create a dedicated schema for all ETL/pipeline tasks
--This provides better organization, security, and maintenance by separating
--orchestration logic from data storage layers
CREATE SCHEMA IF NOT EXISTS data_pipelines;

----We will need a task to orchestrate our insert---
CREATE OR REPLACE TASK data_pipelines.flatten_security_trades_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  TARGET_COMPLETION_INTERVAL = '1 MINUTE'
  COMMENT = 'Normalizes raw security trade JSON data by flattening nested structures and calculating share changes and cash flow impacts'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_data.security_trades_stream')
AS
INSERT INTO conformed_data.security_trades_flattened (
    transaction_date,
    transaction_id,
    customer_id,
    account_type,
    trade_action,
    ticker,
    shares,
    trade_price,
    total_trade_value,
    share_change,
    cash_flow_impact
)
SELECT 
    st.transaction_date,
    st.transaction_id,
    st.transaction_payload:customer_id::INT as customer_id,
    st.transaction_payload:account_type::STRING as account_type,
    t.value:action::STRING as trade_action,
    t.value:ticker::STRING as ticker,
    t.value:shares::INT as shares,
    t.value:trade_price::DECIMAL(10,2) as trade_price,
    t.value:total_trade_value::DECIMAL(10,2) as total_trade_value,
    CASE WHEN t.value:action::STRING = 'Buy' THEN t.value:shares::INT 
         ELSE -1 * t.value:shares::INT END as share_change,
    CASE WHEN t.value:action::STRING = 'Buy' THEN -1 * t.value:total_trade_value::DECIMAL(10,2)
         ELSE t.value:total_trade_value::DECIMAL(10,2) END as cash_flow_impact
FROM raw_data.security_trades_stream st,
     LATERAL FLATTEN(input => st.transaction_payload:trades) t;

-- Add business unit tag for task organization and management
ALTER TASK data_pipelines.flatten_security_trades_task SET TAG tags.public.business_unit = 'Central_IT';

--- This would start the task for good
--ALTER TASK data_pipelines.flatten_security_trades_task RESUME;

--For now we run it once to see it move data for us--
EXECUTE TASK data_pipelines.flatten_security_trades_task;

SELECT * FROM conformed_data.security_trades_flattened;

--Great, there is our data!! --

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    RESULT_LIMIT => 10,
    TASK_NAME=>'flatten_security_trades_task'));

--Create streams on the flattened security trades table for different consumers
--Each stream will be consumed by a different task to avoid stream consumption conflicts
CREATE OR REPLACE STREAM conformed_data.sec_flat_stream_cash_trade
ON TABLE conformed_data.security_trades_flattened
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'Stream for cash transaction processing from security trades';

CREATE OR REPLACE STREAM conformed_data.sec_flat_stream_security_positions
ON TABLE conformed_data.security_trades_flattened
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'Stream for updating security positions from trades';

CREATE OR REPLACE STREAM conformed_data.sec_flat_stream_portfolio_snapshot
ON TABLE conformed_data.security_trades_flattened
APPEND_ONLY = TRUE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'Stream for portfolio snapshot generation';

---PART 3: CASH TRANSACTION MANAGEMENT---
-- We still have cash deposits we want to incoporate into our holdings model
SELECT * FROM raw_data.cash_deposits_stream LIMIT 10;

--There is also cash flow data in normalized_trade that is a result of a client buying a security--
SELECT transaction_date, transaction_id,customer_id,  cash_flow_impact FROM conformed_data.security_trades_flattened LIMIT 10;

--Let's create a total cash activity table in conformed data to get all cash data in on place
CREATE OR REPLACE TABLE conformed_data.cash_transaction_detail (
    customer_id INT,
    transaction_date TIMESTAMP_NTZ,
    cash_deposit_id VARCHAR(50),
    security_purchase_transaction_id VARCHAR(50),
    ticker varchar(10),
    amount DECIMAL(10,2),
    account_type VARCHAR(25),
    transaction_category VARCHAR (100),
    processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Consolidated view of all cash-related transactions including direct cash deposits/withdrawals and cash impacts from security trades. Provides a complete audit trail of cash movements across all customer accounts. Sources: raw_data.cash_deposits and security trade cash flows';

--We can create a simple task to move data from the raw cash deposit table into this one--
CREATE OR REPLACE TASK data_pipelines.insert_cash_deposits_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  TARGET_COMPLETION_INTERVAL = '1 MINUTE'
  COMMENT = 'Loads cash deposit transactions from raw data stream into the conformed cash transaction detail table'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_data.cash_deposits_stream')
AS
INSERT INTO conformed_data.cash_transaction_detail (
    customer_id,
    transaction_date,
    cash_deposit_id,
    ticker,
    amount,
    account_type,
    transaction_category
    
)
SELECT 
    cdt.customer_id,
    cdt.transaction_date,
    cdt.deposit_id,
    cdt.ticker,
    cdt.deposit_amount,
    cdt.account_type_name,
    cdt.transaction_category
FROM raw_data.cash_deposits_stream cdt;

-- Add business unit tag for task organization and management
ALTER TASK data_pipelines.insert_cash_deposits_task SET TAG tags.public.business_unit = 'Central_IT';

EXECUTE TASK data_pipelines.insert_cash_deposits_task;

---Check out our new cash table--
SELECT *
FROM conformed_data.cash_transaction_detail;

--Awesome, lets create a task to insert data into cash_transaction_detail that is a result of a security trade--
CREATE OR REPLACE TASK data_pipelines.insert_trade_cash_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  COMMENT = 'Aggregates cash flow impacts from security trades and inserts them as cash transactions for complete cash activity tracking'
  AFTER data_pipelines.insert_cash_deposits_task
AS
INSERT INTO conformed_data.cash_transaction_detail (
    customer_id,
    transaction_date,
    security_purchase_transaction_id,
    ticker,
    amount,
    account_type,
    transaction_category
    )
SELECT nts.customer_id,
       nts.transaction_date,
       nts.transaction_id,
       'CASH' as ticker,
       SUM(cash_flow_impact) as amount,
       account_type,
       'Security Purchase' as transaction_category
FROM conformed_data.sec_flat_stream_cash_trade nts
GROUP BY ALL;

-- Add business unit tag for task organization and management
ALTER TASK data_pipelines.insert_trade_cash_task SET TAG tags.public.business_unit = 'Central_IT';

--We resume this task so it runs when the root task executes--
EXECUTE TASK data_pipelines.insert_trade_cash_task;

SELECT * FROM conformed_data.cash_transaction_detail
ORDER BY customer_id, transaction_date;

--Awesome-- Cash Management processing is almost done! 
--We can use a Dyanmic Table to create a net cash table for each customer each day
CREATE OR REPLACE DYNAMIC TABLE conformed_data.daily_cash_position_summary
    TARGET_LAG = '1 minute'
  WAREHOUSE = data_transformation_wh
    COMMENT = 'Daily aggregated cash positions by customer, date, ticker, and account type. Automatically refreshes every minute to provide near real-time net cash positions. Aggregates all cash movements from cash_transaction_detail table'
    AS 
    SELECT customer_id
          ,transaction_date
          ,ticker
          ,SUM(amount) as daily_net_cash
          ,account_type
    FROM cash_transaction_detail
    GROUP BY 1,2,3,5;

SELECT * FROM conformed_data.daily_cash_position_summary;


---PART 4: PORTFOLIO HOLDINGS MODEL---
--So far we have:
    ---Flattened some nested JSON containing trade data
    ---Processed all CASH transactions
--Now we need to create a Holdings data structure that allows for visibiltiy into what a customer owns
    ---First, lets create a table that will hold current cash/security positions and calculate relevant metrics--

--Lets add streams to our the two tables we created in the last steps:
--Note: We already created three separate streams on security_trades_flattened table above
--to avoid stream consumption conflicts between multiple tasks

--We can make streams on Dynamic Tables! 
--There is so much flexibility to create your data pipelines in Snowflake
CREATE OR REPLACE STREAM conformed_data.daily_cash_position_summary_stream
ON DYNAMIC TABLE conformed_data.daily_cash_position_summary
SHOW_INITIAL_ROWS = TRUE;

--Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('conformed_data.sec_flat_stream_security_positions') as sec_positions_stream_has_data;
SELECT SYSTEM$STREAM_HAS_DATA('conformed_data.sec_flat_stream_cash_trade') as sec_cash_stream_has_data;
SELECT SYSTEM$STREAM_HAS_DATA('conformed_data.sec_flat_stream_portfolio_snapshot') as sec_snapshot_stream_has_data;
SELECT SYSTEM$STREAM_HAS_DATA('conformed_data.daily_cash_position_summary_stream') as daily_cash_position_summary_stream_has_data;

--Create a table to track current security positions
--This maintains the latest position for each customer/security combination
CREATE OR REPLACE TABLE conformed_data.security_positions_current (
    customer_id INT,
    account_type VARCHAR(25),
    ticker VARCHAR(10),
    current_shares DECIMAL(18,4),
    total_cost_basis DECIMAL(18,2),
    average_cost_per_share DECIMAL(10,4),
    first_trade_date DATE,
    last_trade_date DATE,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_id, account_type, ticker)
)
CLUSTER BY (customer_id, account_type);

--Holdings Task 1: Update current security positions from trade stream
--This task maintains the current state of all security positions
CREATE OR REPLACE TASK data_pipelines.update_security_positions_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    TARGET_COMPLETION_INTERVAL = '1 MINUTE'
  COMMENT = 'Updates current security positions table based on new trades. Handles cost basis calculations for buys and proportional reductions for sells.'
  WHEN SYSTEM$STREAM_HAS_DATA('conformed_data.sec_flat_stream_security_positions')
AS
MERGE INTO conformed_data.security_positions_current target
USING (
  WITH trade_summary AS (
    -- Aggregate trades by customer/account/ticker
    SELECT 
        customer_id,
      account_type,
        ticker,
        SUM(share_change) as net_share_change,
      SUM(CASE WHEN trade_action = 'Buy' THEN total_trade_value ELSE 0 END) as buy_value,
      SUM(CASE WHEN trade_action = 'Sell' THEN ABS(share_change) ELSE 0 END) as shares_sold,
      MAX(transaction_date) as latest_trade_date,
      MIN(transaction_date) as earliest_trade_date
    FROM conformed_data.sec_flat_stream_security_positions
    GROUP BY customer_id, account_type, ticker
  )
  SELECT 
    ts.customer_id,
    ts.account_type,
    ts.ticker,
    -- Calculate new share count
    COALESCE(sp.current_shares, 0) + ts.net_share_change as new_shares,
    -- Calculate new cost basis
    CASE
      -- First time buying this security
      WHEN sp.total_cost_basis IS NULL THEN ts.buy_value
      -- Adding to position
      WHEN ts.net_share_change > 0 THEN sp.total_cost_basis + ts.buy_value
      -- Reducing position (proportional cost basis reduction)
      WHEN ts.net_share_change < 0 AND sp.current_shares > 0 THEN 
        sp.total_cost_basis * (1 - (ts.shares_sold / sp.current_shares))
      -- No change
      ELSE sp.total_cost_basis
    END as new_cost_basis,
    COALESCE(sp.first_trade_date, ts.earliest_trade_date) as first_trade_date,
    ts.latest_trade_date,
    CURRENT_TIMESTAMP() as last_updated
  FROM trade_summary ts
  LEFT JOIN conformed_data.security_positions_current sp
    ON ts.customer_id = sp.customer_id
    AND ts.account_type = sp.account_type
    AND ts.ticker = sp.ticker
) source
ON target.customer_id = source.customer_id
  AND target.account_type = source.account_type
  AND target.ticker = source.ticker
WHEN MATCHED AND source.new_shares > 0 THEN UPDATE SET
  current_shares = source.new_shares,
  total_cost_basis = source.new_cost_basis,
  average_cost_per_share = source.new_cost_basis / source.new_shares,
  last_trade_date = source.latest_trade_date,
  last_updated = source.last_updated
WHEN MATCHED AND source.new_shares <= 0 THEN DELETE
WHEN NOT MATCHED AND source.new_shares > 0 THEN INSERT (
  customer_id,
  account_type,
  ticker,
  current_shares,
  total_cost_basis,
  average_cost_per_share,
  first_trade_date,
  last_trade_date,
  last_updated
) VALUES (
  source.customer_id,
  source.account_type,
  source.ticker,
  source.new_shares,
  source.new_cost_basis,
  source.new_cost_basis / source.new_shares,
  source.first_trade_date,
  source.latest_trade_date,
  source.last_updated
);

-- Add business unit tag
ALTER TASK data_pipelines.update_security_positions_task SET TAG tags.public.business_unit = 'Central_IT';

--Historical Holdings Table (SCD Type 2) - Daily snapshots of all positions
CREATE OR REPLACE TABLE conformed_data.portfolio_holdings_history (
    snapshot_date DATE,
    customer_id INT,
    account_type VARCHAR(25),
    holding_type VARCHAR(10), -- 'SECURITY' or 'CASH'
    ticker VARCHAR(10),
    
    -- Position data
    quantity DECIMAL(18,4), -- shares for securities, amount for cash
    cost_basis DECIMAL(18,2),
    average_cost_per_share DECIMAL(10,4),
    
    -- Market data (from pricing table via ASOF JOIN)
    market_price DECIMAL(10,2),
    market_value DECIMAL(18,2),
    
    -- Performance metrics
    unrealized_gain_loss DECIMAL(18,2),
    unrealized_gain_loss_pct DECIMAL(10,4),
    
    -- Metadata
    last_transaction_date DATE,
    processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (snapshot_date, customer_id)
COMMENT = 'Daily snapshot of all customer holdings including securities and cash positions';


CREATE OR REPLACE TASK data_pipelines.generate_portfolio_snapshot_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'  
    TARGET_COMPLETION_INTERVAL = '1 MINUTE'
  COMMENT = 'Creates daily portfolio snapshots by combining current security positions with latest prices and cash balances'
  AFTER data_pipelines.update_security_positions_task
AS
INSERT INTO conformed_data.portfolio_holdings_history
WITH snapshot_dates AS (
  -- Get unique dates that need snapshots from both streams
  SELECT DISTINCT transaction_date as snapshot_date
  FROM conformed_data.sec_flat_stream_portfolio_snapshot
  UNION
  SELECT DISTINCT transaction_date
  FROM conformed_data.daily_cash_position_summary_stream
),
latest_prices AS (
  -- Get the most recent price for each security as of snapshot date
  SELECT DISTINCT
    sd.snapshot_date,
    sp.ticker,
    FIRST_VALUE(pm.close_price) OVER (
      PARTITION BY sd.snapshot_date, sp.ticker
      ORDER BY pm.price_date DESC
    ) as latest_price
  FROM snapshot_dates sd
  CROSS JOIN (SELECT DISTINCT ticker FROM conformed_data.security_positions_current) sp
  LEFT JOIN security_pricing_master_share.security_pricing_share.security_pricing_master pm
    ON sp.ticker = pm.ticker
    AND pm.price_date <= sd.snapshot_date
)
-- Generate snapshots for each date
SELECT 
  sd.snapshot_date,
  COALESCE(sp.customer_id, cp.customer_id) as customer_id,
  COALESCE(sp.account_type, cp.account_type) as account_type,
  CASE 
    WHEN sp.ticker IS NOT NULL THEN 'SECURITY'
    ELSE 'CASH'
  END as holding_type,
  COALESCE(sp.ticker, 'CASH') as ticker,
  
  -- Position data
  CASE 
    WHEN sp.ticker IS NOT NULL THEN CAST(sp.current_shares AS INT)
    ELSE ROUND(cp.daily_net_cash, 2)
  END as quantity,
  ROUND(COALESCE(sp.total_cost_basis, cp.daily_net_cash), 2) as cost_basis,
  ROUND(sp.average_cost_per_share, 2) as average_cost_per_share,
  
  -- Market data
  ROUND(COALESCE(lp.latest_price, 1.0), 2) as market_price,
  ROUND(CASE
    WHEN sp.ticker IS NOT NULL THEN sp.current_shares * COALESCE(lp.latest_price, 0)
    ELSE cp.daily_net_cash
  END, 2) as market_value,
  
  -- Performance metrics
  ROUND(CASE 
    WHEN sp.ticker IS NOT NULL AND sp.current_shares > 0 THEN 
      (sp.current_shares * COALESCE(lp.latest_price, 0)) - sp.total_cost_basis
    ELSE 0
  END, 2) as unrealized_gain_loss,
  ROUND(CASE 
    WHEN sp.ticker IS NOT NULL AND sp.total_cost_basis > 0 THEN 
      ((sp.current_shares * COALESCE(lp.latest_price, 0)) - sp.total_cost_basis) / sp.total_cost_basis * 100
    ELSE 0
  END, 2) as unrealized_gain_loss_pct,
  
  -- Metadata
  COALESCE(sp.last_trade_date, cp.transaction_date) as last_transaction_date,
  CURRENT_TIMESTAMP() as processed_timestamp
  
FROM snapshot_dates sd
-- Join with security positions
LEFT JOIN conformed_data.security_positions_current sp
  ON 1=1  -- Cartesian product to get all positions for all dates
-- Join with latest prices
LEFT JOIN latest_prices lp
  ON sd.snapshot_date = lp.snapshot_date
  AND sp.ticker = lp.ticker
-- Join with cash positions
FULL OUTER JOIN conformed_data.daily_cash_position_summary cp
  ON sd.snapshot_date = cp.transaction_date
  AND sp.customer_id = cp.customer_id
  AND sp.account_type = cp.account_type
WHERE COALESCE(sp.current_shares, cp.daily_net_cash, 0) > 0;

-- Add business unit tag for task organization
ALTER TASK data_pipelines.update_security_positions_task SET TAG tags.public.business_unit = 'Central_IT';


CREATE OR REPLACE DYNAMIC TABLE conformed_data.current_portfolio_holdings
  TARGET_LAG = '1 minute'
  WAREHOUSE = data_transformation_wh
  COMMENT = 'Dynamic table showing current portfolio holdings. Automatically refreshes from the latest snapshot in portfolio_holdings_history table.'
AS
WITH latest_snapshot AS (
  -- Get the most recent snapshot date
  SELECT MAX(snapshot_date) as max_snapshot_date
  FROM conformed_data.portfolio_holdings_history
),
latest_cash AS (
  -- Get the most recent cash position for each customer/account
  SELECT 
    customer_id,
    account_type,
    daily_net_cash,
    transaction_date
  FROM conformed_data.daily_cash_position_summary
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY transaction_date DESC) = 1
)
SELECT 
  -- Use the latest snapshot date
  ls.max_snapshot_date as snapshot_date,
  
  -- Position identifiers
  COALESCE(h.customer_id, c.customer_id) as customer_id,
  COALESCE(h.account_type, c.account_type) as account_type,
  COALESCE(h.holding_type, 'CASH') as holding_type,
  COALESCE(h.ticker, 'CASH') as ticker,
  
  -- Position data - use latest cash if more recent than holdings snapshot
  CASE 
    WHEN h.holding_type = 'CASH' AND c.transaction_date > ls.max_snapshot_date 
      THEN c.daily_net_cash
    ELSE h.quantity
  END as quantity,
  
  -- Cost basis - for cash, it equals the amount
  CASE 
    WHEN h.holding_type = 'CASH' AND c.transaction_date > ls.max_snapshot_date 
      THEN c.daily_net_cash
    ELSE h.cost_basis
  END as cost_basis,
  
  h.average_cost_per_share,
  h.market_price,
  
  -- Market value - recalculate for updated cash
  CASE 
    WHEN h.holding_type = 'CASH' AND c.transaction_date > ls.max_snapshot_date 
      THEN c.daily_net_cash
    ELSE h.market_value
  END as market_value,
  
  -- Performance metrics (0 for cash)
  CASE 
    WHEN h.holding_type = 'CASH' THEN 0
    ELSE h.unrealized_gain_loss
  END as unrealized_gain_loss,
  
  CASE 
    WHEN h.holding_type = 'CASH' THEN 0
    ELSE h.unrealized_gain_loss_pct
  END as unrealized_gain_loss_pct,
  
  -- Metadata
  COALESCE(h.last_transaction_date, c.transaction_date) as last_transaction_date,
  CURRENT_TIMESTAMP() as last_refreshed
  
FROM latest_snapshot ls
CROSS JOIN conformed_data.portfolio_holdings_history h
LEFT JOIN latest_cash c
  ON h.customer_id = c.customer_id
  AND h.account_type = c.account_type
  AND h.holding_type = 'CASH'
WHERE h.snapshot_date = ls.max_snapshot_date
  AND h.quantity > 0

UNION ALL

-- Add cash positions that might not be in the holdings history yet
SELECT 
  ls.max_snapshot_date as snapshot_date,
  c.customer_id,
  c.account_type,
  'CASH' as holding_type,
  'CASH' as ticker,
  c.daily_net_cash as quantity,
  c.daily_net_cash as cost_basis,
  NULL as average_cost_per_share,
  1.00 as market_price,
  c.daily_net_cash as market_value,
  0 as unrealized_gain_loss,
  0 as unrealized_gain_loss_pct,
  c.transaction_date as last_transaction_date,
  CURRENT_TIMESTAMP() as last_refreshed
FROM latest_snapshot ls
CROSS JOIN latest_cash c
WHERE NOT EXISTS (
  SELECT 1 
  FROM conformed_data.portfolio_holdings_history h
  WHERE h.customer_id = c.customer_id
    AND h.account_type = c.account_type
    AND h.holding_type = 'CASH'
    AND h.snapshot_date = ls.max_snapshot_date
)
AND c.daily_net_cash > 0
ORDER BY customer_id,ticker;

SELECT * 
FROM conformed_data.current_portfolio_holdings;


--Sample Customer Ids to 
--11963
--12005
--12236
--406
--722







