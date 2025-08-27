# Financial Services Demo - Snowflake Data Model & Analytics

A comprehensive financial services data model and toolkit for Snowflake, featuring customer management, portfolio analytics, and hands-on training materials.

## 📁 Repository Structure

### `/setup/` - Core Data Model
**Data Model**: 5-schema architecture optimized for financial analytics
- **`schema/create_schema.sql`** - Complete database schema definition
- **`data/`** - Sample datasets for all tables (CSV/Parquet format)

**Schema Organization:**
```
FINSERV_DEMO Database
├── RAW_DATA Schema - Core transactional data
│   ├── CUSTOMERS - Customer profiles with demographics & risk data  
│   ├── ADVISORS - Financial advisor information
│   ├── RETIREMENT_CONTRIBUTIONS - Retirement account transactions
│   ├── SECURITY_TRADES - Stock/fund trading transactions
│   └── EMPLOYERS - Company/employer master data
│
├── MAP Schema - Relationship mapping tables
│   ├── CUSTOMER_ADVISOR - Customer-advisor assignments
│   ├── CUSTOMER_PORTFOLIO - Customer-portfolio mappings
│   └── CUSTOMER_RETIREMENT_PLANS - Retirement plan associations
│
├── DIMS Schema - Dimension tables
│   ├── DIM_SECURITIES - Security master data
│   ├── DIM_ASSET_CLASS - Asset classification system
│   ├── DIM_PORTFOLIO_COMPOSITION - Portfolio definitions
│   ├── DIM_RETIREMENT_PLAN_FUNDS - Target-date fund allocations
│   └── DIM_DATE - Date dimension with business calendar
│
├── GH_UTIL Schema - GitHub utilities and helper functions
│
└── PUBLIC Schema - Default Snowflake public schema
```

### `/hol_scripts/` - Hands-on Lab Training
Interactive Snowflake training materials for financial services use cases:
- **HoL 1 - Users_And_Roles.sql** - User management, role creation, and permission grants
- **HoL 2 - Compute_and_Batch_Data_Loading.sql** - Warehouse management and data loading workflows

## 🔧 Key Features

### Data Model Highlights
- **Customer Risk Profiling** - Multi-level risk assessment (1-10 scale)
- **Portfolio Management** - JSON-based asset allocation tracking
- **Temporal Tracking** - Date-based relationship management
- **Security Classification** - Hash-based asset categorization
- **Retirement Fund Analytics** - Target-date fund composition and allocation tracking
- **Dual Transaction Systems** - Separate tracking for retirement contributions and security trades

### Schema Implementation Details

#### Core Transaction Tables
- **`RETIREMENT_CONTRIBUTIONS`** - Timestamp-based retirement account transactions with contribution schedules
- **`SECURITY_TRADES`** - Date-based stock/fund trading with transaction actions and categories

#### Enhanced Dimension Tables
- **`DIM_RETIREMENT_PLAN_FUNDS`** - Target-date funds with detailed asset allocation percentages:
  - Bond, US High Cap, International, US Mid Cap, Emerging Markets allocations
  - Years to retirement tracking and target year management
- **`DIM_DATE`** - Business calendar with holiday tracking and business day flags
- **`DIM_ASSET_CLASS`** - Hash-based asset classification with sector and geographic data

#### Advanced Features
- **Schema Tagging** - Business unit tags for data governance (`TAGS.PUBLIC.BUSINESS_UNIT='Central_IT'`)
- **File Format Support** - Built-in Parquet format configuration for efficient data loading
- **Primary Key Constraints** - Date dimension with proper primary key implementation

### Recurring Cash Flow Generation
Automated process for generating realistic recurring deposits and withdrawals:

**Business Logic:**
- **Active Customers**: 2% annual contributions (weekly deposits)
- **Retired Customers**: 4% annual withdrawals (weekly distributions)  
- **Timing**: Customer-specific schedules based on account opening dates
- **Output**: ~520,000 transactions (10k customers × 52 weeks)

## 🚀 Quick Start

### 1. Database Setup
```sql
-- Run the schema creation script
@setup/schema/create_schema.sql
```

### 2. Data Loading  
The schema includes optimized file format configurations for efficient data loading:

```sql
-- Parquet format is pre-configured for large datasets
-- Use the built-in PARQUET_FORMAT for customer data:
COPY INTO RAW_DATA.CUSTOMERS 
FROM @your_stage/customers/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT');

-- CSV loading for dimension tables from setup/data/ directory
COPY INTO DIMS.DIM_ASSET_CLASS 
FROM @your_stage/dim_asset_class/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

### 3. Run Hands-on Labs
Execute the training scripts in sequence:
- `hol_scripts/HoL 1 - Users_And_Roles.sql`
- `hol_scripts/HoL 2 - Compute_and_Batch_Data_Loading.sql`

## 📊 Sample Data Included

### Core Tables (~10K customers)
- **Customers**: Demographics, risk profiles, retirement status
- **Advisors**: Financial advisor information  
- **Retirement Contributions**: Automated contribution schedules and transactions
- **Security Trades**: Stock/fund trading history with buy/sell actions
- **Portfolios**: Asset allocation and composition data
- **Securities**: Stock/fund master data with classifications
- **Retirement Plan Funds**: Target-date fund allocations and compositions

### Analytics Features
- Customer risk profiling (1-10 scale)
- Portfolio composition tracking (JSON-based)
- Recurring cash flow modeling
- Date-based relationship management

---

*A complete financial services data model for Snowflake training and analytics.* 🏦📊