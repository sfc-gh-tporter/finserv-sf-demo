# Financial Services Demo - Snowflake Data Model & Analytics

A comprehensive financial services data model and toolkit for Snowflake, featuring customer management, portfolio analytics, and hands-on training materials.

## 📁 Repository Structure

### `/setup/` - Core Data Model
**Data Model**: 4-layer schema architecture optimized for financial analytics
- **`schema/create_schema.sql`** - Complete database schema definition
- **`data/`** - Sample datasets for all tables (CSV/Parquet format)

**Schema Organization:**
```
FINSERV_DEMO Database
├── RAW_DATA Schema - Core transactional data
│   ├── CUSTOMERS - Customer profiles with demographics & risk data  
│   ├── ADVISORS - Financial advisor information
│   ├── TRANSACTIONS - All financial transactions
│   └── EMPLOYERS - Company/employer master data
│
├── MAP Schema - Relationship mapping tables
│   ├── CUSTOMER_ADVISOR - Customer-advisor assignments
│   ├── CUSTOMER_PORTFOLIO - Customer-portfolio mappings
│   └── CUSTOMER_RETIREMENT_PLANS - Retirement plan associations
│
└── DIMS Schema - Dimension tables
    ├── DIM_SECURITIES - Security master data
    ├── DIM_ASSET_CLASS - Asset classification system
    ├── DIM_PORTFOLIO_COMPOSITION - Portfolio definitions
    ├── DIM_TRANSACTION_CATEGORY - Transaction classifications
    └── DIM_DATE - Date dimension with business calendar
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
```sql
-- Load sample data from setup/data/ directory
-- Use COPY INTO commands for each table
```

### 3. Run Hands-on Labs
Execute the training scripts in sequence:
- `hol_scripts/HoL 1 - Users_And_Roles.sql`
- `hol_scripts/HoL 2 - Compute_and_Batch_Data_Loading.sql`

## 📊 Sample Data Included

### Core Tables (~10K customers)
- **Customers**: Demographics, risk profiles, retirement status
- **Advisors**: Financial advisor information  
- **Transactions**: Historical and generated transaction data
- **Portfolios**: Asset allocation and composition data
- **Securities**: Stock/fund master data with classifications

### Analytics Features
- Customer risk profiling (1-10 scale)
- Portfolio composition tracking (JSON-based)
- Recurring cash flow modeling
- Date-based relationship management

---

*A complete financial services data model for Snowflake training and analytics.* 🏦📊