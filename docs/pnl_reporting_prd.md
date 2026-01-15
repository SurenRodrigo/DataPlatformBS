# P&L Reporting PRD (Wide World Importers)

## Document Info
| Field | Value |
|-------|-------|
| Version | 1.0 |
| Status | Draft |
| Last Updated | 2026-01-15 |
| Author | Data Platform Team |

---

## 1. Executive Summary

This PRD defines the requirements for a **Profit & Loss (P&L) reporting solution** using the Microsoft Wide World Importers (WWI) sample dataset. The solution will compute monthly **Sales Revenue**, **Cost of Goods Sold (COGS)**, and **Gross Profit** by **Stock Group**, storing results in the `dataplatform` database under a `reports` schema.

The implementation follows an **ELT (Extract-Load-Transform)** pattern:
1. **Extract & Load**: Python utility copies source tables from `source_data` database to `dataplatform.pyairbyte_cache` schema
2. **Transform**: DBT reads from `pyairbyte_cache` (Bronze) and applies **Medallion architecture** (Bronze → Silver → Gold) transformations

---

## 2. Goals

| Priority | Goal |
|----------|------|
| P0 | Produce monthly P&L metrics (Revenue, COGS, Gross Profit) per Stock Group |
| P0 | Use WWI source tables with clear, auditable cost derivation logic |
| P0 | Implement DBT medallion pipeline with tests and incremental refresh |
| P1 | Provide Python utility for MSSQL table copy operations |
| P2 | Support late-arriving data reprocessing |

---

## 3. Non-Goals

- Full finance/GL reconciliation or multi-currency support
- Allocation of overhead, logistics, or warehousing costs beyond purchase price
- Real-time updates (batch refresh is sufficient)
- Complex cost accounting methods (FIFO, LIFO, weighted average)

---

## 4. Stakeholders

| Role | Responsibility |
|------|----------------|
| Data Engineering | DBT models, orchestration, Python utility |
| Analytics/BI | Report consumption, validation |
| Platform Ops | Database access, pipeline monitoring |

---

## 5. Source Systems & Data

### 5.1 Source Database
**Database**: `source_data` (Wide World Importers OLTP)

### 5.2 Source Tables by Domain

#### Sales Schema
| Table | Purpose | Key Fields |
|-------|---------|------------|
| `Sales.Invoices` | Invoice headers | InvoiceID, CustomerID, InvoiceDate, BillToCustomerID |
| `Sales.InvoiceLines` | Invoice line items | InvoiceLineID, InvoiceID, StockItemID, Quantity, UnitPrice, ExtendedPrice |

#### Purchasing Schema
| Table | Purpose | Key Fields |
|-------|---------|------------|
| `Purchasing.PurchaseOrders` | PO headers | PurchaseOrderID, SupplierID, OrderDate, ExpectedDeliveryDate |
| `Purchasing.PurchaseOrderLines` | PO line items | PurchaseOrderLineID, PurchaseOrderID, StockItemID, OrderedOuters, ExpectedUnitPricePerOuter, LastReceiptDate |

#### Warehouse Schema
| Table | Purpose | Key Fields |
|-------|---------|------------|
| `Warehouse.StockItems` | Product master | StockItemID, StockItemName, SupplierID, UnitPrice, RecommendedRetailPrice |
| `Warehouse.StockGroups` | Product categories | StockGroupID, StockGroupName |
| `Warehouse.StockItemStockGroups` | Item-to-group mapping | StockItemStockGroupID, StockItemID, StockGroupID |
| `Warehouse.StockItemTransactions` | Inventory movements | StockItemTransactionID, StockItemID, TransactionTypeID, CustomerID, InvoiceID, SupplierID, PurchaseOrderID, TransactionOccurredWhen, Quantity |

### 5.3 Intermediate Cache (Bronze Layer)
**Database**: `dataplatform`
**Schema**: `pyairbyte_cache`
**Purpose**: Raw data landing zone for extracted WWI tables

The Python utility copies source tables from `source_data` to `dataplatform.pyairbyte_cache`:

| Source Table | Cache Table |
|--------------|-------------|
| `source_data.Sales.Invoices` | `dataplatform.pyairbyte_cache.sales_invoices` |
| `source_data.Sales.InvoiceLines` | `dataplatform.pyairbyte_cache.sales_invoice_lines` |
| `source_data.Purchasing.PurchaseOrders` | `dataplatform.pyairbyte_cache.purchasing_purchase_orders` |
| `source_data.Purchasing.PurchaseOrderLines` | `dataplatform.pyairbyte_cache.purchasing_purchase_order_lines` |
| `source_data.Warehouse.StockItems` | `dataplatform.pyairbyte_cache.warehouse_stock_items` |
| `source_data.Warehouse.StockGroups` | `dataplatform.pyairbyte_cache.warehouse_stock_groups` |
| `source_data.Warehouse.StockItemStockGroups` | `dataplatform.pyairbyte_cache.warehouse_stock_item_stock_groups` |
| `source_data.Warehouse.StockItemTransactions` | `dataplatform.pyairbyte_cache.warehouse_stock_item_transactions` |

### 5.4 Target Database (Gold Layer)
**Database**: `dataplatform`
**Schema**: `reports`
**Table**: `profit_and_loss_data`

---

## 6. Business Logic Definitions

### 6.1 Revenue Calculation

**Definition**: Sales Revenue (excluding tax) per invoice line

**Source Field**: `Sales.InvoiceLines.ExtendedPrice`

**Time Grain**: `Sales.Invoices.InvoiceDate` grouped to Month/Year

**Formula**:
```
Revenue = SUM(InvoiceLines.ExtendedPrice) 
          WHERE Invoices.InvoiceDate IN target month/year
          GROUP BY StockGroup, Year, Month
```

### 6.2 Cost of Goods Sold (COGS) Calculation

**Definition**: Purchase cost of items sold, derived from Purchase Order history

**Approach**: Link sales to purchasing via `StockItemTransactions` and `PurchaseOrderLines`

**Logic Flow**:
```
1. For each InvoiceLine (sale):
   a. Get StockItemID and InvoiceDate
   b. Find StockItemTransactions with PurchaseOrderID for that StockItemID
   c. Join to PurchaseOrderLines to get UnitPrice (ExpectedUnitPricePerOuter)
   d. Select LATEST purchase cost ON OR BEFORE InvoiceDate
   e. COGS = UnitCost × Quantity sold
```

**Source Fields**:
- `Purchasing.PurchaseOrderLines.ExpectedUnitPricePerOuter` (unit cost)
- `Purchasing.PurchaseOrderLines.LastReceiptDate` (cost effective date)
- `Warehouse.StockItemTransactions.PurchaseOrderID` (links stock to PO)

**Fallback Rules**:
| Scenario | Action |
|----------|--------|
| No purchase history before InvoiceDate | Use latest known cost after InvoiceDate |
| No purchase history at all for item | Leave COGS as NULL, flag in audit |
| Multiple PO lines for same date | Use most recent PurchaseOrderLineID |

**Formula**:
```
COGS = SUM(LatestUnitCost × InvoiceLines.Quantity)
       WHERE Invoices.InvoiceDate IN target month/year
       GROUP BY StockGroup, Year, Month
```

### 6.3 Gross Profit Calculation

**Formula**:
```
Gross Profit = Revenue - COGS
```

### 6.4 Returns and Credits Handling

**Initial Implementation**: Exclude credit notes/returns

**Rationale**: WWI standard schema does not have explicit credit note tables in OLTP; credit handling deferred to future enhancement.

**Future Enhancement**: If credits identified (e.g., negative quantities, IsCreditNote flag), include as negative revenue/COGS.

---

## 7. Target Data Model

### 7.1 Table Definition

**Table**: `reports.profit_and_loss_data`

**Grain**: One row per `StockGroupID` × `Year` × `Month`

### 7.2 Column Specification

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `pnl_id` | INT IDENTITY | NOT NULL | Surrogate key |
| `stock_group_id` | INT | NOT NULL | FK to StockGroups |
| `stock_group_name` | NVARCHAR(100) | NOT NULL | Denormalized group name |
| `year` | INT | NOT NULL | Calendar year |
| `month` | INT | NOT NULL | Calendar month (1-12) |
| `month_start_date` | DATE | NOT NULL | First day of month |
| `sales_revenue_ex_tax` | DECIMAL(18,2) | NOT NULL | Total revenue excluding tax |
| `cogs` | DECIMAL(18,2) | NULL | Total cost of goods sold |
| `gross_profit` | DECIMAL(18,2) | NULL | Revenue minus COGS |
| `gross_margin_pct` | DECIMAL(5,2) | NULL | Gross profit / Revenue × 100 |
| `units_sold` | DECIMAL(18,2) | NOT NULL | Sum of quantities sold |
| `invoice_line_count` | INT | NOT NULL | Count of invoice lines |
| `invoice_count` | INT | NOT NULL | Count of distinct invoices |
| `items_without_cost` | INT | NULL | Count of lines with missing COGS |
| `source_min_invoice_date` | DATE | NULL | Earliest invoice in period |
| `source_max_invoice_date` | DATE | NULL | Latest invoice in period |
| `cost_method` | NVARCHAR(50) | NOT NULL | 'LATEST_PO_BEFORE_DATE' |
| `created_at` | DATETIME2 | NOT NULL | Row creation timestamp |
| `updated_at` | DATETIME2 | NOT NULL | Last update timestamp |

### 7.3 Indexes

| Index | Columns | Type |
|-------|---------|------|
| PK_profit_and_loss_data | pnl_id | Clustered |
| IX_pnl_stock_group_period | stock_group_id, year, month | Unique, Nonclustered |
| IX_pnl_period | year, month | Nonclustered |

### 7.4 Sample DDL

```sql
CREATE SCHEMA reports;
GO

CREATE TABLE reports.profit_and_loss_data (
    pnl_id INT IDENTITY(1,1) NOT NULL,
    stock_group_id INT NOT NULL,
    stock_group_name NVARCHAR(100) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_start_date DATE NOT NULL,
    sales_revenue_ex_tax DECIMAL(18,2) NOT NULL,
    cogs DECIMAL(18,2) NULL,
    gross_profit DECIMAL(18,2) NULL,
    gross_margin_pct DECIMAL(5,2) NULL,
    units_sold DECIMAL(18,2) NOT NULL,
    invoice_line_count INT NOT NULL,
    invoice_count INT NOT NULL,
    items_without_cost INT NULL,
    source_min_invoice_date DATE NULL,
    source_max_invoice_date DATE NULL,
    cost_method NVARCHAR(50) NOT NULL DEFAULT 'LATEST_PO_BEFORE_DATE',
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_profit_and_loss_data PRIMARY KEY CLUSTERED (pnl_id),
    CONSTRAINT UQ_pnl_stock_group_period UNIQUE (stock_group_id, year, month)
);
GO

CREATE NONCLUSTERED INDEX IX_pnl_period 
ON reports.profit_and_loss_data (year, month);
GO
```

---

## 8. DBT Model Architecture

### 8.1 Layer Overview (ELT + Medallion)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ELT + MEDALLION ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐  │
│  │   EXTRACT    │    │   BRONZE     │    │    SILVER    │    │   GOLD    │  │
│  │   (Python)   │───▶│   (Sources)  │───▶│   (Staging)  │───▶│ (Marts)   │  │
│  │              │    │              │    │              │    │           │  │
│  └──────────────┘    └──────────────┘    └──────────────┘    └───────────┘  │
│                                                                              │
│  - copy_table.py     - pyairbyte_cache  - Standardized     - Cost resolution│
│  - source_data →       schema             naming           - Aggregations   │
│    pyairbyte_cache   - Source freshness - Type casting     - P&L calcs      │
│                        tests            - Deduplication    - Final table    │
│                                         - Basic tests                       │
│                                                                              │
│  Database: source_data  Database: dataplatform                              │
│                         Schema: pyairbyte_cache → staging → reports         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
└─────────────────────────────────────────────────────────────────────────────┘

                          EXTRACT (Python copy_table.py)
                          ════════════════════════════════
source_data.Sales.Invoices ──────────────┐
source_data.Sales.InvoiceLines ──────────┤
source_data.Purchasing.PurchaseOrders ───┼──▶ dataplatform.pyairbyte_cache.*
source_data.Purchasing.PurchaseOrderLines┤    (8 tables)
source_data.Warehouse.StockItems ────────┤
source_data.Warehouse.StockGroups ───────┤
source_data.Warehouse.StockItemStockGroups
source_data.Warehouse.StockItemTransactions
                                              │
                                              ▼
                          TRANSFORM (DBT)
                          ═══════════════
        ┌─────────────────────────────────────────────────────────────┐
        │  BRONZE (Sources)                                           │
        │  dataplatform.pyairbyte_cache.*                             │
        └─────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  SILVER (Staging)                                           │
        │  stg_sales_invoices, stg_sales_invoice_lines,               │
        │  stg_purchase_orders, stg_purchase_order_lines,             │
        │  stg_stock_items, stg_stock_groups,                         │
        │  stg_stock_item_stock_groups, stg_stock_item_transactions   │
        └─────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  INTERMEDIATE                                               │
        │  int_item_stock_groups    ◀── Item to StockGroup mapping    │
        │  int_item_costs           ◀── Purchase cost time-series     │
        │  int_invoice_lines_with_cost ◀── Sales lines with cost      │
        └─────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  GOLD (Reporting)                                           │
        │  dataplatform.reports.profit_and_loss_data                  │
        └─────────────────────────────────────────────────────────────┘
```

### 8.3 Model Specifications

#### 8.3.1 Sources (Bronze)

**File**: `models/sources/source_wwi.yml`

DBT sources point to `dataplatform.pyairbyte_cache` where extracted data lands:

```yaml
version: 2

sources:
  - name: wwi_cache
    description: "WWI data extracted to pyairbyte_cache schema"
    database: dataplatform
    schema: pyairbyte_cache
    tables:
      - name: sales_invoices
        description: "Extracted from source_data.Sales.Invoices"
        columns:
          - name: InvoiceID
            tests: [unique, not_null]
      - name: sales_invoice_lines
        description: "Extracted from source_data.Sales.InvoiceLines"
        columns:
          - name: InvoiceLineID
            tests: [unique, not_null]
      - name: purchasing_purchase_orders
        description: "Extracted from source_data.Purchasing.PurchaseOrders"
      - name: purchasing_purchase_order_lines
        description: "Extracted from source_data.Purchasing.PurchaseOrderLines"
      - name: warehouse_stock_items
        description: "Extracted from source_data.Warehouse.StockItems"
      - name: warehouse_stock_groups
        description: "Extracted from source_data.Warehouse.StockGroups"
      - name: warehouse_stock_item_stock_groups
        description: "Extracted from source_data.Warehouse.StockItemStockGroups"
      - name: warehouse_stock_item_transactions
        description: "Extracted from source_data.Warehouse.StockItemTransactions"
```

#### 8.3.2 Staging Models (Silver)

| Model | Source | Key Transformations |
|-------|--------|---------------------|
| `stg_sales_invoices` | Sales.Invoices | Rename to snake_case, cast InvoiceDate |
| `stg_sales_invoice_lines` | Sales.InvoiceLines | Rename, cast ExtendedPrice to decimal |
| `stg_purchase_orders` | Purchasing.PurchaseOrders | Rename, cast dates |
| `stg_purchase_order_lines` | Purchasing.PurchaseOrderLines | Rename, calculate UnitCost from ExpectedUnitPricePerOuter |
| `stg_stock_items` | Warehouse.StockItems | Rename, cast prices |
| `stg_stock_groups` | Warehouse.StockGroups | Rename |
| `stg_stock_item_stock_groups` | Warehouse.StockItemStockGroups | Rename |
| `stg_stock_item_transactions` | Warehouse.StockItemTransactions | Rename, cast dates |

**Example Staging Model**: `stg_sales_invoice_lines.sql`

```sql
with source as (
    select * from {{ source('wwi_cache', 'sales_invoice_lines') }}
),

renamed as (
    select
        InvoiceLineID as invoice_line_id,
        InvoiceID as invoice_id,
        StockItemID as stock_item_id,
        Description as description,
        PackageTypeID as package_type_id,
        Quantity as quantity,
        UnitPrice as unit_price,
        TaxRate as tax_rate,
        TaxAmount as tax_amount,
        LineProfit as line_profit,
        ExtendedPrice as extended_price,
        LastEditedBy as last_edited_by,
        LastEditedWhen as last_edited_when
    from source
)

select * from renamed
```

#### 8.3.3 Intermediate Models

**Model 1**: `int_item_stock_groups.sql`
- Joins StockItemStockGroups with StockGroups
- Output: stock_item_id, stock_group_id, stock_group_name

**Model 2**: `int_item_costs.sql`
- Joins StockItemTransactions (with PurchaseOrderID) to PurchaseOrderLines
- Creates time-series: stock_item_id, cost_effective_date, unit_cost
- Handles multiple POs per item

**Model 3**: `int_invoice_lines_with_cost.sql`
- Joins invoice_lines + invoices
- Joins to int_item_costs with latest-cost-before-date logic
- Calculates: revenue, cogs, gross_profit per line

#### 8.3.4 Gold Model

**Model**: `profit_and_loss_data.sql`

```sql
{{
    config(
        materialized='table',
        schema='reports'
    )
}}

with invoice_lines_with_cost as (
    select * from {{ ref('int_invoice_lines_with_cost') }}
),

item_groups as (
    select * from {{ ref('int_item_stock_groups') }}
),

lines_with_groups as (
    select
        il.*,
        ig.stock_group_id,
        ig.stock_group_name
    from invoice_lines_with_cost il
    left join item_groups ig on il.stock_item_id = ig.stock_item_id
),

aggregated as (
    select
        stock_group_id,
        stock_group_name,
        year(invoice_date) as year,
        month(invoice_date) as month,
        datefromparts(year(invoice_date), month(invoice_date), 1) as month_start_date,
        sum(extended_price) as sales_revenue_ex_tax,
        sum(cogs) as cogs,
        sum(extended_price) - sum(cogs) as gross_profit,
        case 
            when sum(extended_price) > 0 
            then (sum(extended_price) - sum(cogs)) / sum(extended_price) * 100
            else null 
        end as gross_margin_pct,
        sum(quantity) as units_sold,
        count(*) as invoice_line_count,
        count(distinct invoice_id) as invoice_count,
        sum(case when cogs is null then 1 else 0 end) as items_without_cost,
        min(invoice_date) as source_min_invoice_date,
        max(invoice_date) as source_max_invoice_date,
        'LATEST_PO_BEFORE_DATE' as cost_method,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from lines_with_groups
    where stock_group_id is not null
    group by 
        stock_group_id,
        stock_group_name,
        year(invoice_date),
        month(invoice_date)
)

select * from aggregated
```

### 8.4 File Structure

```
app/data-platform-service/dbt_models/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── sources/
    │   └── source_wwi.yml
    ├── staging/
    │   ├── schema.yml
    │   ├── stg_sales_invoices.sql
    │   ├── stg_sales_invoice_lines.sql
    │   ├── stg_purchase_orders.sql
    │   ├── stg_purchase_order_lines.sql
    │   ├── stg_stock_items.sql
    │   ├── stg_stock_groups.sql
    │   ├── stg_stock_item_stock_groups.sql
    │   └── stg_stock_item_transactions.sql
    ├── intermediate/
    │   ├── schema.yml
    │   ├── int_item_stock_groups.sql
    │   ├── int_item_costs.sql
    │   └── int_invoice_lines_with_cost.sql
    └── marts/
        └── reporting/
            ├── schema.yml
            └── profit_and_loss_data.sql
```

### 8.5 Testing Strategy

| Test Type | Location | Examples |
|-----------|----------|----------|
| Uniqueness | All staging schema.yml | unique on primary keys |
| Not Null | All staging schema.yml | not_null on PKs, required fields |
| Relationships | Intermediate schema.yml | invoice_lines → invoices FK |
| Accepted Values | Intermediate | cost_method in allowed list |
| Custom | Gold model | gross_profit = revenue - cogs |

---

## 9. Python Utility: MSSQL Table Copy (Extract Step)

### 9.1 Purpose

Reusable utility to **Extract** data from `source_data` database and **Load** into `dataplatform.pyairbyte_cache` schema. This is the **E** and **L** in the ELT pipeline, executed before DBT transformations.

### 9.2 Role in Pipeline

```
┌─────────────────┐      copy_table.py       ┌──────────────────────────────┐
│   source_data   │  ─────────────────────▶  │ dataplatform.pyairbyte_cache │
│   (WWI OLTP)    │      (Extract & Load)    │      (Bronze Layer)          │
└─────────────────┘                          └──────────────────────────────┘
                                                          │
                                                          ▼
                                                    DBT Transform
                                                          │
                                                          ▼
                                             ┌──────────────────────────────┐
                                             │   dataplatform.reports       │
                                             │      (Gold Layer)            │
                                             └──────────────────────────────┘
```

### 9.3 Interface

**Command Line**:
```bash
python copy_table.py \
    --src-conn "Driver={ODBC Driver 18 for SQL Server};Server=source-server;Database=source_data;..." \
    --src-schema "Sales" \
    --src-table "Invoices" \
    --dst-conn "Driver={ODBC Driver 18 for SQL Server};Server=platform-server;Database=dataplatform;..." \
    --dst-schema "pyairbyte_cache" \
    --dst-table "sales_invoices" \
    [--truncate] \
    [--batch-size 10000]
```

### 9.4 Extract Jobs for P&L

| Source | Destination |
|--------|-------------|
| `source_data.Sales.Invoices` | `dataplatform.pyairbyte_cache.sales_invoices` |
| `source_data.Sales.InvoiceLines` | `dataplatform.pyairbyte_cache.sales_invoice_lines` |
| `source_data.Purchasing.PurchaseOrders` | `dataplatform.pyairbyte_cache.purchasing_purchase_orders` |
| `source_data.Purchasing.PurchaseOrderLines` | `dataplatform.pyairbyte_cache.purchasing_purchase_order_lines` |
| `source_data.Warehouse.StockItems` | `dataplatform.pyairbyte_cache.warehouse_stock_items` |
| `source_data.Warehouse.StockGroups` | `dataplatform.pyairbyte_cache.warehouse_stock_groups` |
| `source_data.Warehouse.StockItemStockGroups` | `dataplatform.pyairbyte_cache.warehouse_stock_item_stock_groups` |
| `source_data.Warehouse.StockItemTransactions` | `dataplatform.pyairbyte_cache.warehouse_stock_item_transactions` |

### 9.5 Features

| Feature | Description |
|---------|-------------|
| Schema Discovery | Reads source via `INFORMATION_SCHEMA.COLUMNS` |
| Auto Schema Creation | Creates destination schema if missing |
| Auto Table Creation | Creates destination table with mapped types |
| Type Mapping | Maps MSSQL types (varchar, nvarchar, int, bigint, decimal, datetime2, bit) |
| Batch Insert | Uses `fast_executemany=True` for performance |
| Truncate Option | Optional truncate before load |
| Progress Logging | Reports rows copied, time elapsed |

### 9.6 Type Mapping Table

| Source Type | Destination Type |
|-------------|------------------|
| varchar(n) | varchar(n) |
| nvarchar(n) | nvarchar(n) |
| int | int |
| bigint | bigint |
| decimal(p,s) | decimal(p,s) |
| datetime | datetime2 |
| datetime2 | datetime2 |
| date | date |
| bit | bit |
| uniqueidentifier | uniqueidentifier |

### 9.7 Location

**File**: `app/data-platform-service/data-manager/scripts/copy_table.py`

---

## 10. Incremental Load Strategy

### 10.1 Approach

- **Incremental by Month**: Reprocess current month + last N months
- **Late-Arriving Data**: Recompute last 3 months on each run
- **Full Refresh**: Support manual full refresh via dbt flag

### 10.2 Implementation

```sql
{{
    config(
        materialized='incremental',
        unique_key=['stock_group_id', 'year', 'month'],
        incremental_strategy='merge'
    )
}}

...

{% if is_incremental() %}
where invoice_date >= dateadd(month, -3, datefromparts(year(getdate()), month(getdate()), 1))
{% endif %}
```

---

## 11. Validation & Acceptance Criteria

### 11.1 Data Quality Checks

| Check | Query | Expected |
|-------|-------|----------|
| Revenue Total | SUM(sales_revenue_ex_tax) | Matches SUM(InvoiceLines.ExtendedPrice) |
| Row Count | COUNT(*) per month | > 0 for months with invoices |
| COGS Coverage | AVG(items_without_cost) | < 5% of lines |
| Gross Profit | gross_profit | = revenue - cogs |
| Stock Group Coverage | COUNT(DISTINCT stock_group_id) | Matches WWI stock groups |

### 11.2 Validation Queries

```sql
-- Check total revenue matches source (via pyairbyte_cache)
SELECT 
    SUM(sales_revenue_ex_tax) as pnl_revenue,
    (SELECT SUM(ExtendedPrice) FROM dataplatform.pyairbyte_cache.sales_invoice_lines) as cache_revenue
FROM dataplatform.reports.profit_and_loss_data;

-- Check COGS coverage
SELECT 
    year, month,
    SUM(invoice_line_count) as total_lines,
    SUM(items_without_cost) as missing_cost,
    100.0 * SUM(items_without_cost) / SUM(invoice_line_count) as pct_missing
FROM dataplatform.reports.profit_and_loss_data
GROUP BY year, month
ORDER BY year, month;

-- Verify cache data matches source (run against source_data)
SELECT 
    (SELECT COUNT(*) FROM source_data.Sales.InvoiceLines) as source_count,
    (SELECT COUNT(*) FROM dataplatform.pyairbyte_cache.sales_invoice_lines) as cache_count;
```

---

## 12. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Missing purchase history | COGS incomplete | Fallback logic + audit column |
| Cost timing ambiguity | Incorrect COGS | Document method in cost_method |
| Schema changes in WWI | Pipeline failure | dbt source tests |
| Large data volume | Slow refresh | Incremental loads |
| Cache staleness | Stale data in reports | Run Extract before Transform; add freshness tests |
| Extract job failure | DBT sees old data | Monitor Extract job; add row count validation |

---

## 13. Implementation Milestones

| Milestone | Deliverables | Dependencies |
|-----------|--------------|--------------|
| M1: Schema Setup | pyairbyte_cache schema, reports schema, DDL scripts | DB access |
| M2: Python Utility | copy_table.py for Extract | M1 |
| M3: Extract Jobs | Run copy_table.py for 8 WWI tables to pyairbyte_cache | M2 |
| M4: DBT Sources & Staging | source_wwi.yml, 8 staging models | M3 |
| M5: Intermediate Models | 3 intermediate models, cost logic | M4 |
| M6: Gold Model | profit_and_loss_data, tests | M5 |
| M7: Validation | Validation queries, documentation | M6 |

---

## 14. Open Questions

1. **Stock Group hierarchy**: Should we support parent/child group rollups?
2. **Historical restatement**: If costs change retroactively, should we recompute all history?
3. **Multi-year comparison**: Should the table support YoY comparison columns?

---

## 15. Appendix

### A. WWI Schema Reference

- Sales ER Diagram: `docs/ExampleDocs/SalesER.png`
- Purchasing ER Diagram: `docs/ExampleDocs/PurchasingER.png`
- Warehouse ER Diagram: `docs/ExampleDocs/WarehouseER.png`

### B. Related Documents

- Plan File: `.cursor/plans/pnl_reporting_plan_19fec9c6.plan.md`
- DBT Project: `app/data-platform-service/dbt_models/`

### C. Glossary

| Term | Definition |
|------|------------|
| COGS | Cost of Goods Sold - direct cost of inventory sold |
| Gross Profit | Revenue minus COGS |
| Gross Margin | Gross Profit as percentage of Revenue |
| Stock Group | Product category in WWI |
| Medallion Architecture | Bronze (raw) → Silver (cleansed) → Gold (aggregated) |
| ELT | Extract-Load-Transform - data pipeline pattern where raw data is loaded before transformation |
| pyairbyte_cache | Schema in dataplatform DB where extracted source data lands (Bronze layer) |
| Extract | Process of copying data from source_data to pyairbyte_cache using Python utility |