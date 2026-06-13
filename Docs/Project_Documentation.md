# Orion Technical Assessment — Data Analytics Engineer
## Complete Solution Package: ETL, Data Model, DAX, Dashboard & Documentation

---

# PHASE 1 — DATA DISCOVERY

## 1.1 File Inventory

| File | Type | Records | Structure |
|---|---|---|---|
| `Sales.json` | Flat array of objects | 298,246 | Single denormalized table — every transaction row repeats product, customer, and geography attributes |
| `forecast.json` | Flat array of objects | 33 | Aggregated forecast: one row per `CountryRegion` × `Brand` × `Year` (2009 only) |

## 1.2 Entity & Relationship Analysis

`Sales.json` is a **fully denormalized "wide" table** containing three blended entities:

| Entity | Fields | Notes |
|---|---|---|
| **Product** | `ProductKey`, `Product Name`, `Brand`, `Color`, `Subcategory`, `Category` | Repeats for every transaction of the same product (2,495 distinct products) |
| **Customer** | `CustomerKey`, `Customer Code`, `Name`, `Education`, `Occupation`, `Continent`, `City`, `State`, `CountryRegion` | Repeats for every transaction by the same customer (8,868 distinct customers) |
| **Transaction (Sales)** | `OrderDate`, `Quantity`, `Net Price` | The only truly transactional fields — no unique line-item key exists |

`forecast.json` represents a **pre-aggregated planning entity** at `CountryRegion` × `Brand` × `Year` granularity — this is a fundamentally different grain than Sales (transaction-level), which is the central modeling challenge addressed in Phase 3.

**Relationship hierarchy implied:**
```
Product (Category → Subcategory → Brand → ProductKey)
Customer (Continent → CountryRegion → State → City → CustomerKey)
Sales = Product × Customer × Date (transaction grain)
Forecast = Brand × CountryRegion × Year (planning grain — no transaction-level link)
```

## 1.3 Data Quality Findings

| # | Issue | Detail | Impact |
|---|---|---|---|
| 1 | **Massive duplication** | 218,008 of 298,246 rows (73.1%) are **exact duplicates** | Without de-duplication, Total Sales is overstated by ~3.7x ($42.6M true vs ~$155M raw) — the single most critical finding |
| 2 | **Mis-mapped column** | `Color` field contains **Subcategory** values for 100% of rows (e.g., Color = "Cell phones Accessories"), not actual product colors | Misleading field name; column is redundant and must be dropped or renamed |
| 3 | **Missing demographic data** | `Name`, `Education`, `Occupation` are `null` for 50,441 of 80,238 de-duplicated rows (62.9%) | Indicates these fields apply only to a subset of customers (likely a "loyalty/profile" segment); must be handled as "Unknown" rather than dropped |
| 4 | **Inconsistent data type — Date** | `OrderDate` is stored as text string (`"1/1/2008"`), not ISO date | Must be parsed to proper `datetime` for the Date dimension and time intelligence |
| 5 | **No surrogate transaction key** | No `SalesKey`/`OrderID`/`LineItemID` exists | A synthetic surrogate key must be generated for `FactSales` |
| 6 | **Different grain: Sales vs Forecast** | Sales = per-transaction (date, product, customer); Forecast = per Country/Brand/Year (2009 only) | Direct one-to-many relationship is impossible; requires conformed `DimBrand`/`DimGeography`/`DimDate(Year)` dimensions (Phase 3) |
| 7 | **Forecast covers only 2009** | No 2008 forecast values exist | "Forecast vs Actual" comparisons are valid only for FY2009 |
| 8 | **Whitespace / formatting** | Minor leading/trailing whitespace risk in text fields (Product Name, City, etc.) | Standardized via `.strip()` during cleaning |
| 9 | **Orphan record check** | No orphan ProductKey/CustomerKey found after de-duplication — referential integrity holds | Confirmed during validation stage |
| 10 | **Range/validity checks** | No negative or zero values found in `Quantity` or `Net Price` | Clean numeric ranges — no outlier corrections required |

## 1.4 Key Profiling Statistics (post-discovery)

- Distinct Products: **2,495** | Distinct Customers: **8,868**
- Countries: United States, Germany, China | Continents: North America, Europe, Asia
- Brands (11): A. Datum, Adventure Works, Contoso, Fabrikam, Litware, Northwind Traders, Proseware, Southridge Video, Tailspin Toys, The Phone Company, Wide World Importers
- Categories (8): Cell phones, Computers, TV and Video, Home Appliances, Cameras and camcorders, Audio, Games and Toys, Music/Movies/Audio Books
- Date range: **2008-01-01 to 2009-12-31** (603 distinct transaction dates)

---

# PHASE 2 — ETL DESIGN

## 2.1 ETL Architecture

```
                ┌──────────────────┐        ┌──────────────────┐
 RAW JSON  ───▶ │  1. EXTRACT       │ ─────▶ │  2. CLEAN          │
 (Sales,        │  - load JSON      │        │  - de-duplicate    │
 Forecast)      │  - parse to df    │        │  - trim strings    │
                │  - error handling │        │  - fix dtypes      │
                └──────────────────┘        │  - handle nulls    │
                                              │  - drop bad column │
                                              └─────────┬──────────┘
                                                         ▼
┌────────────────────────────────────────────────────────────────────┐
│  3. TRANSFORM — Normalize into Star Schema                            │
│   DimDate · DimProduct · DimCustomer · DimGeography · DimBrand       │
│   FactSales (transaction grain) · FactForecast (Country×Brand×Year) │
└──────────────────────────────────────┬───────────────────────────────┘
                                        ▼
                          ┌──────────────────────┐
                          │  4. VALIDATE          │
                          │  - FK / orphan checks │
                          │  - range checks       │
                          │  - row reconciliation │
                          └─────────┬─────────────┘
                                    ▼
                          ┌──────────────────────┐
                          │  5. LOAD              │
                          │  - write CSV outputs  │
                          │  - structured logging │
                          └──────────────────────┘
```

## 2.2 Transformation Rationale

| Step | Transformation | Why it's required |
|---|---|---|
| Extract | Wrap JSON load in try/except for `FileNotFoundError`/`JSONDecodeError` | Production pipelines must fail loudly and traceably, not silently |
| Clean 1 | Drop 218,008 exact-duplicate rows | Prevents 3.7x overstatement of every sales KPI downstream |
| Clean 2 | Strip whitespace on all text columns | Prevents silent grouping errors in Power BI (e.g., "USA " vs "USA" treated as different members) |
| Clean 3 | Drop `Color` (= duplicate of `Subcategory`) | Removes a misleading, redundant attribute before it reaches the model |
| Clean 4 | Parse `OrderDate` → `datetime64` | Enables joining to `DimDate` and using Power BI time-intelligence DAX functions |
| Clean 5 | Filter `Quantity <= 0` / `Net Price <= 0` | Defensive validation — guards against future data drops containing invalid transactions |
| Clean 6 | Fill `Name`/`Education`/`Occupation` nulls with `"Unknown"` | Preserves all 8,868 customers in `DimCustomer` while making nulls explicit and filterable (vs. blank/NaN, which Power BI handles poorly) |
| Clean 7 | Cast keys to `int`, prices to rounded `float` | Guarantees consistent join types and avoids floating-point join failures |
| Transform | Split wide table into Dim/Fact tables, generate surrogate `SalesKey` and `ForecastKey` | Normalization removes redundancy (each product/customer attribute stored once), enables a proper star schema, and gives Forecast its own grain |
| Transform | Build `DimBrand` and `DimGeography` as conformed dimensions shared by both facts | Solves the differing-grain problem — both facts can now relate to the same Brand/Country dimensions |
| Transform | Build `DimDate` spanning full calendar years 2008–2009 (not just transaction dates) | Required by Power BI's "mark as date table" best practice — continuous calendar avoids gaps in time-intelligence calculations |
| Validate | FK/orphan checks, non-positive amount checks | Ensures the model loads into Power BI without relationship errors and protects KPI accuracy |
| Load | Write CSVs + structured log file | CSV is DB-agnostic (loadable into SQL Server/Postgres/Power BI directly); log file gives an audit trail for the hiring manager and future operators |

## 2.3 Python ETL Code

The full production script is provided as `src/etl_orion.py` (included in the deliverable package). Key characteristics:

- Modular functions per stage (`extract`, `clean_sales`, `clean_forecast`, `build_dim_*`, `build_fact_*`, `validate`, `load`)
- `logging` module writes to both console and `docs/etl_run.log` (timestamped, leveled)
- Explicit `try/except` around file I/O and JSON parsing
- A hard `ValueError` is raised if validation fails — the pipeline will not load bad data
- Idempotent: re-running produces identical output

### Actual run results (from `etl_run.log`)

```
EXTRACT  | Sales: 298,246 rows, 18 columns
EXTRACT  | Forecast: 33 rows, 4 columns
CLEAN    | Sales - removed 218,008 exact-duplicate rows (298,246 -> 80,238)
CLEAN    | Sales - dropped redundant 'Color' column
CLEAN    | Sales - filled 50,441 nulls in Name/Education/Occupation with 'Unknown'
TRANSFORM| DimProduct  - 2,495 unique products
TRANSFORM| DimCustomer - 8,868 unique customers
TRANSFORM| DimGeography- 3 country records
TRANSFORM| DimBrand    - 11 brands
TRANSFORM| DimDate     - 731 days (2008-01-01 -> 2009-12-31)
TRANSFORM| FactSales   - 80,238 rows, Total Sales = $42,644,947.03
TRANSFORM| FactForecast- 33 rows, Total Forecast  = $39,004,512.00
VALIDATE | All referential integrity and range checks PASSED
LOAD     | 7 CSV files written successfully
```

---

# PHASE 3 — DATA MODELING

## 3.1 Recommended Schema: **Star Schema (with one conformed-dimension snowflake branch)**

**Justification:**
- Star schema is preferred for Power BI: simpler relationships, faster DAX evaluation (fewer joins for the VertiPaq engine), easier for business users to navigate in Fields pane.
- A pure star is used for `FactSales`. To solve the **grain mismatch** with `FactForecast`, two dimensions (`DimBrand`, `DimGeography`) are **conformed** — shared by both fact tables. This is technically a small snowflake (Product → Brand, Customer → Geography) but is the cleanest way to let two facts at different grains coexist without duplicating attributes or building a bridge table.

## 3.2 Table Catalog

| Table | Type | Grain | PK | FKs |
|---|---|---|---|---|
| `DimDate` | Dimension | 1 row per calendar day | `DateKey` (int, yyyymmdd) | — |
| `DimProduct` | Dimension | 1 row per product | `ProductKey` | — |
| `DimCustomer` | Dimension | 1 row per customer | `CustomerKey` | — |
| `DimBrand` | Dimension (conformed) | 1 row per brand | `BrandKey` | — |
| `DimGeography` | Dimension (conformed) | 1 row per country | `GeographyKey` | — |
| `FactSales` | Fact | 1 row per transaction line | `SalesKey` | `DateKey`, `ProductKey`, `CustomerKey` |
| `FactForecast` | Fact | 1 row per Country × Brand × Year | `ForecastKey` | `GeographyKey`, `BrandKey`, `Year` |

## 3.3 Entity Relationship Diagram (text wireframe)

```
                         ┌──────────────┐
                         │   DimDate     │
                         │ DateKey (PK)  │
                         │ Year, Quarter │
                         │ Month, etc.   │
                         └──────┬────────┘
                   1 ────────────┴──────────── *
                         │              │
              ┌──────────▼──────┐  ┌────▼─────────────┐
              │   FactSales      │  │  FactForecast     │
              │ SalesKey (PK)    │  │ ForecastKey (PK)  │
              │ DateKey (FK)     │  │ Year (FK, via     │
              │ ProductKey (FK)  │  │   DimDate.Year -  │
              │ CustomerKey (FK) │  │   inactive rel.)  │
              │ Quantity         │  │ BrandKey (FK)     │
              │ NetPrice         │  │ GeographyKey (FK) │
              │ SalesAmount      │  │ Forecast ($)      │
              └────┬─────────┬───┘  └────┬─────────┬────┘
           * ──────┘         └────── *   │         │
   ┌─────────────┐    ┌──────────────┐   │         │
   │ DimProduct   │    │ DimCustomer   │  │         │
   │ ProductKey   │───▶│ CustomerKey   │  │         │
   │ Brand        │ *  │ CountryRegion │  │         │
   │ Category     │    │ ...           │  │         │
   └──────┬───────┘    └──────┬────────┘  │         │
          │ * (Brand text)     │ * (Country text)    │
          ▼ 1                  ▼ 1                   │
   ┌─────────────┐      ┌──────────────┐             │
   │  DimBrand    │◀─────────────────────────────────┘ (1)
   │  BrandKey    │
   │  Brand       │
   └─────────────┘
   ┌─────────────┐
   │ DimGeography │◀──────────────────────────────────── (1)
   │ GeographyKey │
   │ CountryRegion│
   │ Continent    │
   └─────────────┘
```

> **Note on `DimProduct`/`DimCustomer` → `DimBrand`/`DimGeography`:** These are *not* physical Power BI relationships (to avoid ambiguous multi-path joins). Instead, `DimBrand` and `DimGeography` relate **only to `FactForecast`**, while filtering of `FactSales` by Brand/Country happens via `DimProduct.Brand` / `DimCustomer.CountryRegion`. A measure (`[Forecast]`, Phase 4) uses `TREATAS` to project the Sales-side filter context onto the Forecast table, keeping the model a clean star with no bidirectional cross-filtering.

## 3.4 Handling the Sales ↔ Forecast Grain Mismatch

| Aspect | FactSales | FactForecast |
|---|---|---|
| Grain | Transaction (date-level) | Country × Brand × Year |
| Date granularity | Full date | Year only |
| Geography granularity | City/State/Country | Country only |
| Product granularity | Individual ProductKey | Brand only |

**Solution implemented:**
1. `DimDate` relates to `FactSales` via `DateKey` (active, 1-to-many) — full date-level analysis.
2. `DimDate.Year` relates to `FactForecast.Year` via a **second relationship marked inactive** (Power BI does not allow two active relationships from one table). DAX measures activate it with `USERELATIONSHIP`.
3. `DimBrand` and `DimGeography` relate to `FactForecast` (1-to-many, active). DAX measures use `TREATAS` to translate the Brand/Country filters coming from `DimProduct`/`DimCustomer` (via Sales-side slicers) into `FactForecast`.
4. This lets the same page slicers (Country, State, Brand, Year) filter **both** visuals correctly despite the grain difference.

## 3.5 Date Dimension Design

`DimDate` is a continuous calendar (731 rows, 2008-01-01 → 2009-12-31), built independently of transaction dates (best practice — avoids gaps). Columns: `DateKey` (yyyymmdd int, PK), `Date`, `Year`, `Quarter`, `QuarterName`, `Month`, `MonthName`, `MonthShort`, `YearMonth`, `Day`, `DayName`, `WeekdayNum`, `IsWeekend`.

In Power BI: **Mark as Date Table** using the `Date` column; both relationships described above are built from here.

---

# PHASE 4 — DAX MEASURES

All measures assume table/column names exactly as produced by the ETL output.

```dax
-- 1. Total Sales $
Total Sales $ =
SUM ( FactSales[SalesAmount] )


-- 2. Sales 2009
Sales 2009 =
CALCULATE (
    [Total Sales $],
    DimDate[Year] = 2009
)


-- 3. Sales 2008
Sales 2008 =
CALCULATE (
    [Total Sales $],
    DimDate[Year] = 2008
)


-- 4. Sales Growth %
Sales Growth % =
DIVIDE (
    [Sales 2009] - [Sales 2008],
    [Sales 2008],
    0
)


-- 5. YoY Difference  (absolute $ change, also usable with a Year slicer)
YoY Difference =
VAR CurrentYearSales = [Total Sales $]
VAR PriorYearSales =
    CALCULATE (
        [Total Sales $],
        DATEADD ( DimDate[Date], -1, YEAR )
    )
RETURN
    CurrentYearSales - PriorYearSales


-- 6. Top 10 Products  (ranking helper measure, used with a Top N filter / visual filter)
Product Sales Rank =
RANKX (
    ALLSELECTED ( DimProduct[ProductName] ),
    [Total Sales $],
    ,
    DESC,
    DENSE
)
-- Applied via a visual-level "Top N" filter on Product Name using [Total Sales $],
-- or by filtering Product Sales Rank <= 10 in a calculated table / measure-based filter.


-- 7. Product Share %
Product Share % =
DIVIDE (
    [Total Sales $],
    CALCULATE ( [Total Sales $], ALL ( DimProduct ) ),
    0
)


-- 8. Forecast vs Actual
-- Forecast measure (translated from Brand/Geography dims into Sales filter context)
Forecast $ =
CALCULATE (
    SUM ( FactForecast[Forecast] ),
    TREATAS ( VALUES ( DimProduct[Brand] ), DimBrand[Brand] ),
    TREATAS ( VALUES ( DimCustomer[CountryRegion] ), DimGeography[CountryRegion] ),
    USERELATIONSHIP ( DimDate[Year], FactForecast[Year] )
)

Forecast vs Actual =
VAR Actual = [Total Sales $]
VAR Plan   = [Forecast $]
RETURN
    Actual - Plan


-- 9. Forecast Variance  (% variance)
Forecast Variance =
DIVIDE (
    [Total Sales $] - [Forecast $],
    [Forecast $],
    0
)


-- 10. Forecast Accuracy %
Forecast Accuracy % =
1 - ABS (
    DIVIDE (
        [Total Sales $] - [Forecast $],
        [Forecast $],
        0
    )
)
```

### Notes on implementation
- Measures 8–10 require: (a) the inactive `DimDate[Year] ↔ FactForecast[Year]` relationship, and (b) active relationships `DimBrand → FactForecast` and `DimGeography → FactForecast`. `TREATAS` propagates the Brand/Country context selected via Product/Customer slicers onto these dimensions.
- Because forecast data exists only for 2009, `[Forecast vs Actual]` and related measures are meaningful only when the Year filter = 2009 (or no year filter, since `Forecast $` will simply return the 2009 total in that case via the inactive relationship).

---

# PHASE 5 — DASHBOARD DESIGN

## 5.1 Single-Page Executive Dashboard — "Orion Sales & Forecast Overview"

### Wireframe (16:9 canvas, grid reference)

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│ TITLE BAR:  "Orion Sales & Forecast — Executive Dashboard"      [Country][State]   │
│                                                                  [Year][Brand] slc  │
├──────────────┬──────────────┬──────────────┬──────────────┬──────────────────────┤
│ KPI CARD      │ KPI CARD      │ KPI CARD      │ KPI CARD      │  KPI CARD             │
│ Total Sales $ │ Sales 2009    │ Sales Growth %│ Forecast $    │  Forecast Accuracy %  │
├──────────────┴──────────────┴──────────────┴──────────────┴──────────────────────┤
│  ┌───────────────────────────────┐  ┌──────────────────────────────────────────┐  │
│  │ LINE CHART                      │  │ CLUSTERED COLUMN CHART                     │  │
│  │ Monthly Sales Trend (2008 vs   │  │ Sales 2009 vs 2008 vs Forecast, by Country │  │
│  │ 2009), drill from Year→Qtr→Mon │  │ (group axis: Country, series: 08/09/Fcst)  │  │
│  └───────────────────────────────┘  └──────────────────────────────────────────┘  │
├──────────────────────────────┬─────────────────────────────────────────────────────┤
│  HORIZONTAL BAR CHART          │  TABLE / MATRIX                                     │
│  Top 10 Products by Sales $    │  Top Customer Behavior:                             │
│  (with Product Share % label)  │  Customer | Total Spend | Top Category | Qtrly Qty │
│                                 │  (sorted desc by spend, top 10 customers)          │
├─────────────────────────────────────────────────────────────────────────────────┤
│  TREEMAP                                                  │  DONUT CHART              │
│  Sales by Category → Subcategory (drill-down)            │  Sales by Brand           │
└────────────────────────────────────────────────────────────────────────────────────┘
```

## 5.2 Visual Specifications

| Position | Visual Type | Measures/Fields | Purpose |
|---|---|---|---|
| Top KPI strip | 5x Card visuals | `Total Sales $`, `Sales 2009`, `Sales Growth %`, `Forecast $`, `Forecast Accuracy %` | At-a-glance headline KPIs |
| Mid-left | Line chart | X: `DimDate[Date]` (hierarchy Year/Quarter/Month), Y: `Total Sales $`, legend split by Year | Trend analysis; **drill-down enabled** Year → Quarter → Month |
| Mid-right | Clustered column chart | X: `DimGeography[CountryRegion]`, Y values: `Sales 2009`, `Sales 2008`, `Forecast $` | Directly answers "compare 2009 vs 2008" and "forecast vs actual" by country, filterable by Country/State |
| Lower-left | Horizontal bar chart | X: `Total Sales $`, Y: `DimProduct[ProductName]` filtered Top 10 via `Product Sales Rank`, data label = `Product Share %` | Top 10 products + their share of total sales |
| Lower-right | Table/matrix | `DimCustomer[Name/CustomerCode]`, `Total Sales $`, `DimProduct[Category]` (top category per customer), quarterly quantity columns | Top customer behavior across the year span — sortable, with conditional formatting (data bars) on spend |
| Bottom-left | Treemap | Group: `Category` → `Subcategory`, Size: `Total Sales $` | Drill-down category analysis; click-to-filter the whole page |
| Bottom-right | Donut chart | Legend: `DimBrand[Brand]`, Value: `Total Sales $` | Brand-mix composition, cross-filters Top 10 Products visual |

## 5.3 Filters & Slicers

- **Top-bar slicers (apply to whole page):** `CountryRegion`, `State` (dependent/cascading on Country), `Year` (2008/2009), `Brand`
- These slicers drive both `FactSales`-based visuals (direct relationships) and `FactForecast`-based KPI/column chart (via the `TREATAS`/inactive-relationship DAX pattern in Phase 4) — so the **same controls filter both granularities** as required by the assessment.
- A page-level **"Reset filters"** bookmark button is recommended for UX.

## 5.4 Drill-Down & Interactivity

- Line chart: native Power BI drill-down (Year → Quarter → Month → Day), with "Expand all" enabled for trend storytelling.
- Treemap: click a Category to cross-filter Top 10 Products and the customer table to that category.
- Top 10 Products bar chart: tooltip page showing the product's monthly trend (tooltip drill-through).
- Right-click drill-through page (optional, Phase 5 stretch): "Customer Detail" page showing a single customer's full purchase history — accessible by right-clicking any customer row.

## 5.5 UX Recommendations

- Use a consistent color palette: one accent color for "Actual" series, a contrasting muted color/pattern for "Forecast" series (dashed line or hatched bars) so users instantly distinguish plan vs. actual.
- KPI cards should include conditional-formatting icons (▲/▼) for `Sales Growth %` and `Forecast Variance` to support fast visual scanning.
- Keep the Country/State/Year/Brand slicer bar pinned/sticky at the top so it remains visible while scrolling on smaller screens.
- Add tooltips on every visual explaining the metric (e.g., "Forecast Accuracy % = 1 − |Variance|") for self-service users unfamiliar with the formulas.
- Mobile layout: stack KPI cards 2-per-row, single-column visual order: KPIs → Trend → Country comparison → Top Products → Customers → Category mix.

---

# PHASE 6 — DOCUMENTATION

## 6.1 ETL Documentation Summary

- **Source:** Two flat JSON exports (`Sales.json`, `forecast.json`), provided as a denormalized transactional extract and a pre-aggregated forecast extract respectively.
- **Process:** Python/pandas pipeline (`src/etl_orion.py`) executes Extract → Clean → Transform → Validate → Load. Full execution log retained at `docs/etl_run.log`.
- **Critical correction:** 73.1% of raw Sales rows were exact duplicates; removing them was the single most impactful cleaning step (reduces inflated revenue figures by ~3.7x).
- **Output:** 7 CSV tables (1 calendar dimension, 4 descriptive dimensions, 2 fact tables) ready for import into Power BI or any relational database (schema-compatible with SQL Server/PostgreSQL `CREATE TABLE` DDL — PK/FK columns are explicit integers).

## 6.2 Data Model Documentation Summary

- Star schema with two conformed dimensions (`DimBrand`, `DimGeography`) bridging two facts of different grain.
- `DimDate` is a standalone, gap-free calendar marked as the official Date Table.
- All relationships are single-direction, 1-to-many, from Dimension → Fact (no bidirectional cross-filtering), in line with Power BI performance best practices for the VertiPaq engine.
- One inactive relationship (`DimDate[Year]` ↔ `FactForecast[Year]`) is activated on-demand via `USERELATIONSHIP` inside forecast-related measures.

## 6.3 Assumptions

1. The 218,008 duplicate rows in `Sales.json` represent a data-export artifact (e.g., repeated extraction without deduplication), not legitimate repeat transactions — supported by every field being byte-identical including `Net Price` to 4 decimal places.
2. `Color` was assumed mis-mapped (containing Subcategory data) based on 100% consistency across all 80,238 cleaned rows; it was removed rather than renamed, since it adds no new information.
3. Customers with `null` Name/Education/Occupation are treated as a distinct, valid segment ("Unknown" demographic profile) rather than data-entry errors, since the nulls are consistent (always all three fields together) and represent 62.9% of the base — too large to be incidental.
4. `forecast.json`'s 2009-only coverage is assumed intentional (a single-year planning cycle); Forecast Accuracy/Variance measures are therefore most meaningful when the dashboard's Year slicer = 2009.
5. `Net Price` is assumed to be the **per-unit** net price (post-discount, pre-tax); `SalesAmount = Quantity × Net Price`.
6. Currency for both Sales and Forecast is assumed to be USD (consistent with "$" requirement in the assessment).

## 6.4 Business Insights (from cleaned data)

- **Total Sales (2008–2009): $42.64M** vs. **Total 2009 Forecast: $39.00M** — actuals for the full two-year window exceed the single-year 2009 forecast baseline, indicating either strong growth or an initially conservative plan.
- The **United States** dominates both sales volume (237,630 of 298,246 raw line items) and forecast allocation, followed by Germany and then China — both facts agree directionally on market priority.
- **Fabrikam, Contoso, and Adventure Works** are consistently the top forecasted brands across all three countries — these should be cross-checked first against actual `Total Sales $` by brand to flag over/under-performance.
- The high proportion (62.9%) of customers with unknown demographics represents a **data-collection gap** — recommend the business prioritize capturing Education/Occupation at point-of-sale for a richer segmentation in future cycles.

## 6.5 GitHub Repository Structure

```
orion-sales-analytics/
├── README.md                     # Project overview, setup, how to run
├── src/
│   └── etl_orion.py               # Production ETL pipeline (this submission)
├── data/
│   ├── raw/
│   │   ├── Sales.json
│   │   └── forecast.json
│   └── processed/
│       ├── dim_date.csv
│       ├── dim_product.csv
│       ├── dim_customer.csv
│       ├── dim_brand.csv
│       ├── dim_geography.csv
│       ├── fact_sales.csv
│       └── fact_forecast.csv
├── powerbi/
│   └── Orion_Dashboard.pbix        # Power BI model + dashboard
├── docs/
│   ├── ETL_Documentation.md
│   ├── Data_Model_Documentation.md
│   ├── DAX_Measures.md
│   ├── Dashboard_Wireframe.png
│   ├── Assumptions.md
│   └── etl_run.log                 # Execution audit log
└── requirements.txt                 # pandas, numpy
```

---

# PHASE 7 — INTERVIEW PREPARATION

## 7.1 Likely Reviewer Questions & Recommended Answers

**Q1: "Why did you remove 73% of the rows? How do you know they were duplicates and not legitimate repeat orders?"**
> *Answer:* "I tested with `df.duplicated()` across all 18 columns, including `Net Price` to four decimal places — every field matched exactly, including the order date. Legitimate repeat purchases would still vary in at least one dimension (different date, different quantity, or a transaction ID). The absence of any unique identifier combined with byte-identical rows strongly indicates an export artifact, likely a join fan-out or repeated extraction. I documented this assumption explicitly so the business can confirm against the source system."

**Q2: "Why is `Color` not in your final model?"**
> *Answer:* "I profiled it and found it was 100% identical to `Subcategory` for every product — it appears the source system mapped the wrong column. Keeping a duplicated, mislabeled field would confuse business users (someone filtering by 'Color' would actually be filtering by Subcategory), so I removed it and flagged the finding for the source-system owners."

**Q3: "How do you handle the fact that Sales and Forecast are at completely different grains?"**
> *Answer:* "I built two conformed dimensions — `DimBrand` and `DimGeography` — that both facts can relate to. `FactSales` keeps its full transaction-level relationships (Date, Product, Customer). `FactForecast` relates directly to `DimBrand`/`DimGeography`/`DimDate[Year]` (inactive). In DAX, I use `TREATAS` to push the Brand/Country context selected via the Sales-side dimensions onto the Forecast table, so slicers work uniformly across both grains without needing bidirectional relationships, which would hurt performance and create ambiguity."

**Q4: "Why CSV output instead of directly loading to a database?"**
> *Answer:* "CSV keeps the deliverable portable and database-agnostic — the hiring manager can load it into Power BI directly, or `bcp`/`COPY` it into SQL Server/Postgres using the PK/FK structure I've defined. For a production deployment I'd extend the `load()` function with a SQLAlchemy connection and parameterized `to_sql()` calls, but I kept the assessment deliverable dependency-light."

**Q5: "Why mark `DimDate[Year]`↔`FactForecast[Year]` as inactive instead of just using `RELATED`/a direct relationship?"**
> *Answer:* "`DimDate` already has one active relationship to `FactSales` via `DateKey`. Power BI only permits one active relationship path between any two tables in the filter-propagation graph at a time without creating ambiguity. Making the Year-to-Forecast link inactive and activating it inside specific measures via `USERELATIONSHIP` keeps the default behavior clean for Sales while still enabling Forecast-aware measures on demand."

## 7.2 Technical Challenges Encountered & How They Were Solved

| Challenge | Resolution |
|---|---|
| Detecting the duplication issue wasn't obvious from a quick `.head()` — required full-row `.duplicated()` profiling | Ran systematic profiling (`isna().sum()`, `duplicated()`, `nunique()`, dtype checks, range checks) before writing any transformation code |
| Differing fact grains could not use a standard single relationship | Introduced conformed dimensions (`DimBrand`, `DimGeography`) + `TREATAS`/`USERELATIONSHIP` DAX pattern |
| `OrderDate` as string would break time intelligence | Explicit `pd.to_datetime` parse with format string + error coercion and row-drop logging |
| Large null blocks (62.9%) in demographic fields could mislead "missing data" alarms | Investigated pattern (always all 3 fields together) before deciding it was a segment, not corruption — filled with explicit "Unknown" rather than imputing or dropping |
| Ensuring the pipeline is auditable | Implemented structured `logging` to both console and a persisted `etl_run.log`, with row-count reconciliation at every stage |

## 7.3 Anticipated Follow-Up / Stretch Questions

- *"How would this scale to millions of rows / multiple years?"* → Note that `DimDate` is pre-built independent of data, dedup logic is vectorized (no row-by-row loops), and the same script structure would work; for true scale, recommend chunked reads (`pd.read_json` with lines=True / Spark) and incremental loads keyed on a true business key once the source system provides one.
- *"What would you change about the source system?"* → Add a unique `OrderLineID`, fix the `Color`/`Subcategory` mapping, populate demographic fields consistently, and extend forecast exports to cover 2008 (or future years) for richer YoY forecast analysis.
- *"How would you test this pipeline?"* → Unit tests per transformation function (e.g., assert dedup count, assert no nulls in PK columns, assert `SalesAmount = Quantity * NetPrice`), plus the validation stage already built in as an integration-level smoke test.

---

*End of submission package. Accompanying files: `src/etl_orion.py`, `docs/etl_run.log`, and 7 processed CSV tables in `data/`.*
