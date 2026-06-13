#  Sales & Forecast Analytics

A Power BI analytics project: cleans raw Sales & Forecast JSON exports, builds a star-schema data model, and powers an executive dashboard with sales, growth, and forecast-accuracy KPIs.

## What this project does

- Cleans **298,246 raw sales records** down to **80,238 valid transactions** (removes 218K duplicate rows + fixes a mislabeled column)
- Builds a clean **star schema**: 5 dimension tables + 2 fact tables
- Provides **10 ready-to-use DAX measures** for Power BI
- Includes a single-page **executive dashboard** design

## Project Structure

```
orion-sales-analytics/
├── src/
│   └── etl_orion.py              # ETL script (run this)
├── notebooks/
│   └── Orion_ETL_Notebook.ipynb  # Same pipeline, step-by-step in Jupyter
├── data/
│   ├── raw/                       # Input: Sales.json, forecast.json
│   └── processed/                 # Output: 7 CSV tables (created by ETL)
├── docs/
│   ├── Project_Documentation.md   # Full write-up: model, DAX, dashboard
│   └── etl_run.log                 # Log file (created by ETL)
└── requirements.txt
```

## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Add input files
Place `Sales.json` and `forecast.json` into `data/raw/`.

### 3. Run the ETL
```bash
python src/etl_orion.py
```

This produces 7 CSV files in `data/processed/`:

| File | Description |
|---|---|
| `dim_date.csv` | Calendar dimension (2008–2009) |
| `dim_product.csv` | Product, Brand, Category, Subcategory |
| `dim_customer.csv` | Customer demographics & location |
| `dim_geography.csv` | Country / Continent |
| `dim_brand.csv` | Brand list |
| `fact_sales.csv` | Cleaned transaction-level sales |
| `fact_forecast.csv` | Forecast by Country × Brand × Year |

### 4. Load into Power BI
Import the 7 CSVs, build relationships, mark `dim_date` as the Date Table, and add the DAX measures from `docs/Project_Documentation.md`.

## Key Result

| Metric | Value |
|---|---|
| Total Sales (2008–2009) | $42,644,947 |
| Total Forecast (2009) | $39,004,512 |
| Duplicate rows removed | 218,008 (73.1%) |

## Tech Stack

- Python, pandas
- Power BI (Star Schema, DAX)
- Jupyter Notebook (exploration)
