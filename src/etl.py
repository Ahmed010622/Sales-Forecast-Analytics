"""
Technical Assessment - ETL Pipeline
Pipeline stages:
    1. EXTRACT  - load raw JSON files
    2. CLEAN    - de-duplicate, fix types, trim strings, handle nulls
    3. TRANSFORM- normalize into Dimension & Fact tables (star schema)
    4. VALIDATE - referential integrity, range checks, row-count reconciliation
    5. LOAD     - write CSV outputs + write log file
"""

import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import numpy as np

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = r"C:\Users\ahmed.deabes\Downloads\orion-sales-analytics\Data\Raw"  # Assuming raw data is provided at this absolute path
OUT_DIR = os.path.join(BASE_DIR, "data\\processed")
LOG_DIR = os.path.join(BASE_DIR, "docs")

SALES_FILE = os.path.join(RAW_DIR, "Sales.json")
FORECAST_FILE = os.path.join(RAW_DIR, "forecast.json")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --------------------------------------------------------------------------- #
# LOGGING
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "etl_run.log"), mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("orion_etl")


# --------------------------------------------------------------------------- #
# STAGE 1 - EXTRACT
# --------------------------------------------------------------------------- #
def extract(path: str, name: str) -> pd.DataFrame:
    log.info(f"EXTRACT | Loading {name} from {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        log.error(f"EXTRACT | File not found: {path}")
        raise e
    except json.JSONDecodeError as e:
        log.error(f"EXTRACT | Invalid JSON in {path}: {e}")
        raise e

    df = pd.DataFrame(data)
    log.info(f"EXTRACT | {name}: {len(df):,} rows, {len(df.columns)} columns")
    return df


# --------------------------------------------------------------------------- #
# STAGE 2 - CLEAN SALES
# --------------------------------------------------------------------------- #
def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    log.info("CLEAN | Sales - starting cleaning routine")
    initial_rows = len(df)

    # 2.1 Remove exact-duplicate transaction rows (no unique sales key provided)
    dupes = df.duplicated().sum()
    df = df.drop_duplicates().reset_index(drop=True)
    log.info(f"CLEAN | Sales - removed {dupes:,} exact-duplicate rows "
             f"({initial_rows:,} -> {len(df):,})")

    # 2.2 Trim whitespace on all string/object columns
    str_cols = df.select_dtypes(include="object").columns
    for c in str_cols:
        df[c] = df[c].astype(str).where(df[c].notna(), df[c])
        df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)
    log.info(f"CLEAN | Sales - trimmed whitespace on {len(str_cols)} text columns")

    # 2.3 Drop the "Color" column - it is a mis-mapped duplicate of "Subcategory"
    #     (confirmed during discovery: Color == Subcategory for every product)
    if "Color" in df.columns:
        mismatches = (df["Color"] != df["Subcategory"]).sum()
        if mismatches == 0:
            df = df.drop(columns=["Color"])
            log.info("CLEAN | Sales - dropped redundant 'Color' column "
                     "(identical to 'Subcategory' for all rows)")
        else:
            log.warning(f"CLEAN | Sales - 'Color' differs from 'Subcategory' in "
                        f"{mismatches} rows; column retained for review")

    # 2.4 Convert OrderDate (string M/D/YYYY) -> datetime
    df["OrderDate"] = pd.to_datetime(df["OrderDate"], format="%m/%d/%Y", errors="coerce")
    bad_dates = df["OrderDate"].isna().sum()
    if bad_dates:
        log.warning(f"CLEAN | Sales - {bad_dates} rows had unparseable OrderDate "
                     f"and were dropped")
        df = df.dropna(subset=["OrderDate"])

    # 2.5 Validate numeric ranges
    invalid_qty = df[df["Quantity"] <= 0]
    if len(invalid_qty):
        log.warning(f"CLEAN | Sales - removing {len(invalid_qty)} rows with Quantity <= 0")
        df = df[df["Quantity"] > 0]

    invalid_price = df[df["Net Price"] <= 0]
    if len(invalid_price):
        log.warning(f"CLEAN | Sales - removing {len(invalid_price)} rows with Net Price <= 0")
        df = df[df["Net Price"] > 0]

    # 2.6 Handle nulls in demographic columns - convert to explicit 'Unknown'
    for col in ["Name", "Education", "Occupation"]:
        nulls = df[col].isna().sum()
        df[col] = df[col].fillna("Unknown")
        log.info(f"CLEAN | Sales - filled {nulls:,} nulls in '{col}' with 'Unknown'")

    # 2.7 Standardize key dtypes
    df["ProductKey"] = df["ProductKey"].astype(int)
    df["CustomerKey"] = df["CustomerKey"].astype(int)
    df["Quantity"] = df["Quantity"].astype(int)
    df["Net Price"] = df["Net Price"].astype(float).round(4)

    log.info(f"CLEAN | Sales - finished. Final row count: {len(df):,}")
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# STAGE 2 - CLEAN FORECAST
# --------------------------------------------------------------------------- #
def clean_forecast(df: pd.DataFrame) -> pd.DataFrame:
    log.info("CLEAN | Forecast - starting cleaning routine")
    initial_rows = len(df)

    dupes = df.duplicated().sum()
    df = df.drop_duplicates().reset_index(drop=True)
    log.info(f"CLEAN | Forecast - removed {dupes} duplicate rows "
             f"({initial_rows} -> {len(df)})")

    for c in ["CountryRegion", "Brand"]:
        df[c] = df[c].astype(str).str.strip()

    df["Forecast"] = df["Forecast"].astype(float).round(2)
    df["Year"] = df["Year"].astype(int)

    invalid = df[df["Forecast"] <= 0]
    if len(invalid):
        log.warning(f"CLEAN | Forecast - {len(invalid)} rows with Forecast <= 0 found, retained for review")

    log.info(f"CLEAN | Forecast - finished. Final row count: {len(df)}")
    return df


# --------------------------------------------------------------------------- #
# STAGE 3 - TRANSFORM: BUILD DIMENSIONAL MODEL
# --------------------------------------------------------------------------- #
def build_dim_product(sales: pd.DataFrame) -> pd.DataFrame:
    cols = ["ProductKey", "Product Name", "Brand", "Category", "Subcategory"]
    dim = sales[cols].drop_duplicates(subset="ProductKey").reset_index(drop=True)
    dim = dim.rename(columns={
        "Product Name": "ProductName",
    })
    log.info(f"TRANSFORM | DimProduct - {len(dim):,} unique products")
    return dim


def build_dim_customer(sales: pd.DataFrame) -> pd.DataFrame:
    cols = ["CustomerKey", "Customer Code", "Name", "Education", "Occupation",
            "Continent", "City", "State", "CountryRegion"]
    dim = sales[cols].drop_duplicates(subset="CustomerKey").reset_index(drop=True)
    dim = dim.rename(columns={"Customer Code": "CustomerCode"})
    log.info(f"TRANSFORM | DimCustomer - {len(dim):,} unique customers")
    return dim


def build_dim_geography(sales: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    geo_sales = sales[["Continent", "CountryRegion"]].drop_duplicates()
    geo_fc = forecast[["CountryRegion"]].drop_duplicates()
    geo = geo_sales.merge(geo_fc.drop_duplicates(), on="CountryRegion", how="outer")
    geo["Continent"] = geo["Continent"].fillna("Unknown")
    geo = geo.drop_duplicates(subset="CountryRegion").reset_index(drop=True)
    geo.insert(0, "GeographyKey", range(1, len(geo) + 1))
    log.info(f"TRANSFORM | DimGeography - {len(geo)} country records")
    return geo


def build_dim_brand(sales: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    brands = pd.Index(sales["Brand"].unique()).union(forecast["Brand"].unique())
    dim = pd.DataFrame({"Brand": sorted(brands)})
    dim.insert(0, "BrandKey", range(1, len(dim) + 1))
    log.info(f"TRANSFORM | DimBrand - {len(dim)} brands")
    return dim


def build_dim_date(sales: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    min_date = sales["OrderDate"].min()
    max_date = sales["OrderDate"].max()

    # Extend the calendar to cover forecast years fully (Jan 1 - Dec 31)
    fc_years = forecast["Year"].unique()
    fc_min = pd.Timestamp(year=int(min(fc_years)), month=1, day=1)
    fc_max = pd.Timestamp(year=int(max(fc_years)), month=12, day=31)

    start = min(min_date, fc_min)
    end = max(max_date, fc_max)

    dates = pd.date_range(start=start, end=end, freq="D")
    dim = pd.DataFrame({"Date": dates})
    dim["DateKey"] = dim["Date"].dt.strftime("%Y%m%d").astype(int)
    dim["Year"] = dim["Date"].dt.year
    dim["Quarter"] = dim["Date"].dt.quarter
    dim["QuarterName"] = "Q" + dim["Quarter"].astype(str) + " " + dim["Year"].astype(str)
    dim["Month"] = dim["Date"].dt.month
    dim["MonthName"] = dim["Date"].dt.strftime("%B")
    dim["MonthShort"] = dim["Date"].dt.strftime("%b")
    dim["YearMonth"] = dim["Date"].dt.strftime("%Y-%m")
    dim["Day"] = dim["Date"].dt.day
    dim["DayName"] = dim["Date"].dt.strftime("%A")
    dim["WeekdayNum"] = dim["Date"].dt.weekday + 1  # 1=Mon
    dim["IsWeekend"] = dim["WeekdayNum"].isin([6, 7])

    dim = dim[["DateKey", "Date", "Year", "Quarter", "QuarterName", "Month",
               "MonthName", "MonthShort", "YearMonth", "Day", "DayName",
               "WeekdayNum", "IsWeekend"]]
    log.info(f"TRANSFORM | DimDate - {len(dim):,} days "
             f"({dim['Date'].min().date()} -> {dim['Date'].max().date()})")
    return dim


def build_fact_sales(sales: pd.DataFrame) -> pd.DataFrame:
    fact = sales[["ProductKey", "CustomerKey", "OrderDate", "Quantity", "Net Price"]].copy()
    fact["DateKey"] = fact["OrderDate"].dt.strftime("%Y%m%d").astype(int)
    fact["SalesAmount"] = (fact["Quantity"] * fact["Net Price"]).round(2)
    fact = fact.rename(columns={"Net Price": "NetPrice"})
    fact.insert(0, "SalesKey", range(1, len(fact) + 1))
    fact = fact[["SalesKey", "DateKey", "ProductKey", "CustomerKey",
                  "Quantity", "NetPrice", "SalesAmount"]]
    log.info(f"TRANSFORM | FactSales - {len(fact):,} rows, "
             f"Total Sales = ${fact['SalesAmount'].sum():,.2f}")
    return fact


def build_fact_forecast(forecast: pd.DataFrame,
                         dim_brand: pd.DataFrame,
                         dim_geo: pd.DataFrame) -> pd.DataFrame:
    fact = forecast.merge(dim_brand, on="Brand", how="left")
    fact = fact.merge(dim_geo[["GeographyKey", "CountryRegion"]], on="CountryRegion", how="left")
    fact = fact[["Year", "GeographyKey", "BrandKey", "Forecast"]].copy()
    fact.insert(0, "ForecastKey", range(1, len(fact) + 1))
    log.info(f"TRANSFORM | FactForecast - {len(fact)} rows, "
             f"Total Forecast = ${fact['Forecast'].sum():,.2f}")
    return fact


# --------------------------------------------------------------------------- #
# STAGE 4 - VALIDATE
# --------------------------------------------------------------------------- #
def validate(fact_sales, fact_forecast, dim_product, dim_customer, dim_date, dim_brand, dim_geo):
    log.info("VALIDATE | Running referential integrity checks")
    errors = []

    # FK checks
    orphan_products = ~fact_sales["ProductKey"].isin(dim_product["ProductKey"])
    if orphan_products.any():
        errors.append(f"FactSales has {orphan_products.sum()} rows with orphan ProductKey")

    orphan_customers = ~fact_sales["CustomerKey"].isin(dim_customer["CustomerKey"])
    if orphan_customers.any():
        errors.append(f"FactSales has {orphan_customers.sum()} rows with orphan CustomerKey")

    orphan_dates = ~fact_sales["DateKey"].isin(dim_date["DateKey"])
    if orphan_dates.any():
        errors.append(f"FactSales has {orphan_dates.sum()} rows with orphan DateKey")

    orphan_fc_brand = fact_forecast["BrandKey"].isna().sum()
    if orphan_fc_brand:
        errors.append(f"FactForecast has {orphan_fc_brand} rows with unmatched BrandKey")

    orphan_fc_geo = fact_forecast["GeographyKey"].isna().sum()
    if orphan_fc_geo:
        errors.append(f"FactForecast has {orphan_fc_geo} rows with unmatched GeographyKey")

    # Range checks
    if (fact_sales["SalesAmount"] <= 0).any():
        errors.append("FactSales contains non-positive SalesAmount values")

    if errors:
        for e in errors:
            log.error(f"VALIDATE | FAIL - {e}")
        raise ValueError("Validation failed - see log for details")

    log.info("VALIDATE | All referential integrity and range checks PASSED")


# --------------------------------------------------------------------------- #
# STAGE 5 - LOAD
# --------------------------------------------------------------------------- #
def load(tables: dict):
    log.info("LOAD | Writing output CSV files")
    for name, df in tables.items():
        path = os.path.join(OUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        log.info(f"LOAD | wrote {path} ({len(df):,} rows, {len(df.columns)} cols)")


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
def main():
    log.info("=" * 70)
    log.info("ORION ETL PIPELINE - START")
    log.info("=" * 70)

    # Extract
    raw_sales = extract(SALES_FILE, "Sales")
    raw_forecast = extract(FORECAST_FILE, "Forecast")

    # Clean
    sales = clean_sales(raw_sales)
    forecast = clean_forecast(raw_forecast)

    # Transform - Dimensions
    dim_product = build_dim_product(sales)
    dim_customer = build_dim_customer(sales)
    dim_geo = build_dim_geography(sales, forecast)
    dim_brand = build_dim_brand(sales, forecast)
    dim_date = build_dim_date(sales, forecast)

    # Transform - Facts
    fact_sales = build_fact_sales(sales)
    fact_forecast = build_fact_forecast(forecast, dim_brand, dim_geo)

    # Validate
    validate(fact_sales, fact_forecast, dim_product, dim_customer, dim_date, dim_brand, dim_geo)

    # Load
    load({
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_geography": dim_geo,
        "dim_brand": dim_brand,
        "fact_sales": fact_sales,
        "fact_forecast": fact_forecast,
    })

    log.info("=" * 70)
    log.info("ORION ETL PIPELINE - COMPLETED SUCCESSFULLY")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
