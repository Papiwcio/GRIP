# Dataset: Data_core_2019-2024
Version: v1.0
Date: 2026-04-17
Changes:
- Initial canonical dataset build
- Added `sector_en` alongside raw `sector`
- Expanded `sector_en` to cover all observed sector categories with general English translations

## Purpose

`Data_core_2019-2024.parquet` is the canonical cleaned annual master panel derived from `Data_panel_2019-2024.parquet` for regression analysis.

Unit of observation: one row per firm (`nip`) per year (`year`).

## Source and outputs

- Input: `Data_panel_2019-2024.parquet`
- Production script: `build_core_panel.py`
- Outputs:
  - `Data_core_2019-2024.parquet`
  - `Data_core_2019-2024.xlsx`
- Validation notebook: `check_core_panel.ipynb`

## Build rules

1. Read the legacy enriched annual panel from `Data_panel_2019-2024.parquet`
2. Keep one row per (`nip`, `year`)
3. Keep `rank_2019` and create `in_rank_2019`
4. Drop `rank_2020` to `rank_2024`
5. Trim whitespace in string fields and convert empty placeholders to missing values
6. Coerce identifier and financial columns to consistent numeric types
7. Remove rows with missing `nip` or `year`
8. Remove duplicate firm-year rows if present:
   - keep the row with the highest number of non-missing values
   - break remaining ties by sorted order
9. Sort the final panel by `nip`, `year`

## Final column set

### Identification

- `nip`
- `year`
- `company`

### Structural descriptors

- `rank_2019`
- `in_rank_2019`
- `pkd`
- `pkd_description`
- `sector`
- `sector_en`
- `manufacturing`
- `owner_type`
- `owner`
- `owner_num`
- `gpw`
- `city`
- `incorporation_year_krs`
- `business_start_year`
- `regon`
- `krs`
- `legal_form`
- `sj`

### Raw financial variables

- `sales`
- `operating_result`
- `profit_before_tax`
- `income_tax`
- `net_profit`
- `depreciation`
- `exports`
- `employment`
- `wages_total`
- `total_assets`
- `fixed_assets`
- `current_assets`
- `equity`
- `total_liabilities`

### Real and log variables

- `price_index`
- `sales_real`
- `ln_sales`
- `ln_total_assets`
- `ln_employment`

### Ratios

- `profit_margin`
- `operating_margin`
- `export_ratio`
- `asset_turnover`
- `capital_ratio`
- `equity_multiplier`
- `roa`
- `roe`
- `depreciation_ratio`
- `wage_intensity`

### Productivity variables

- `sales_per_employee`
- `assets_per_employee`

### Growth variables

- `sales_growth_yoy`
- `sales_real_growth_yoy`
- `sales_log_growth_yoy`

### Data quality flags

- `has_sales`
- `has_assets`
- `has_employment`

Total columns: `58`

## Calculated variables and formulas

### Structural descriptors

- `in_rank_2019`: `1` if `rank_2019` is non-missing, otherwise `0`.
- `sector`: original Polish business classification label from the source data.
- `sector_en`: general English translation of `sector` used for analytical work. It does not replace `sector`; both are kept. `sector_en` is intended to be general and reusable across broader future analyses.
  - `budownictwo` -> `construction`
  - `chemia` -> `chemicals`
  - `energetyka` -> `energy`
  - `górnictwo i hutnictwo` -> `mining and metallurgy`
  - `handel detaliczny` -> `retail trade`
  - `handel hurtowy` -> `wholesale trade`
  - `media, telekomunkacja, IT` -> `media, telecommunications, and IT`
  - `motoryzacja` -> `automotive`
  - `ochrona zdrowia i farmacja` -> `health and pharma`
  - `paliwa` -> `fuels`
  - `produkcja` -> `production`
  - `transport` -> `transport`
  - `usługi` -> `services`
  - `żywność` -> `food`
  Note: whitespace is trimmed before mapping. If an unexpected sector value appears, `sector_en` remains missing and the build script prints a warning.
- `owner`: `"Foreign"` if `owner_type` starts with `"5"`, otherwise `"Domestic"`.
- `owner_num`: `1` if `owner = "Foreign"`, otherwise `0`.
- `manufacturing`: `1` if the two-digit PKD section is between `10` and `33`, otherwise `0`.

### Real and log variables

- `price_index`: year-specific deflator indexed to `2019 = 1.0`.
  - `2019 = 1.000000000`
  - `2020 = 1.034000000`
  - `2021 = 1.086734000`
  - `2022 = 1.243223696`
  - `2023 = 1.384951196`
  - `2024 = 1.434809439`
- `sales_real`: `sales / price_index`.
  Note: if `price_index = 0`, return `NaN` under the safe-division rule.
- `ln_sales`: `log(sales)`.
  Note: defined only for strictly positive `sales`; otherwise `NaN`.
- `ln_total_assets`: `log(total_assets)`.
  Note: defined only for strictly positive `total_assets`; otherwise `NaN`.
- `ln_employment`: `log(employment)`.
  Note: defined only for strictly positive `employment`; otherwise `NaN`.

### Ratios

- `profit_margin`: `net_profit / sales`.
  Note: if `sales = 0`, return `NaN`.
- `operating_margin`: `operating_result / sales`.
  Note: if `sales = 0`, return `NaN`.
- `export_ratio`: `exports / sales`.
  Note: if `sales = 0`, return `NaN`.
- `asset_turnover`: `sales / total_assets`.
  Note: if `total_assets = 0`, return `NaN`.
- `capital_ratio`: `equity / total_assets`.
  Note: if `total_assets = 0`, return `NaN`.
- `equity_multiplier`: `total_assets / equity`.
  Note: if `equity = 0`, return `NaN`.
- `roa`: `net_profit / total_assets`.
  Note: if `total_assets = 0`, return `NaN`.
- `roe`: `net_profit / equity`.
  Note: if `equity = 0`, return `NaN`.
- `depreciation_ratio`: `depreciation / total_assets`.
  Note: if `total_assets = 0`, return `NaN`.
- `wage_intensity`: `wages_total / sales`.
  Note: if `sales = 0`, return `NaN`.

### Productivity variables

- `sales_per_employee`: `sales / employment`.
  Note: if `employment = 0`, return `NaN`.
- `assets_per_employee`: `total_assets / employment`.
  Note: if `employment = 0`, return `NaN`.

### Growth variables

- `sales_growth_yoy`: `(sales_t / sales_{t-1}) - 1`, computed within firm after sorting by `nip`, `year`.
  Note: if lagged `sales <= 0` or missing, return `NaN`.
- `sales_real_growth_yoy`: `(sales_real_t / sales_real_{t-1}) - 1`, computed within firm after sorting by `nip`, `year`.
  Note: if lagged `sales_real <= 0` or missing, return `NaN`.
- `sales_log_growth_yoy`: `ln_sales_t - ln_sales_{t-1}`, computed within firm after sorting by `nip`, `year`.
  Note: if either log value is missing, return `NaN`.

### Data quality flags

- `has_sales`: `1` if `sales > 0`, otherwise `0`.
- `has_assets`: `1` if `total_assets > 0`, otherwise `0`.
- `has_employment`: `1` if `employment > 0`, otherwise `0`.

## Safe division rule

All ratio-style variables use safe division:

- if the denominator is zero, return `NaN`
- if the denominator is missing, return `NaN`

This rule applies to:

- `sales_real`
- all ratio variables
- productivity variables

## Notes for research use

- Panel coverage: `2019-2024`
- Grain: annual firm-year only
- The notebook contains inspection only and no transformation logic
- The final dataset is sorted by `nip`, `year`
