# Dataset: Data_core_2018-2024
Version: v1.1
Date: 2026-06-27
Changes:
- Initial canonical dataset build
- Extended coverage to 2018 for P1 lag growth
- Aligned the canonical schema with the columns currently available in `Data_panel_2018-2024.parquet`
- Omitted unavailable descriptors rather than fabricating or substituting values

## Purpose

`Data_core_2018-2024.parquet` is the canonical cleaned annual master panel derived from `Data_panel_2018-2024.parquet`.

The 2018 observation is included only to supply sales for the 2018 -> 2019 lag-growth calculation used by P1 models. Main growth outcomes, trajectories, `SGrowth_NR`, and start-of-period covariates continue to begin in 2019.

Unit of observation: one row per firm (`nip`) per year (`year`).

## Source and outputs

- Input: `Data_panel_2018-2024.parquet`
- Production script: `build_core_panel.py`
- Outputs:
  - `Data_core_2018-2024.parquet`
  - `Data_core_2018-2024.xlsx`
- Validation notebook: `check_core_panel.ipynb`

## Build rules

1. Read the annual panel from `Data_panel_2018-2024.parquet`
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
- `manufacturing`
- `owner_type`
- `owner`
- `owner_num`
- `city`
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
- `zobowiazania_i_rezerwy_na_zobowiazania`
- `zobowiazania_dlugoterminowe`
- `zobowiazania_krotkoterminow`
- `liabilities_provisions`
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

Total columns: `57`

## Calculated variables and formulas

### Structural descriptors

- `in_rank_2019`: `1` if `rank_2019` is non-missing, otherwise `0`.
- `owner`: `"Foreign"` if `owner_type` starts with `"5"`, otherwise `"Domestic"`.
- `owner_num`: `1` if `owner = "Foreign"`, otherwise `0`.
- `manufacturing`: `1` if the two-digit PKD section is between `10` and `33`, otherwise `0`.

The current source does not contain `business_start_year`, `gpw`, `incorporation_year_krs`, or `sector`. Consequently, these columns and the derived `sector_en` column are not part of the canonical output. No proxy or replacement variable is used.

### Liability source columns

The five liability variables are preserved exactly as supplied:

- `total_liabilities`
- `liabilities_provisions`
- `zobowiazania_i_rezerwy_na_zobowiazania`
- `zobowiazania_dlugoterminowe`
- `zobowiazania_krotkoterminow`

They are raw annual variables, not calculated substitutes for one another. Their observed year coverage differs in the current source, and missing values remain `NaN`.

### Real and log variables

- `price_index`: year-specific deflator indexed to `2019 = 1.0`.
  - `2018 = 0.977517106549`
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

The 2018 value of `sales` and its derived `sales_real` value provide the 2018 -> 2019 comparison used for P1 lag growth in the period dataset. Other annual financial, employment, and balance-sheet variables may be missing in 2018 and do not cause validation failure.

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

- Panel coverage: `2018-2024`
- Analytical coverage: main outcomes remain `2019-2024`; 2018 is used only for P1 lag growth
- Grain: annual firm-year only
- The notebook contains inspection only and no transformation logic
- The final dataset is sorted by `nip`, `year`
