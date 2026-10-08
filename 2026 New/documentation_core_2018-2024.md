# Dataset: data_core_2018-2024

## Excel analysis formatting

The first worksheet retains its existing name (`Sheet1`) and all 17,731 firm-year rows / 59 canonical columns. It is formatted like the Period data sheet: native Excel Table `CoreData` with filters and blue banded rows, dark-blue wrapped headers with white text, Arial 10, fitted descriptor widths/heights, hidden gridlines and 90% zoom. The header and the three Core identification columns (`nip`, `year`, `company`) are frozen at `D2`.

Ratios and simple year-on-year growth display as percentages with two decimal places. Logs, log growth, asset turnover, equity multipliers and CPI display as decimal values with four places. Original financial amounts, real sales, average FTE employment and per-employee measures use thousands separators and two decimal places; fractional FTEs remain visible. Ranks, years, codes and flags display as integers; text NIPs stay text. These are display formats only; no rounding, clipping, conversion, imputation or new financial variables are stored.

`code_build_core.write_outputs()` automatically applies the formatting to future exports. For an existing workbook, the builder's `format_excel_output(Path('data_core_2018-2024.xlsx'))` applies only presentation changes, preserving stored cell values, sheet name, existing filter conditions and other worksheets. This avoids overwriting researcher Excel edits during a formatting-only request. A normal data rebuild continues to use the builder's authoritative input and does not import Excel edits into Parquet.

The formatting-only refresh on 8 October 2026 preserved every stored cell value across all 17,731 rows and 59 columns, including one pre-existing `sj` difference from canonical Parquet. Original numeric XML tokens are retained to avoid floating-point reserialization changes. That existing difference was neither corrected nor propagated to canonical data. No Parquet, Period workbook or regression/audit output is written by the formatter. During validation the two OLS workbooks were independently updated outside this formatting operation and left untouched; the other 17 protected dataset/workbook hashes remained unchanged. Repeat formatting was also checked on a disposable workbook for retained native filter conditions, text identifiers and an extra reader-notes sheet.
Version: v1.1
Date: 2026-06-27
Changes:
- Initial canonical dataset build
- Extended coverage to 2018 for P1 lag growth
- Aligned the canonical schema with the columns currently available in `data_panel_2018-2024.parquet`
- Read `sector` directly from `data_panel_2018-2024.parquet` and derived `sector_en`
- Filled stable descriptors within firm from the first non-missing 2019-2024 observation

## Purpose

`data_core_2018-2024.parquet` is the canonical cleaned annual master panel derived from `data_panel_2018-2024.parquet`.

The 2018 observation is included only to supply sales for the 2018 -> 2019 lag-growth calculation used by P1 models. Main growth outcomes, trajectories, `SGrowth_NR`, and start-of-period covariates continue to begin in 2019.

Unit of observation: one row per firm (`nip`) per year (`year`).

## Source and outputs

- Input: `data_panel_2018-2024.parquet`
- Production script: `code_build_core.py`
- Outputs:
  - `data_core_2018-2024.parquet`
  - `data_core_2018-2024.xlsx`
- Validation notebook: `notebook_check_core.ipynb`

## Build rules

1. Read the annual panel from `data_panel_2018-2024.parquet`
2. Keep one row per (`nip`, `year`)
3. Keep `rank_2019` and create `in_rank_2019`
4. Drop `rank_2020` to `rank_2024`
5. For rows where canonical `sales` is missing, fill it from source `przychody`; in the current input this supplies all 2,533 observations for 2018 P1 lag growth. Omit `przychody` after this explicit mapping and omit source-only `gpw`.
6. Trim whitespace in string fields and convert empty placeholders to missing values
7. Coerce identifier and financial columns to consistent numeric types
8. Remove rows with missing `nip` or `year`
9. Remove duplicate firm-year rows if present:
   - keep the row with the highest number of non-missing values
   - break remaining ties by sorted order
10. Sort the final panel by `nip`, `year`

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

Total columns: `59`

## Calculated variables and formulas

### Structural descriptors

- `in_rank_2019`: `1` if `rank_2019` is non-missing, otherwise `0`.
- `sector`: retained as supplied in `data_panel_2018-2024.parquet`; missing values remain missing.
- `sector_en`: translated from `sector` using the fixed mapping in `code_build_core.py`.
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
  - if `sector` is missing or unmatched, `sector_en` remains missing and the build summary reports it
- `owner`: `"Foreign"` if `owner_type` starts with `"5"`, otherwise `"Domestic"`.
- `owner_num`: `1` if `owner = "Foreign"`, otherwise `0`.
- `manufacturing`: `1` if the two-digit PKD section is between `10` and `33`, otherwise `0`.

The unavailable `business_start_year` and `incorporation_year_krs` columns remain omitted. The canonical raw-financial columns `income_tax`, `zobowiazania_dlugoterminowe`, and `zobowiazania_krotkoterminow` are absent from the current source; they are retained in the canonical schema and filled with `NaN`, with their unavailability reported by the build.

### Liability source columns

The five liability variables remain in the canonical schema:

- `total_liabilities`
- `liabilities_provisions`
- `zobowiazania_i_rezerwy_na_zobowiazania`
- `zobowiazania_dlugoterminowe`
- `zobowiazania_krotkoterminow`

They are raw annual variables, not calculated substitutes for one another. `zobowiazania_dlugoterminowe` and `zobowiazania_krotkoterminow` are unavailable in the current source and therefore remain `NaN`; no substitute is constructed. The other liability variables are preserved exactly as supplied.

`income_tax` is also unavailable in the current source and remains `NaN`; it is not replaced with zero or inferred from another variable.

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
