# Data_period_2019-2024

## Purpose

`Data_period_2019-2024.parquet` is the final firm-level period dataset derived from the canonical annual master file `Data_core_2019-2024.parquet`.

It is designed for the next modelling stage and contains only the variables required for the agreed three-period resilience analysis.

## Unit of observation

One row per firm (`nip`).

## Period definitions

- `P1 = 2019 -> 2020`
- `P2 = 2020 -> 2022`
- `P3 = 2022 -> 2024`

## Variable blocks

### Identifiers

- `nip`
- `company`
- `rank_2019`
- `in_rank_2019`

### Stable descriptors

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
- `legal_form`

### Period outcomes

- `growth_real_P1`
- `growth_real_P2`
- `growth_real_P3`
- `growth_log_P1`
- `growth_log_P2`
- `growth_log_P3`
- `growth_log_ann_P1`
- `growth_log_ann_P2`
- `growth_log_ann_P3`

### Lagged outcomes

- `lag_growth_real_P2`
- `lag_growth_real_P3`
- `lag_growth_log_P2`
- `lag_growth_log_P3`
- `lag_growth_log_ann_P2`
- `lag_growth_log_ann_P3`

### Start-of-period covariates

For each period, covariates are measured at the exact start year of that period:

- `*_start_P1` from year `2019`
- `*_start_P2` from year `2020`
- `*_start_P3` from year `2022`

Each start-of-period block contains:

- `ln_sales`
- `ln_total_assets`
- `ln_employment`
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
- `sales_per_employee`
- `assets_per_employee`

### Period availability flags

- `has_P1_data`
- `has_P2_data`
- `has_P3_data`

### Trajectory descriptors

- `has_complete_trajectory`
- `P1_sign`
- `P2_sign`
- `P3_sign`
- `trajectory_3step`
- `trajectory_group`
- `index_2019`
- `index_2020`
- `index_2022`
- `index_2024`

## Calculated variables and formulas

### Period outcomes: real growth

- `growth_real_P1 = sales_real_2020 / sales_real_2019 - 1`
- `growth_real_P2 = sales_real_2022 / sales_real_2020 - 1`
- `growth_real_P3 = sales_real_2024 / sales_real_2022 - 1`

Rule:

- if the start value is missing or equal to zero, return `NaN`

### Period outcomes: log growth

- `growth_log_P1 = ln(sales_real_2020) - ln(sales_real_2019)`
- `growth_log_P2 = ln(sales_real_2022) - ln(sales_real_2020)`
- `growth_log_P3 = ln(sales_real_2024) - ln(sales_real_2022)`

Rule:

- use natural log
- if either value is missing or non-positive, return `NaN`

### Period outcomes: annualised log growth

- `growth_log_ann_P1 = growth_log_P1 / 1`
- `growth_log_ann_P2 = growth_log_P2 / 2`
- `growth_log_ann_P3 = growth_log_P3 / 2`

These are the preferred regression-ready dependent variables.

### Lagged growth variables

- `lag_growth_real_P2 = growth_real_P1`
- `lag_growth_real_P3 = growth_real_P2`
- `lag_growth_log_P2 = growth_log_P1`
- `lag_growth_log_P3 = growth_log_P2`
- `lag_growth_log_ann_P2 = growth_log_ann_P1`
- `lag_growth_log_ann_P3 = growth_log_ann_P2`

No other lag structure is included.

### Period availability flags

- `has_P1_data = 1` if `sales_real_2019` and `sales_real_2020` both exist and `sales_real_2019 > 0`, else `0`
- `has_P2_data = 1` if `sales_real_2020` and `sales_real_2022` both exist and `sales_real_2020 > 0`, else `0`
- `has_P3_data = 1` if `sales_real_2022` and `sales_real_2024` both exist and `sales_real_2022 > 0`, else `0`

### Trajectory descriptors

- `has_complete_trajectory = 1` if `growth_real_P1`, `growth_real_P2`, and `growth_real_P3` are all non-missing, else `0`.
- `P1_sign`, `P2_sign`, `P3_sign`: `D` if the corresponding real growth is `< 0`, `G` if it is `>= 0`. If the growth value is missing, the sign is missing.
- `trajectory_3step`: concatenated three-step sign pattern `P1_sign-P2_sign-P3_sign`. If any sign is missing, `trajectory_3step` is missing.
- `trajectory_group`: broader descriptive grouping from `trajectory_3step`:
  - `D-D-D` -> `Persistent decline`
  - `D-G-G` -> `Recovery`
  - `D-D-G` -> `Recovery`
  - `G-G-G` -> `Consistent growth`
  - `G-D-D` -> `Delayed decline`
  - `D-G-D` -> `Unstable / reversal`
  - `G-D-G` -> `Unstable / reversal`
  - `G-G-D` -> `Unstable / reversal`
- `index_2019 = 100`
- `index_2020 = 100 * (1 + growth_real_P1)`
- `index_2022 = index_2020 * (1 + growth_real_P2)`
- `index_2024 = index_2022 * (1 + growth_real_P3)`

These index variables are descriptive constructed indicators only. They are not raw observed annual sales values and are intended for descriptive interpretation and visualisation.

## Start-of-period covariate logic

Covariates are always taken from the exact first year of the relevant period:

- `P1` covariates from `2019`
- `P2` covariates from `2020`
- `P3` covariates from `2022`

This means the covariates are measured before the period outcome is realized.

## Stable descriptor logic

The following firm-level descriptors are collapsed to one row per firm by taking the first non-missing observed value within `nip`:

- `company`
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
- `legal_form`

If optional columns such as `gpw`, `city`, or `legal_form` are unavailable, they are skipped gracefully.

## Sector variables

- `sector` is the original source business classification label inherited from `Data_core_2019-2024.parquet`.
- `sector_en` is the canonical English translation inherited from the annual core file and is the analytical sector variable used downstream.
- If `sector` is retained in the period dataset, it is kept only for traceability; downstream modelling uses `sector_en`.

## Missing-data handling

- zero or missing start values produce `NaN` in real-growth variables
- non-positive real sales values prevent log-growth calculation
- missing inputs in start-of-period covariates remain missing in the firm-level output

## Build notes

- the only source file is `Data_core_2019-2024.parquet`
- the output contains exactly one row per firm
- deterministic trajectory descriptors are included for descriptive analysis and later trajectory-based modelling
- no regressions or model outputs are included
