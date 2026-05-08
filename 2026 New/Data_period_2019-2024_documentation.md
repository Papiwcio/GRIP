# Data_period_2019-2024

## Purpose

`Data_period_2019-2024.parquet` is the canonical firm-level period dataset derived from `Data_core_2019-2024.parquet`.

It is a single master dataset with two parallel measurement layers:

- real layer (`r...`) built from inflation-adjusted `sales_real`
- nominal layer (`n...`) built from raw `sales`

No separate real-only or nominal-only dataset is created.

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

### Real growth outcomes

- `rgrowth_P1`
- `rgrowth_P2`
- `rgrowth_P3`
- `rgrowth_log_P1`
- `rgrowth_log_P2`
- `rgrowth_log_P3`
- `rgrowth_log_ann_P1`
- `rgrowth_log_ann_P2`
- `rgrowth_log_ann_P3`

### Real lagged outcomes

- `lag_rgrowth_P2`
- `lag_rgrowth_P3`
- `lag_rgrowth_log_P2`
- `lag_rgrowth_log_P3`
- `lag_rgrowth_log_ann_P2`
- `lag_rgrowth_log_ann_P3`

### Nominal growth outcomes

- `ngrowth_P1`
- `ngrowth_P2`
- `ngrowth_P3`
- `ngrowth_log_P1`
- `ngrowth_log_P2`
- `ngrowth_log_P3`
- `ngrowth_log_ann_P1`
- `ngrowth_log_ann_P2`
- `ngrowth_log_ann_P3`

### Nominal lagged outcomes

- `lag_ngrowth_P2`
- `lag_ngrowth_P3`
- `lag_ngrowth_log_P2`
- `lag_ngrowth_log_P3`
- `lag_ngrowth_log_ann_P2`
- `lag_ngrowth_log_ann_P3`

### Start-of-period covariates

For each period, covariates are measured at the exact start year:

- `*_start_P1` from `2019`
- `*_start_P2` from `2020`
- `*_start_P3` from `2022`

Each block contains:

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

### Availability flags

Real layer:

- `has_rP1_data`
- `has_rP2_data`
- `has_rP3_data`

Nominal layer:

- `has_nP1_data`
- `has_nP2_data`
- `has_nP3_data`

### Real trajectory descriptors

- `has_complete_rtrajectory`
- `rP1_sign`
- `rP2_sign`
- `rP3_sign`
- `rtrajectory_3step`
- `rtrajectory_label`
- `rtrajectory_group`
- `rindex_2019`
- `rindex_2020`
- `rindex_2022`
- `rindex_2024`

### Nominal trajectory descriptors

- `has_complete_ntrajectory`
- `nP1_sign`
- `nP2_sign`
- `nP3_sign`
- `ntrajectory_3step`
- `ntrajectory_label`
- `ntrajectory_group`
- `nindex_2019`
- `nindex_2020`
- `nindex_2022`
- `nindex_2024`

### SGrowth_NR and diagnostics

- `SGrowth_NR`
- `rgrowth_ann_2019_2024`
- `rgrowth_log_ann_2019_2024`
- `ngrowth_ann_2019_2024`
- `ngrowth_log_ann_2019_2024`

The log annualised 2019-2024 variables are also the FULL-period dependent-variable candidates used by the scenario OLS pipeline.

### Quantile-based performance classifications

- `RPerf_Q_all`
- `RPerf_Q_rank2019`
- `RPerf_Q_manu2019`
- `NPerf_Q_all`
- `NPerf_Q_rank2019`
- `NPerf_Q_manu2019`

## Calculated variables and formulas

### Real period growth

- `rgrowth_P1 = sales_real_2020 / sales_real_2019 - 1`
- `rgrowth_P2 = sales_real_2022 / sales_real_2020 - 1`
- `rgrowth_P3 = sales_real_2024 / sales_real_2022 - 1`

Validity rule:

- start and end values must both be present and strictly positive
- otherwise return `NaN`

### Nominal period growth

- `ngrowth_P1 = sales_2020 / sales_2019 - 1`
- `ngrowth_P2 = sales_2022 / sales_2020 - 1`
- `ngrowth_P3 = sales_2024 / sales_2022 - 1`

Validity rule:

- start and end values must both be present and strictly positive
- otherwise return `NaN`

### Real log growth

- `rgrowth_log_P1 = ln(sales_real_2020) - ln(sales_real_2019)`
- `rgrowth_log_P2 = ln(sales_real_2022) - ln(sales_real_2020)`
- `rgrowth_log_P3 = ln(sales_real_2024) - ln(sales_real_2022)`

Rule:

- natural logarithm is used
- start and end must both be present and strictly positive
- otherwise return `NaN`

### Nominal log growth

- `ngrowth_log_P1 = ln(sales_2020) - ln(sales_2019)`
- `ngrowth_log_P2 = ln(sales_2022) - ln(sales_2020)`
- `ngrowth_log_P3 = ln(sales_2024) - ln(sales_2022)`

Rule:

- natural logarithm is used
- start and end must both be present and strictly positive
- otherwise return `NaN`

### Real annualised log growth

- `rgrowth_log_ann_P1 = rgrowth_log_P1 / 1`
- `rgrowth_log_ann_P2 = rgrowth_log_P2 / 2`
- `rgrowth_log_ann_P3 = rgrowth_log_P3 / 2`

### Nominal annualised log growth

- `ngrowth_log_ann_P1 = ngrowth_log_P1 / 1`
- `ngrowth_log_ann_P2 = ngrowth_log_P2 / 2`
- `ngrowth_log_ann_P3 = ngrowth_log_P3 / 2`

These annualised log variables are the regression-ready dependent-variable families for periods of unequal length.

### Real lag variables

- `lag_rgrowth_P2 = rgrowth_P1`
- `lag_rgrowth_P3 = rgrowth_P2`
- `lag_rgrowth_log_P2 = rgrowth_log_P1`
- `lag_rgrowth_log_P3 = rgrowth_log_P2`
- `lag_rgrowth_log_ann_P2 = rgrowth_log_ann_P1`
- `lag_rgrowth_log_ann_P3 = rgrowth_log_ann_P2`

### Nominal lag variables

- `lag_ngrowth_P2 = ngrowth_P1`
- `lag_ngrowth_P3 = ngrowth_P2`
- `lag_ngrowth_log_P2 = ngrowth_log_P1`
- `lag_ngrowth_log_P3 = ngrowth_log_P2`
- `lag_ngrowth_log_ann_P2 = ngrowth_log_ann_P1`
- `lag_ngrowth_log_ann_P3 = ngrowth_log_ann_P2`

No additional lag structures are included.

### Availability flags

Real layer:

- `has_rP1_data = 1` if `sales_real_2019` and `sales_real_2020` are both present and `> 0`, else `0`
- `has_rP2_data = 1` if `sales_real_2020` and `sales_real_2022` are both present and `> 0`, else `0`
- `has_rP3_data = 1` if `sales_real_2022` and `sales_real_2024` are both present and `> 0`, else `0`

Nominal layer:

- `has_nP1_data = 1` if `sales_2019` and `sales_2020` are both present and `> 0`, else `0`
- `has_nP2_data = 1` if `sales_2020` and `sales_2022` are both present and `> 0`, else `0`
- `has_nP3_data = 1` if `sales_2022` and `sales_2024` are both present and `> 0`, else `0`

### Real trajectory descriptors

- `has_complete_rtrajectory = 1` if `rgrowth_P1`, `rgrowth_P2`, and `rgrowth_P3` are all non-missing, else `0`
- `rP1_sign`, `rP2_sign`, `rP3_sign`: `D` if the corresponding `rgrowth` is `< 0`, `G` if it is `>= 0`, else missing
- `rtrajectory_3step`: `rP1_sign-rP2_sign-rP3_sign` when all three signs are observed, else missing
- `rtrajectory_label`: descriptive label mapped from `rtrajectory_3step` using `TRAJECTORY_LABEL_MAP`; missing when `rtrajectory_3step` is missing
- `rtrajectory_group`: mapped from `rtrajectory_3step` using the shared `TRAJECTORY_GROUP_MAP`

Real index logic:

- `rindex_2019 = 100` if `has_complete_rtrajectory = 1`, else `NaN`
- `rindex_2020 = rindex_2019 * (1 + rgrowth_P1)`
- `rindex_2022 = rindex_2020 * (1 + rgrowth_P2)`
- `rindex_2024 = rindex_2022 * (1 + rgrowth_P3)`

### Nominal trajectory descriptors

- `has_complete_ntrajectory = 1` if `ngrowth_P1`, `ngrowth_P2`, and `ngrowth_P3` are all non-missing, else `0`
- `nP1_sign`, `nP2_sign`, `nP3_sign`: `D` if the corresponding `ngrowth` is `< 0`, `G` if it is `>= 0`, else missing
- `ntrajectory_3step`: `nP1_sign-nP2_sign-nP3_sign` when all three signs are observed, else missing
- `ntrajectory_label`: descriptive label mapped from `ntrajectory_3step` using `TRAJECTORY_LABEL_MAP`; missing when `ntrajectory_3step` is missing
- `ntrajectory_group`: mapped from `ntrajectory_3step` using the shared `TRAJECTORY_GROUP_MAP`

Nominal index logic:

- `nindex_2019 = 100` if `has_complete_ntrajectory = 1`, else `NaN`
- `nindex_2020 = nindex_2019 * (1 + ngrowth_P1)`
- `nindex_2022 = nindex_2020 * (1 + ngrowth_P2)`
- `nindex_2024 = nindex_2022 * (1 + ngrowth_P3)`

### Shared descriptive trajectory-label mapping

The same descriptive label mapping is applied to real and nominal trajectories:

- `D-D-D -> Persistent decline`
- `D-G-G -> Recovery`
- `D-D-G -> Late recovery`
- `G-G-G -> Consistent growth`
- `G-D-D -> Deterioration`
- `D-G-D -> Instability`
- `G-D-G -> Volatile growth`
- `G-G-D -> Late deterioration`

### Shared trajectory-group mapping

The same mapping is applied to real and nominal trajectories:

- `D-D-D -> Persistent decline`
- `D-G-G -> Reversal`
- `D-D-G -> Reversal`
- `G-G-G -> Consistent growth`
- `G-D-D -> Reversal`
- `D-G-D -> Reversal`
- `G-D-G -> Reversal`
- `G-G-D -> Reversal`

### SGrowth_NR

`SGrowth_NR` keeps the existing logic and uses both real and nominal annualised growth over `2019 -> 2024`.

Diagnostic columns:

- `rgrowth_ann_2019_2024 = (sales_real_2024 / sales_real_2019)^(1/5) - 1`
- `rgrowth_log_ann_2019_2024 = (ln(sales_real_2024) - ln(sales_real_2019)) / 5`
- `ngrowth_ann_2019_2024 = (sales_2024 / sales_2019)^(1/5) - 1`
- `ngrowth_log_ann_2019_2024 = (ln(sales_2024) - ln(sales_2019)) / 5`

Validity rule:

- both nominal sales and real sales must be positive in `2019` and `2024`
- otherwise `SGrowth_NR` is missing
- log annualised diagnostic variables return `NaN` when their own start or end sales are missing, zero, or negative

Classification:

- `R2` if `rgrowth_ann_2019_2024 >= 0` and `ngrowth_ann_2019_2024 > 0.20`
- `R1` if `rgrowth_ann_2019_2024 >= 0` and `ngrowth_ann_2019_2024 <= 0.20`
- `N1` if `rgrowth_ann_2019_2024 < 0` and `ngrowth_ann_2019_2024 >= 0`
- `N2` if `ngrowth_ann_2019_2024 < 0`
- otherwise missing

### Quantile-based performance classifications

These variables are descriptive only. They are not regression variables at this stage.

They classify firms by relative position in the `2019 -> 2024` annualised growth distribution.

- `RPerf_Q_*` uses `rgrowth_ann_2019_2024`
- `NPerf_Q_*` uses `ngrowth_ann_2019_2024`

Validity rule:

- `sales_2019 > 0`
- `sales_2024 > 0`
- `sales_real_2019 > 0`
- `sales_real_2024 > 0`

If this rule is not satisfied, all six performance classification variables are missing.

Benchmark populations:

- `*_all`: all valid firms
- `*_rank2019`: valid firms with `in_rank_2019 == 1`
- `*_manu2019`: valid firms with `in_rank_2019 == 1` and `manufacturing == 1`

Threshold construction:

- `p10` and `p90` are computed once on the fixed benchmark population for each column
- thresholds do not change later when the dataset is filtered in Excel, pivot tables, or regression scripts
- threshold values (`p10` and `p90`) are exported to the Excel output in a separate sheet (`Performance_Thresholds`) for transparency and reproducibility

Classification rules:

| Label | Rule |
| --- | --- |
| Bottom 10% | growth <= p10 |
| Moderate decline | p10 < growth < 0 |
| Moderate growth | 0 <= growth < p90 |
| Top 10% | growth >= p90 |

Interpretation:

- `RPerf_Q_all`: real performance relative to all valid firms
- `RPerf_Q_rank2019`: real performance relative to firms in the 2019 ranking
- `RPerf_Q_manu2019`: real performance relative to manufacturing firms in the 2019 ranking
- `NPerf_Q_all`, `NPerf_Q_rank2019`, and `NPerf_Q_manu2019`: nominal analogues that are mainly useful for descriptive robustness and inflation comparison

Methodological note:

The same firm may receive different labels across `*_all`, `*_rank2019`, and `*_manu2019` because each column uses a different benchmark population.

Example:

- a firm can be `Top 10%` within manufacturing but only `Moderate growth` relative to all firms

This is expected and intentional.

## Start-of-period covariate logic

Covariates are always taken from the exact first year of the relevant period:

- `P1` covariates from `2019`
- `P2` covariates from `2020`
- `P3` covariates from `2022`

This preserves temporal ordering between covariates and subsequent period outcomes.

## Stable descriptor logic

The following firm-level descriptors are collapsed to one row per `nip` by taking the first non-missing observed value within the annual panel:

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

If optional columns such as `gpw`, `city`, or `legal_form` are absent in the source, they are skipped gracefully.

## Missing-data and denominator handling

- If a start or end value is missing, the related growth variable is `NaN`.
- If a start or end value is non-positive, ordinary growth, log growth, annualised log growth, and availability for that period are treated as invalid and return `NaN` or `0` as applicable.
- No missing values are silently replaced with zero.
- No denominators equal to zero are used in growth calculation; such cases are invalid and return `NaN`.
- Start-of-period covariates remain missing when the annual source value is missing.

## Regression note

Regression scripts must explicitly choose the real or nominal dependent-variable family.

- real models should use `rgrowth_log_ann_P*` and `lag_rgrowth_log_ann_P*`
- nominal models should use `ngrowth_log_ann_P*` and `lag_ngrowth_log_ann_P*`
- FULL-period real models use `rgrowth_log_ann_2019_2024`
- FULL-period nominal models use `ngrowth_log_ann_2019_2024`
- FULL-period models use P1 start-of-period covariates and do not use lag growth
- avoid mixing real dependent variables with nominal lag variables unless that cross-design is intentional and explicitly tested

## Build notes

- the only source file is `Data_core_2019-2024.parquet`
- the output contains exactly one row per firm
- real and nominal growth systems are built in one deterministic master pipeline
- trajectory descriptors are descriptive outputs and not regression results
- no regressions or model outputs are included in this dataset

## Last Updated For

- Script: `build_period_dataset.py`
- Output file: `Data_period_2019-2024.parquet` and `Data_period_2019-2024.xlsx`
- Main change covered: real/nominal period schema, SGrowth_NR diagnostics, 2019-2024 FULL annualised log-growth variables, start covariate logic, and regression handoff notes
- Date: 2026-05-08
