# Results_period_ols

## Purpose

`run_period_ols.py` estimates the agreed OLS models from `Data_period_2019-2024.parquet` and exports a reporting workbook for cross-period comparison.

The raw-coefficient tables remain the main model outputs. Standardised-variable models and standardised beta tables are added only as supplementary interpretation aids.

## Data source

- Input dataset: `Data_period_2019-2024.parquet`
- Estimation sample filter: `in_rank_2019 == 1 & manufacturing == 1`

Because the filtered sample fixes `manufacturing == 1` for every retained observation, `manufacturing` is removed from the regressors in all models.
The applied sample filter implies that results should be interpreted as within-manufacturing firm dynamics among firms present in the baseline ranking.
Manufacturing is defined from PKD codes 10-33 (Section C), while `sector_en` is the analytical sector variable used to capture heterogeneity within the filtered manufacturing sample.

## Models estimated

- `P1_baseline`
- `P1_winsor`
- `P2_baseline`
- `P2_winsor`
- `P3_baseline`
- `P3_winsor`
- `P1_baseline_std`
- `P1_winsor_std`
- `P2_baseline_std`
- `P2_winsor_std`
- `P3_baseline_std`
- `P3_winsor_std`

The model specifications are identical across raw, winsorised, and standardised variants, ensuring comparability across estimation approaches. The additional `_std` models standardise the dependent variable and numeric regressors on each model's estimation sample while leaving binary regressors unstandardised.

## Dependent variables

Baseline:

- `growth_log_ann_P1`
- `growth_log_ann_P2`
- `growth_log_ann_P3`

Winsorised:

- `growth_log_ann_P1_w`
- `growth_log_ann_P2_w`
- `growth_log_ann_P3_w`

## Independent variables by period

### P1

- `ln_sales_start_P1`
- `profit_margin_start_P1`
- `export_ratio_start_P1`
- `asset_turnover_start_P1`
- `capital_ratio_start_P1`
- `owner_num`
- `sector_en` (categorical)

### P2

- `ln_sales_start_P2`
- `profit_margin_start_P2`
- `export_ratio_start_P2`
- `asset_turnover_start_P2`
- `capital_ratio_start_P2`
- `owner_num`
- `lag_growth_log_ann_P2`
- `sector_en` (categorical)

### P3

- `ln_sales_start_P3`
- `profit_margin_start_P3`
- `export_ratio_start_P3`
- `asset_turnover_start_P3`
- `capital_ratio_start_P3`
- `owner_num`
- `lag_growth_log_ann_P3`
- `sector_en` (categorical)

All models include an intercept.
Ownership is displayed as `Foreign` in user-facing tables, with domestic firms as the reference group.
Sector enters all 12 models through dummy variables based on `sector_en`, with `production` fixed as the omitted reference category.
Ownership reference: `Domestic`.
Sector reference: `production`.

## Winsorisation rule

- winsorise only the dependent variable
- lower bound: 1st percentile
- upper bound: 99th percentile
- independent variables are not winsorised

## Harmonised comparison labels

The comparison sheets use harmonised row labels instead of raw period-specific variable names:

- `ln_sales`
- `profit_margin`
- `export_ratio`
- `asset_turnover`
- `capital_ratio`
- `Foreign`
- `sector: chemicals`
- `sector: mining and metallurgy`
- `sector: automotive`
- `sector: health and pharma`
- `sector: fuels`
- `sector: services`
- `sector: food`
- `lag_growth_log_ann`
- `const`

`production` is the omitted sector reference and is therefore not shown in the comparison sheets.

## Standardised beta reporting

Standardised coefficients are reported in additional comparison sheets only. They are not used for estimation.
Standardised-variable models (denoted by `_std`) are estimated on z-scored dependent and independent variables, whereas standardised beta coefficients are derived from raw models post-estimation.
Standardised models allow direct comparison of effect sizes across variables, while raw models preserve economic interpretability in original units.

They are computed after estimation as:

- `beta_std = beta_raw * std(X) / std(Y)`

For winsor models, `std(Y)` uses the winsorised dependent variable.

## Workbook structure

`Results_period_ols.xlsx` contains:

- `Model_Summary`
- `Coefficients_All`
- `Compare_Baseline`
- `Compare_Winsor`
- `Compare_Baseline_StdBeta`
- `Compare_Winsor_StdBeta`
- `Compare_Baseline_StdModel`
- `Compare_Winsor_StdModel`
- `Diagnostics`
- `Variable_Labels`

`Model_Summary` and `Coefficients_All` now include all 12 models:

- raw models
- winsor models
- standardised-variable models
- standardised-variable winsor models

## Diagnostics additions

The diagnostics sheet includes:

- input row count before filtering
- row count after filtering
- unique firms after filtering
- dependent variable used
- model family
- winsor lower and upper bounds
- number of observations affected by winsorisation
- number of rows dropped due to missing values
- exact regressors used

Sector enters the regressions as a categorical control via `sector_en` dummy variables. The condensed comparison tables remain focused on the core harmonised covariates, while the full sector dummy coefficients are available in `Coefficients_All`.
The sector controls are restricted to sector levels observed within the filtered manufacturing sample, with `production` fixed as the omitted reference category across all model variants.

Sector labels in reporting tables are taken directly from `sector_en`. The canonical label `services` is used in the workbook and the sector dummies capture heterogeneity within the filtered manufacturing sample.

## Notes

This remains the baseline OLS stage. It does not include:

- interactions
- trajectory classes
- nonlinear models
- additional regressors beyond the agreed specification

Raw models remain the main economically interpretable models. Standardised-variable models and standardised beta tables are supplementary interpretation tools.

Comparison tables now:

- use a fixed harmonised variable order across all tabs
- include `N`, `R-squared`, and `Adjusted R-squared`

The model specification is final and frozen apart from presentation-layer reporting updates.
