# Results_period_ols_scenarios

## Purpose

`run_period_ols_scenarios.py` is the current master OLS regression pipeline for the 2026 period analysis.

It estimates scenario-based OLS models from `Data_period_2018-2024.parquet` and exports:

- `Results_period_ols_scenarios.xlsx`

Current script roles:

- Main OLS scenario runner: `run_period_ols_scenarios.py`
- Descriptive trajectory runner: `run_trajectory_analysis.py`
- Shared analytical definitions: `analysis_config.py`

The older single-sample workbook path is no longer the primary documented workflow. Current interpretation should use the scenario workbook.

## Input

- `Data_period_2018-2024.parquet`

The 2018 data is used only for the 2018-2019 lag-growth variable included in P1 models. Main dependent variables, trajectories, `SGrowth_NR`, and FULL-period growth remain based on 2019-2024.

The script applies the trajectory-completeness filter implied by `growth_mode`, then applies the scenario filter.

For nominal mode:

- `has_complete_ntrajectory == 1`

For real mode:

- `has_complete_rtrajectory == 1`

## Scenarios

The current scenario definitions are:

- `ALL`: no additional scenario filter
- `RANK2019`: `in_rank_2019 == 1`
- `MANUFACTURING`: `manufacturing == 1`
- `RANK2019_MANUFACTURING`: `in_rank_2019 == 1 & manufacturing == 1`

Each scenario runs the same model grid.

## Periods

Each scenario estimates models for:

- `P1`
- `P2`
- `P3`
- `FULL`

Period dependent variables are annualised log growth variables.

The current shared setting is `growth_mode = "nominal"`: the dependent variable is **nominal annualised log sales growth**, calculated from current-price `sales`, not inflation-adjusted `sales_real` and not the log sales level.

| Period | Current nominal source column | Formula |
| --- | --- | --- |
| P1 | `ngrowth_log_ann_P1` | `(ln(sales_2020) - ln(sales_2019)) / 1` |
| P2 | `ngrowth_log_ann_P2` | `(ln(sales_2022) - ln(sales_2020)) / 2` |
| P3 | `ngrowth_log_ann_P3` | `(ln(sales_2024) - ln(sales_2022)) / 2` |
| FULL | `ngrowth_log_ann_2019_2024` | `(ln(sales_2024) - ln(sales_2019)) / 5` |

Switching the shared setting to `real` selects the corresponding `rgrowth_log_ann_*` columns and substitutes inflation-adjusted `sales_real` in these formulas. Endpoint sales must be strictly positive; missing or non-positive endpoints yield missing growth. The measures are in log points per year, not CAGR. Model variants apply the documented winsorisation and standardisation to these source variables.

The workbook README states the price basis, exact source column, interval, and formula for every period. `Compare_Main` and `Compare_Raw` show `Dependent variable` and `Dependent variable column` rows at the start of each scenario block. These descriptions are generated from the configured growth mode and update automatically when it changes. Description rows do not alter the regression specification or estimates.

`FULL` is the 2019-2024 full-period model. It uses:

- `rgrowth_log_ann_2019_2024` when `growth_mode == "real"`
- `ngrowth_log_ann_2019_2024` when `growth_mode == "nominal"`

`FULL` uses P1 starting covariates, such as `ln_sales_start_P1` and `export_ratio_start_P1`.

`FULL` does not include lag growth.

## Model Variants

Each scenario-period combination runs:

- `baseline`
- `baseline_std`
- `winsor`
- `winsor_std`

With 4 scenarios, 4 periods, and 4 variants, the expected total is 64 estimated models unless a model is skipped for a valid diagnostic reason.

## Current Specification

Current base regressors:

- `ln_sales`
- `profit_margin`
- `export_ratio`
- `asset_turnover`
- `capital_ratio`
- `sales_per_employee`

The default source-column rule is:

- `{base_name}_start_{period}`

For `FULL`, the regressor period is resolved to `P1`.

Owner control:

- raw variable: `owner_num`
- displayed as: `Foreign`

Categorical controls:

- `sector_en` is required and included in every regression
- reference category: `production`
- scripts fail clearly if the column or reference category is missing

Interaction:

- `export_ratio x ln_sales`
- generated as `export_ratio_x_ln_sales_start_P1`, `export_ratio_x_ln_sales_start_P2`, and `export_ratio_x_ln_sales_start_P3`
- `P1` and `FULL` both use the same raw interaction column: `export_ratio_x_ln_sales_start_P1`
- for `FULL`, the interaction is calculated from `export_ratio_start_P1 * ln_sales_start_P1`

Lag growth:

- included for configured lag periods `P1`, `P2`, and `P3`
- P1 uses `lag_*growth_log_ann_P1`, calculated from 2018-2019
- P2 and P3 retain their existing prior-period lag variables
- not included for `FULL`

## Winsorisation

Winsorisation applies only to the dependent variable.

Current settings:

- lower bound: 1st percentile
- upper bound: 99th percentile

Independent variables are not winsorised.

Winsorised dependent variables are created inside the estimation workflow and are not required as input columns.

## Standardisation

Standardised model variants standardise numeric regressors within each model estimation sample.

Under the current configuration:

- `standardised_models == True`
- `standardise_dependent == True`

Categorical dummies, ownership dummy, and intercept are excluded from standardisation unless explicitly marked otherwise in the registry.

## Workbook Structure

`Results_period_ols_scenarios.xlsx` contains:

- `README`
- `Compare_Main`
- `Compare_Raw`
- `Model_Summary_Long`
- `AUDIT_AND_TECHNICAL_TABS`
- `Diagnostics_Long`
- `Dropped_Rows_Long`
- `Coefficients_Long`
- `Variable_Labels`
- `Run_Log`

Working tabs come first. Audit and technical tabs follow after `AUDIT_AND_TECHNICAL_TABS`.

The workbook is formatted for research use:

- headers are bold and coloured
- top rows are frozen
- comparison sheets freeze the first two columns
- filters are applied to data sheets
- tabs are colour coded by purpose

## Human-Facing Tabs

### README

Compact explanation of the analysis name, input file, output file, scenarios, periods, dependent-variable logic, model variants, winsorisation, standardisation, and significance stars.

### Compare_Main

Reader-facing comparison table for standardised models.

Columns:

- `scenario`
- `display_name`
- `P1_StdBaseline`
- `P2_StdBaseline`
- `P3_StdBaseline`
- `FULL_StdBaseline`
- `P1_StdWinsor`
- `P2_StdWinsor`
- `P3_StdWinsor`
- `FULL_StdWinsor`

Rows use display labels from the variable registry.

### Compare_Raw

Reader-facing comparison table for raw models.

Columns:

- `scenario`
- `display_name`
- `P1_Baseline`
- `P2_Baseline`
- `P3_Baseline`
- `FULL_Baseline`
- `P1_Winsor`
- `P2_Winsor`
- `P3_Winsor`
- `FULL_Winsor`

Rows use display labels from the variable registry.

Pre-modelling descriptive, missingness, winsorisation, trajectory, and correlation
diagnostics are reported separately in
`Results_variable_diagnostics_and_trajectories.xlsx`. This keeps regression
results separate from pre-modelling diagnostics while preserving the same shared
scenario definitions and exact model-variable logic.

`run_period_ols_scenarios.py` writes only the regression workbook.
`Results_variable_diagnostics_and_trajectories.xlsx` is generated independently
by `run_trajectory_analysis.py` using the shared definitions in `analysis_config.py`.

### Model_Summary_Long

Long model summary table. It retains scenario, period, model, model family, dependent-variable fields, sample filter, observation counts, lag inclusion, and fit statistics.

It includes `dependent_variable_label` for readability while retaining the raw dependent-variable name.

## Audit And Technical Tabs

### AUDIT_AND_TECHNICAL_TABS

Navigation separator. Tabs after this point contain technical diagnostics, dropped-row audit trails, full coefficient outputs, variable dictionaries, and run logs.

### Diagnostics_Long

Technical diagnostics by model.

It includes raw generated dependent variables and regressors, plus label columns such as:

- `dependent_variable_label`
- `generated_regressor_labels_for_period`
- `x_variable_labels_used`

### Dropped_Rows_Long

Consolidated audit trail for rows dropped from model estimation because of missing dependent, regressor, or categorical-control values.

Columns include:

- `scenario`
- `model`
- `period`
- `model_family`
- `winsorised`
- `standardised_model`
- `nip`
- `company`
- `dependent_variable`
- `dependent_variable_label`
- `missing_columns`
- `missing_column_labels`
- `missing_values_count`
- `row_index`
- `reason`

### Coefficients_Long

Full technical coefficient output.

It retains:

- `scenario`
- `model`
- `period`
- `model_family`
- `growth_mode`
- `dependent_variable`
- `winsorised`
- `standardised_model`
- `sample_filter`
- `raw_variable`
- `display_name`
- `variable_type`
- coefficient, standard error, t-statistic, p-value, and confidence interval bounds

### Variable_Labels

Consolidated variable dictionary generated from the internal variable registry.

It includes:

- `raw_name`
- `display_name`
- `variable_type`
- `standardise`
- `interpretation`

Dependent variables are included for both real and nominal growth families:

- `rgrowth_log_ann_P1`
- `rgrowth_log_ann_P2`
- `rgrowth_log_ann_P3`
- `rgrowth_log_ann_2019_2024`
- `ngrowth_log_ann_P1`
- `ngrowth_log_ann_P2`
- `ngrowth_log_ann_P3`
- `ngrowth_log_ann_2019_2024`

### Run_Log

Scenario and model execution log. It records successful model estimation and skipped models, with warning messages where applicable.

## Workbook Philosophy

Human-facing tabs use `display_name` or `variable_label` as the main label.

Raw variable names are retained in technical tabs for reproducibility.

The workbook is designed so that interpretation can start from `README`, `Compare_Main`, `Compare_Raw`, `Descriptive_Stats`, and `Correlation_Long`, while auditability is preserved in the technical tabs.

## Last Updated For

- Script: `run_period_ols_scenarios.py`
- Output file: `Results_period_ols_scenarios.xlsx`
- Main change covered: interaction renamed to `export_ratio_x_ln_sales`; interaction columns now follow starting-point regressor periods, with P1 and FULL sharing `export_ratio_x_ln_sales_start_P1`
- Date: 2026-07-01
