# OLS primary and interaction reports

The primary report below is unchanged. The interaction report's **current** presentation is the four-sheet compact supplement documented in `documentation_ols_interactions.md`: separate export × size and profitability × manufacturing models, standardised variants only. The full interaction layout described below is retained as an explicit legacy capability (`run_ols_reports(interaction_layout='full')`), not the default active workbook. Use `python code_ols_interactions.py` to refresh the supplement without touching the primary report.

## Purpose

`code_ols_scenarios.py` is the current master OLS regression pipeline for the 2026 period analysis.

It estimates scenario-based OLS models from `data_period_2018-2024.parquet` and exports:

- `results_ols_scenarios.xlsx`
- `results_ols_interactions.xlsx`

The first workbook contains the **primary additive specification with no interaction terms**. The second contains the **extended specification with the active terms in `INTERACTION_METADATA`**, using the agreed estimation-sample mean-centring. Both contain the full 64-model grid and use the same underlying regression engine, sample definitions, controls, covariance estimator and outcome/predictor preprocessing.

Run `python code_ols_scenarios.py` to produce primary OLS and the compact supplement. `run_ols_reports()` retains the legacy paired A/primary calculations, verifies identical ordered firm IDs and transformed outcomes, and now publishes the compact supplement by default; the new B comparison is separately refitted with Manufacturing retained. A mismatched pair raises an error. No additional data-quality exclusions or source changes are introduced. Supplementary-only updates run `code_ols_interactions.py` and do not rewrite primary OLS.

The lower-level `run_period_ols_scenarios()` defaults to the primary additive report; explicitly set `include_interactions=True` and a separate output path when requesting the extended specification alone. Generic engine helpers keep their previous active-metadata default for imports by quantile, severe and trajectory diagnostics. Shared `code_config.py` is unchanged; those analyses are not rerun or modified by the report split.

Current script roles:

- Main OLS scenario runner: `code_ols_scenarios.py`
- Descriptive trajectory runner: `code_diagnostics_trajectories.py`
- Shared analytical definitions: `code_config.py`

The older single-sample workbook path is no longer the primary documented workflow. Current interpretation should use the scenario workbook.

## Input

- `data_period_2018-2024.parquet`

The 2018 data is used only for the 2018-2019 lag-growth variable included in P1 models. Main dependent variables, trajectories, `SGrowth_NR`, and FULL-period growth remain based on 2019-2024.

The script first applies `code_config.MANUAL_EXCLUSIONS` by verified NIP or exact company name (outer whitespace ignored), then applies trajectory completeness and scenario filters. The excluded firms are Orlen SA GK, Płock; Ignitis Polska sp. z o.o., Warszawa; Elektrobudowa SA w upadłości likwidacyjnej GK, Katowice; Zakłady Mięsne Henryk Kania SA w upadłości; Globus sp. z o.o., Warszawa; and Ordipol sp. z o.o. (w upadłości), Bielany Wrocławskie. All period/model variants use the exclusions before complete cases, winsorisation and scaling. The README lists names, NIPs, reason codes/descriptions, matched counts and removed/already-absent status. Canonical datasets are retained. See `documentation_manual_exclusions.md` for verification and current sample sizes.

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

Interaction (extended workbook only):

- `export_ratio x ln_sales`
- generated as `export_ratio_x_ln_sales_start_P1`, `export_ratio_x_ln_sales_start_P2`, and `export_ratio_x_ln_sales_start_P3`
- P1 and FULL retain the same source-column name `export_ratio_x_ln_sales_start_P1`, and both use P1 starting covariates. Their final products are rebuilt separately using their own complete-case estimation samples.
- With `centre_interaction_inputs=True`, each continuous input subtracts its actual estimation-sample mean before multiplication. Metadata-defined binary inputs stay 0/1; binary × binary inputs are not centred. Observed 0/1 values do not override a continuous metadata type.
- The resulting product follows the existing `standardise` metadata: z-score using ddof=0 in standardised variants, centred raw product in raw variants. Ordinary regressors retain their original treatment; no input is double-standardised.
- `add_interaction_columns()` initially prepares product availability for missing-value checks. `get_estimation_sample()` and `build_design_matrix()` rebuild the final product after complete cases; preliminary full-data products are never used as fitted regressors.
- Constituent main effects must accompany every active interaction; otherwise the code reports a specification error. Centring changes the interpretation of main effects to the other continuous input's mean, while preserving raw interaction coefficients, fitted values, residuals, R² and interaction t/p tests.
- Actual means, population SDs, product moments, correlations, all-predictor VIF and 64 before/after comparisons are recorded in `results_interaction_centring.xlsx`. Reproduce with `python3 code_check_interaction_centring.py`; see `documentation_interaction_centring.md`.

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

Both workbooks retain these specification-specific working and technical sheets:

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

The interaction report additionally contains `Model_Comparison` immediately before the technical separator. The primary report contains no generated interaction rows, coefficient records, variable labels or missingness references. Existing unrelated trajectory/correlation/data-quality outputs stay in their separate workbooks.

Trailing researcher notes and historical values are preserved by scenario and display label. On first migration, all original annotations are carried into the interaction workbook; notes on retained rows also remain in the primary workbook. Subsequent reruns preserve each workbook's own annotations. Unlabelled historical columns receive a clear reader-annotation heading, and existing researcher headings are preserved. These columns are historical/reader input, not new coefficient estimates, and should be reviewed when interpretations refer to the earlier interaction specification.

### Model_Comparison (interaction workbook only)

One row per scenario, period and variant (64 rows). It contains N without/with interactions, an identical-observations indicator, export-ratio and log-sales coefficients and p-values for both specifications, all active interaction coefficients and p-values, R² and adjusted R² for both specifications, and `delta_R2 = R2_with - R2_without`. Interaction columns are generated dynamically from metadata, including multiple active terms. Observation fingerprints are retained in `Diagnostics_Long` for both reports.

Variants are explicit: raw coefficients retain original regressor units; standardised variants retain the established ddof=0 scaling. The matched sample gives identical growth clipping limits and transformed outcomes in each pair. Coefficient changes are conditional associations; the extended main effects are evaluated at the other continuous input's sample mean, while primary coefficients describe additive associations. ΔR² measures the joint in-sample fit gain from all included interactions, not a causal effect or an automatic significance test. Adjusted R² may decrease even when R² increases.

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
`results_diagnostics_trajectories.xlsx`. This keeps regression
results separate from pre-modelling diagnostics while preserving the same shared
scenario definitions and exact model-variable logic.

`code_ols_scenarios.py` writes only the two OLS regression workbooks.
`results_diagnostics_trajectories.xlsx` is generated independently
by `code_diagnostics_trajectories.py` using the shared definitions in `code_config.py`.

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

Interpretation starts from `README`, `Compare_Main`, `Compare_Raw` and, for the extended specification, `Model_Comparison`. Descriptive/correlation outputs remain separate; auditability is preserved in each report's technical tabs.

## Last Updated For

- Script: `code_ols_scenarios.py`
- Output files: `results_ols_scenarios.xlsx`, `results_ols_interactions.xlsx`
- Main change: separate primary additive and extended centred-interaction reports, with identical observations and preprocessing for paired models.
- Date: 2026-10-08

## Reproducibility checks

Run `python code_check_ols_reporting.py` after creating both reports. Tests reproduce all 64 primary models by independently fitting the reduced design obtained by dropping interaction columns from the corresponding centred design; compare coefficients, p-values, covariance, predictions and R²; verify saved estimates, full grids, firm IDs, outcome transformations, model comparison, sheet order and HC3 estimator routing; and assert source/other outputs remain unchanged. Existing seven generic centring tests run with `python -m unittest code_check_interaction_centring.CentringTests` without overwriting the centring diagnostics workbook.

During migration, `GRIP_OLS_LEGACY_WORKBOOK=/tmp/grip_ols_pre_split.xlsx python code_check_ols_reporting.py` additionally compares every extended coefficient, SE, t-statistic, p-value, confidence interval, N and fit statistic against the preserved pre-split workbook and checks annotation preservation. This optional external snapshot is not required for normal reproducibility; keep a snapshot before later methodological changes if historical equivalence must be rechecked.

Validation on 8 October 2026: **10 reporting tests and seven generic centring tests passed**, including the migration snapshot comparisons. Each workbook contains 64 estimated models with zero skipped models, and all 64 pairs have exactly the same ordered NIPs and transformed outcomes. All 36 nonempty trailing reader/historical values from the previous workbook are preserved in the interaction workbook; notes on the removed interaction row are not copied into a nonexistent primary row. All 18 unrelated dataset/workbook files checked retain their pre-change SHA-256 hashes; shared configuration/exclusion definitions are unchanged. The technical-sheet structure, filters, frozen panes and model-comparison arithmetic were verified and representative working/comparison ranges visually inspected. No quantile, severe, trajectory, centring-diagnostics or data-quality output was regenerated.

The observed ΔR² range across the full model grid is 0.00001486–0.01072619. For RANK2019 `winsor_std`, P1/P2/P3/FULL gains are approximately 0.001480 / 0.000394 / 0.002107 / 0.001667. These are absolute R² differences, not percentage growth effects. Comparison coefficients/fit statistics display four decimal places; p-values below 0.0001 display `<0.0001`, while their full numeric precision is retained.
