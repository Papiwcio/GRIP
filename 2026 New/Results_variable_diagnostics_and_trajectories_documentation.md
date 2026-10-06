# Results_variable_diagnostics_and_trajectories

## Purpose

`Results_variable_diagnostics_and_trajectories.xlsx` is the structured pre-modelling diagnostics and trajectory-analysis workbook supporting the regressions in `Results_period_ols_scenarios.xlsx`.

The workbook is rebuilt directly and completely by `run_trajectory_analysis.py`. The script constructs the OLS-aligned diagnostic, correlation, trajectory and audit sheets. It does not write or replace the regression workbook.

`run_period_ols_scenarios.py` independently generates `Results_period_ols_scenarios.xlsx` only. It does not call the trajectory runner or write this diagnostics workbook.

Shared scenario definitions, period definitions, model variables, interaction rules, winsorisation settings, and sample masks are sourced from `analysis_config.py`. After writing, `run_trajectory_analysis.py` reopens the workbook and fails if the sheet order differs or any required sheet is empty.

Input:

- `Data_period_2018-2024.parquet`

Main analytical window:

- 2019-2024

Role of 2018:

- used only to calculate P1 lag growth
- not used as a trajectory outcome period

## Shared Scenarios

Scenario definitions come from `analysis_config.get_scenario_definitions()` and are shared with OLS:

- `ALL`
- `MANUFACTURING`
- `RANK2019`
- `RANK2019_MANUFACTURING`

The selected real or nominal complete-trajectory flag is applied in addition to the scenario filter. `30_SCENARIO_SUMMARY` reports filter logic, observation counts, and inclusion in OLS and trajectory diagnostics.

## Workbook Structure

### Navigation

- `00_README_STRUCTURE`: purpose, relationship to OLS, analytical sequence, section legend, colour coding, and correlation interpretation warnings

### Variables And Sample Diagnostics

- `01_VARIABLES`: generated variable dictionary
- `02_SAMPLE_SUMMARY`: sample composition and growth summaries
- `03_MISSINGNESS`: scenario-period-variable missingness and descriptive statistics before complete-case model deletion
- `04_WINSOR_IMPACT`: before/after winsorisation counts and paired distribution diagnostics only for variables actually winsorised

### Trajectory Analysis

- `10_TRAJECTORY_SUMMARY`: trajectory counts and shares
- `11_TRAJECTORY_BY_SCENARIO`: growth summaries by trajectory and scenario
- `12_TRAJECTORY_BY_SECTOR`: sector and ownership trajectory profiles
- `13_TRAJECTORY_PROFILE`: long-form median, p10 and p90 profiles
- `14_TRAJECTORY_PROFILE_PIVOT`: readable profile comparison
- `15_PERFORMANCE_BANDS`: trajectory distribution by performance band
- `16_PATH_INDEX`: path-index diagnostics

### Correlation Diagnostics

- `20_CORR_MAIN_BASELINE`: clearly separated baseline `FULL`-period matrices for all four shared scenarios
- `21_CORR_MAIN_WINSOR`: clearly separated winsorised `FULL`-period matrices for all four shared scenarios
- `22_CORR_WITH_DV_BASELINE`: dependent-variable correlations by scenario and period, with two-sided Pearson p-values
- `23_CORR_WITH_DV_WINSOR`: winsorised dependent-variable correlations by scenario and period, with two-sided Pearson p-values
- `24_CORR_PREDICTOR_RISK`: predictor-predictor correlations, high-correlation flags, and baseline/winsor consistency check
- `25_CORR_STABILITY_SCENARIOS`: correlations compared across the shared scenarios

Pearson correlations for standardised variants are intentionally not exported because non-degenerate linear standardisation does not change Pearson correlations.

Winsorised correlations are separate because winsorisation changes extreme dependent-variable values and can change correlations involving the dependent variable. Predictors are not winsorised under the current model specification, so predictor-predictor correlations should be identical across baseline and winsor variants.

Correlations are diagnostic evidence about direction, redundancy, and stability. They are not mechanical variable-selection criteria.

For every row in `22_CORR_WITH_DV_BASELINE`, `23_CORR_WITH_DV_WINSOR`, and `90_CORR_LONG_ALL`, `p_value` is the two-sided p-value for testing a zero population Pearson correlation. It is calculated with `scipy.stats.pearsonr()` from exactly the same pairwise-complete observations used for `correlation` and `N`. The stored value remains numeric and is displayed to four decimal places. If fewer than two paired observations are available or either variable is constant, both `correlation` and `p_value` are missing; `N` retains the pairwise observation count.

The matrix sheets `20_CORR_MAIN_BASELINE` and `21_CORR_MAIN_WINSOR` continue to display correlation coefficients only and do not include p-values.

### Scenario Diagnostics

- `30_SCENARIO_SUMMARY`: shared definitions, counts and inclusion flags
- `31_SCENARIO_DIAGNOSTICS`: variant-aware missingness and descriptive diagnostics for all four shared scenarios, with explicit sample-inclusion notes

### Audit Appendices

- `90_CORR_LONG_ALL`: complete long-form pairwise correlation audit trail, including two-sided Pearson p-values
- `91_APPENDIX_FULL_MATRICES`: matrices for every scenario, period, and exported correlation variant
- `92_FIRM_TRAJECTORIES`: firm-level trajectory inspection table
- `93_CONFIG_AUDIT`: selected trajectory family, period definitions and method settings

## Winsorisation Impact Rules

`04_WINSOR_IMPACT` contains one row per scenario, period and variable actually winsorised. `model_variant` is reported once as `winsor`; `winsor_std` is not duplicated because standardisation changes the regression specification, not the underlying winsorisation. Under the current specification only the dependent variable is winsorised, so predictors are not listed as zero-change rows.

Reported fields include:

- non-missing counts before and after
- number of values changed
- adjacent before/after pairs for minimum, p01, p05, median, p95, p99 and maximum

Missing values remain missing. Winsorisation does not replace missing values with zero.

## Presentation

All sheets:

- freeze the top row
- use bold, role-coloured headers
- use filters where appropriate
- hide gridlines
- use typed numeric cells and consistent number formats

Tab colours:

- dark blue: navigation
- green: variables and sample diagnostics
- blue: trajectory results
- orange: correlation diagnostics
- purple: scenario-specific diagnostics
- grey: detailed audit appendices

## Last Updated

- Source script: `run_trajectory_analysis.py`
- Output: `Results_variable_diagnostics_and_trajectories.xlsx`
- Date: 2026-09-29
