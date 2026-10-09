# GRIP OLS scenarios

The primary additive workbook is `results_ols_scenarios.xlsx`. Its estimation engine remains `code_ols_scenarios.py`; routine coordinated execution is `python code_run_quantitative_pipeline.py`. The compact interaction supplement is produced by the same engine, with its existing two separate specifications.

## Common company samples

The central pipeline in `code_common_samples.py` supplies ALL, MANUFACTURING, RANK2019 and RANK2019_MANUFACTURING. Current Ns are 2,332; 949; 1,748; 754 respectively. Each population has one company-ID set across P1/P2/P3/FULL, raw/standardised and baseline/winsor OLS, applicable interaction specifications, quantiles and corresponding diagnostics. Missing inputs cannot silently drop firms after central selection. Population membership is distinct from data validity.

Complete trajectories and the six configured manual rules remain applicable; five manual firms are present and Orlen is already absent upstream. The adopted logical validity policy requires comparable export intensity within [0,1] and valid model-specific inputs/denominators. Source values are unchanged. The central workbook owns unique company exclusions and annual evidence; individual regression workbooks link to it instead of repeating those lists.

See [GRIP quantitative workflow](GRIP_quantitative_workflow.md) for authoritative selection, dynamic eligibility, approval/adoption, sample-change reporting and validation.

## Statistical specification

Current mode is nominal annualised log sales growth, measured in log points per year. P1 uses `(ln(sales_2020)-ln(sales_2019))/1`; P2 uses `(ln(sales_2022)-ln(sales_2020))/2`; P3 uses `(ln(sales_2024)-ln(sales_2022))/2`; FULL uses `(ln(sales_2024)-ln(sales_2019))/5`. A configured switch to real mode uses the corresponding CPI-adjusted sales and existing real growth columns.

Starting regressors remain `ln_sales`, `profit_margin`, `export_ratio`, `asset_turnover`, `capital_ratio`, `sales_per_employee`, ownership and sector controls. Covariates come from 2019/2020/2022 for P1/P2/P3, and 2019 for FULL. P1 lag is 2018–2019; P2/P3 retain prior-defined-period lags; FULL has no lag. Main OLS contains no interactions. Shared covariance remains nonrobust; generic configured covariance support is preserved.

Each scenario retains four variants: baseline, baseline_std, winsor, winsor_std. Only outcomes are winsorised at 1%/99% within the final common sample, using existing linear quantile interpolation. Standardisation uses existing `(x-mean)/SD` with ddof=0 and divisor 1 for zero dispersion. No predictor clipping is introduced. The reduced company sample changes means, SDs, interaction centring and winsor cutoffs through these existing formulas.

## Workbook structure

Nine populated sheets: README, Compare_Main, Compare_Raw, Model_Summary_Long, AUDIT_AND_TECHNICAL_TABS, Diagnostics_Long, Coefficients_Long, Variable_Labels, Run_Log. `Dropped_Rows_Long` is superseded by the consolidated unique registers. Compare_Main displays standardised variants; Compare_Raw displays raw variants, with coefficients above p-values. Reader annotations on comparison sheets remain preserved by scenario and row label; original workbooks are also archived.

Model_Summary_Long retains coefficients' supporting fit statistics and metadata: model ID, population ID, COMMON sample ID, specification ID, N, unique company count and sorted company-ID SHA256. Coefficients_Long retains estimates, SEs, p-values and confidence intervals. Run logs and regression-specific statistical diagnostics remain.

## Validation and history

The 64 primary models retain identical company sets within populations. Saved Ns/fingerprints, invalid-input exclusion, nesting, source hashes and consolidated registers are checked automatically. Matched-sample tests against Git commit 674476e confirm unchanged transformed outcomes, design matrices and numerical estimates. Archived period-specific results differ because Stage 2 deliberately changes membership, not because an estimator or variable was substituted. The required historical comparison input `archive/results_ols_2026-05-06.xlsx` remains preserved.
