# Period quantile regression

## Reproduction and specification

Run `python3 code_quantile.py` from this directory. The input is `data_period_2018-2024.parquet`; the output is `results_quantile.xlsx`. Dataset values and canonical dataset schemas are not modified by this analysis.

The runner reads `code_config.PERIOD_MODEL_SETTINGS`, the same control panel used by `code_ols_scenarios.py`. It reuses the OLS model, interaction, variable metadata, categorical-level, and design-matrix builders. Changes to the shared model settings therefore apply to both analyses. The quantile runner currently estimates the OLS `winsor_std` specification at Q10, Q50, and Q90; it does not estimate OLS's three other variants.

The shared `code_config.MANUAL_EXCLUSIONS` removes the same five specified firms as OLS, by verified NIP or exact name (outer whitespace ignored), before analytical filtering, winsorisation and standardisation. README rows identify every firm, reason code and description, and distinguish newly removed firms from those already absent upstream. No additional predictor clipping was introduced. See `documentation_manual_exclusions.md` for rules and post-exclusion sample validation.

The dependent variable is **nominal annualised log sales growth**, based on current-price `sales`. It is measured in log points per year, not the log sales level or CAGR. Its source formulas are `(ln(sales_2020) - ln(sales_2019)) / 1` for P1, `(ln(sales_2022) - ln(sales_2020)) / 2` for P2, `(ln(sales_2024) - ln(sales_2022)) / 2` for P3, and `(ln(sales_2024) - ln(sales_2019)) / 5` for FULL.

The README identifies the nominal/real price basis, exact column, interval, and formula for each period. Each `Compare_*` sheet begins with `Dependent variable` and `Dependent variable column` rows. Their descriptions are generated from the shared growth mode; switching it to `real` selects `rgrowth_log_ann_*` columns and uses inflation-adjusted `sales_real`. These descriptions do not alter estimation or stored coefficients.

The current specification is:

- nominal annualised log growth as the dependent variable;
- four scenarios: ALL, MANUFACTURING, RANK2019, and RANK2019_MANUFACTURING;
- complete nominal trajectories required before model-specific missing-value exclusion;
- periods P1 (2019–2020), P2 (2020–2022), P3 (2022–2024), and FULL (2019–2024);
- numeric starting covariates: `ln_sales`, `profit_margin`, `export_ratio`, `asset_turnover`, `capital_ratio`, and `sales_per_employee`;
- `owner_num` as the foreign-ownership dummy;
- `export_ratio × ln_sales`, calculated from the corresponding starting covariates before standardisation;
- `sector_en` categorical controls, with `production` omitted as the reference category.

FULL uses P1 starting covariates and the same `export_ratio_x_ln_sales_start_P1` interaction as P1. Other periods use their own starting covariates and interactions.

## Lagged growth

| Model | Lag regressor | Prior growth interval |
| --- | --- | --- |
| P1 | `lag_ngrowth_log_ann_P1` | 2018–2019 |
| P2 | `lag_ngrowth_log_ann_P2` | 2019–2020 |
| P3 | `lag_ngrowth_log_ann_P3` | 2020–2022 |
| FULL | None | No lag in the shared OLS specification |

The comparison sheets display these period-specific coefficients under the shared OLS label `lag_growth_log_ann`. Exact source column names appear in `Coefficients_Long` and `Diagnostics_Long`; fuller descriptive labels appear in `Variable_Labels`. The earlier quantile workbook already contained these lag coefficients under longer labels; this update aligns their presentation and source configuration with OLS.

## Missing values, winsorisation, and standardisation

Within each scenario-period sample, observations missing the dependent variable, any active regressor, or any categorical control are dropped. Values are not imputed or replaced with zero. Source log-growth variables already require positive endpoint sales; a zero or non-positive sales endpoint produces missing growth in the dataset build.

The dependent variable alone is clipped to the sample's 1st and 99th percentiles, using pandas' default linear interpolation. Bounds are calculated after model-specific missing-value exclusions and recorded in `Diagnostics_Long`. Regressors are not winsorised.

The winsorised dependent variable and metadata-designated numeric regressors, lagged growth, and interaction are standardised within the estimation sample using `(value - mean) / population_standard_deviation`, with `ddof=0`. A zero or missing standard deviation uses divisor 1, matching the existing OLS behaviour. Ownership, sector dummies, and the intercept retain their original coding under the current metadata.

## Estimator and output interpretation

QuantReg retains `max_iter=5000`, `p_tol=1e-6`, and statsmodels' default robust covariance, Epanechnikov kernel, and Hall–Sheather bandwidth. OLS's `covariance_type='nonrobust'` is an estimator-specific option and is not passed to QuantReg. Quantile inference and pseudo-R² differ from OLS inference and R².

Q10, Q50, and Q90 estimate conditional growth quantiles given the regressors, rather than regressions on fixed bottom/median/top firm groups. They are not the unconditional `Performance_Thresholds` cutoffs.

`Compare_FULL`, `Compare_P1`, `Compare_P2`, and `Compare_P3` contain the readable coefficients and p-values. `Coefficients_Long` retains numerical coefficient estimates and inference; `Model_Summary_Long` records sample sizes and pseudo-R². Convergence warnings, if any, are retained in `Convergence_Summary`, `Diagnostics_Long`, and `Run_Log`. `Quantile_Patterns` is descriptive, not a formal cross-quantile test.

## Validation of the 6 October 2026 rerun

- All 48 models were estimated: four scenarios × four periods × three quantiles.
- No models were skipped and no convergence warnings were recorded.
- Shared settings, regressor lists, design matrices, standardisation lists, and winsorisation rules were checked against OLS `winsor_std` for all 16 scenario-period specifications.
- Model observation counts and winsorisation bounds were verified independently against the output workbook.
- All three period comparison sheets contain one populated `lag_growth_log_ann` row, covering all four scenarios and all three quantiles; FULL has no lag coefficient.
- The shared owner and lag inclusion switches were checked.
- Existing workbook tab names and ordering were preserved.
