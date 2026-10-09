# GRIP quantile regression

`code_quantile.py` produces `results_quantile.xlsx` through the shared quantitative pipeline. The statistical specification, statsmodels estimator, quantiles Q10/Q50/Q90, iteration limit 5,000 and tolerance 1e-6 remain unchanged. The preferred treatment remains winsor_std; no new quantile variants are invented.

## Shared eligibility

One approved common company set per population is supplied by `code_common_samples.py`: ALL 2,332; MANUFACTURING 949; RANK2019 1,748; RANK2019_MANUFACTURING 754. These exact IDs are shared with additive OLS, applicable interaction models, all four periods and regression diagnostics. All required control/lag/interaction source values are validated before selection. Quantile estimation must not silently drop firms. Optimisation failures or warnings are reported without substituting a sample.

The quantile specification retains the active export × size interaction. Continuous inputs are mean-centred under the existing shared engine. The separate profitability × manufacturing OLS interaction is part of common eligibility requirements for its applicable populations, but is not added to quantile models.

## Outcomes, timing and transformations

Current outcome basis is nominal annualised log sales growth. P1=2019→2020, P2=2020→2022, P3=2022→2024, FULL=2019→2024. Starting controls remain 2019/2020/2022, with FULL using P1 starting controls. P1 lag uses 2018 sales only; P2/P3 keep the existing lags; FULL has no lag. All existing controls, ownership and sector dummy definitions are retained.

The outcome alone is clipped at its existing 1st/99th percentiles after common membership is fixed. Winsorised outcome and metadata-designated continuous predictors use within-sample population SDs (ddof=0); binary indicators and sectors retain their coding. Predictor values are not winsorised. Changed sample means/SDs/cutoffs are expected consequences of membership alignment.

## Outputs and reproducibility

The established workbook sheets and two-line coefficient/p-value presentation remain. Model_Summary_Long now records population/sample/specification/model identifiers, N, unique company count and sorted-ID fingerprint. Numerical coefficients, inference, pseudo-R², convergence summaries, diagnostics and run logs are retained. No repeated exclusion register is added; consolidated eligibility and source evidence are in `results_data_quality_and_samples.xlsx`.

Run `python code_run_quantitative_pipeline.py` for the approved coordinated workflow. Audit and explicitly adopt future authoritative specification changes before revised production output. See [GRIP quantitative workflow](GRIP_quantitative_workflow.md). All 48 current fits completed without convergence warnings.
