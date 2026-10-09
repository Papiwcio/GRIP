# Stage 2 common-sample update

The additive severe-decline analysis is included in the unified pipeline. RANK2019 uses 1,748 firms and its manufacturing subset 754, identical to corresponding OLS, interaction, quantile and trajectory populations. P1/P2/P3 retain their existing outcomes, predictors, lags, threshold definitions and Firth/OLS estimators. Primary and threshold model summaries now retain company fingerprints and sample/model metadata. Complete-case loss after central selection is a critical error, not a new sample. Exclusion details are reported centrally. Any numerical examples below from earlier runs are historical; current estimates are in `results_severe_p1_decline_analysis.xlsx`.

See [GRIP quantitative workflow](GRIP_quantitative_workflow.md).

# Severe P1 decline supplementary analysis

## Scope and reproduction

Run `python3 code_severe_p1_decline.py` from this directory. Run `python3 code_check_severe_p1_decline.py` to reproduce the workbook with independent mathematical and model-alignment checks.

Input: `data_period_2018-2024.parquet`. Output: `Results_severe_P1_decline_analysis.xlsx`, retaining the exact filename in the user's specification as an explicit exception to the default lowercase convention. This is a separate supplementary workbook; canonical datasets, archive files, main OLS, quantile, and diagnostics workbooks are not modified.

The user selected two samples: `Rank2019` and `Rank2019_Manufacturing`. Both use the main OLS nominal complete-trajectory requirement. The manufacturing sample is nested within the ranking sample; the two results are not independent replications. There is no ALL or manufacturing-only scenario grid, FULL model, quantile model, or duplicated raw/baseline variant.

`code_config.MANUAL_EXCLUSIONS` now applies the same six specified company exclusions as all main regressions and diagnostics, before sample selection, fixed-group construction and transformations. The README reports each configured firm, verified NIP, reason code/description and removed/already-absent status. Orlen is already absent from the canonical input; Ignitis, Elektrobudowa, Kania, Globus and Ordipol are removed from the analysis in memory. Canonical files retain their rows and schema.

Current group counts before model-specific missing-value exclusions:

| Sample | Analytical N | Below −15% | Below −20% | Below −25% |
| --- | --- | --- | --- | --- |
| Rank2019 | 1,822 | 341 (18.72%) | 226 (12.40%) | 153 (8.40%) |
| Rank2019_Manufacturing | 786 | 133 (16.92%) | 87 (11.07%) | 53 (6.74%) |

Logit complete-case N is 1,786 and 781, respectively; P2 N is 1,791 and 782; P3 N is 1,795 and 783. Each period uses the same N across thresholds. On 8 October 2026, all interaction terms were removed at the researcher's request. The previous workbook is retained as `archive/results_severe_p1_decline_analysis_2026-10-08.xlsx`.

## Fixed severe-decline definitions

Current shared growth mode: **nominal**. Membership uses **simple nominal P1 sales growth**, `ngrowth_P1 = sales_2020 / sales_2019 - 1`, not annualised log growth. If the shared growth mode changes to real, the same definitions use `rgrowth_P1` and inflation-adjusted sales.

| Variable | Rule |
| --- | --- |
| `BottomP1_15` | 1 when P1 simple growth < −0.15; otherwise 0 |
| `BottomP1_20` | 1 when P1 simple growth < −0.20; otherwise 0; principal specification |
| `BottomP1_25` | 1 when P1 simple growth < −0.25; otherwise 0 |

Equality to the cutoff belongs to the other-firm group. Missing or nonfinite P1 growth gives missing membership, never 0. Source growth requires positive endpoint sales; missing, zero, and non-positive endpoints are handled as unavailable by the canonical build. The complete-trajectory sample is checked for available membership.

Each indicator is created in analysis memory only and remains fixed for all later P2/P3 models. Exactly one threshold indicator appears in any regression. The three severe groups are nested: BottomP1_25 ⊆ BottomP1_20 ⊆ BottomP1_15.

## Sampling and predictors

Apply `code_config.build_sample_mask` with the relevant sample and `has_complete_ntrajectory`. Model-specific complete cases and design matrices use the same functions as `code_ols_scenarios.py`.

The logistic predictor set is the primary additive P1 specification: 2019 `ln_sales`, `profit_margin`, `export_ratio`, `asset_turnover`, `capital_ratio`, and `sales_per_employee`; `owner_num`; 2018–2019 annualised log-growth lag; and sector controls with `production` as reference. No interaction, realised P1 outcome, later-period covariate, or trajectory classification enters the logistic predictor matrix.

Starting covariates are period-specific as in OLS. Ownership and sector use the established stable firm descriptors, treated as conceptually predetermined. Those canonical descriptors take the first non-missing observation from 2019 onward; their predetermination is a modelling assumption rather than proof that every underlying observation predates P1.

Model-specific missing values are dropped without imputation. Continuous predictors retain the main standardised OLS definitions: centre and divide by population SD (`ddof=0`) within the estimation sample. A zero SD uses divisor 1, as in main OLS. Ownership, sector indicators, and BottomP1 indicators retain their existing dummy coding. Regressors are not winsorised. The runner uses local `include_interactions=False`; shared metadata and other analysis specifications are unchanged.

## Logistic estimator and inference

The manufacturing sample has 15 health/pharma firms with no severe decline at any threshold. Its ordinary maximum-likelihood sector coefficient is separated. The recommended bias-reduced method is therefore used consistently for both samples and all thresholds, retaining every firm and all established sector controls. The method choice was disclosed during implementation; it is explicitly labelled in the workbook. The script supports ordinary logistic fitting if subsequently requested, with unreliable fits reported rather than presented as valid results.

Firth estimates maximise `l(β) + 0.5 log|X'WX|`. The adjusted score is `X'[y − p + h(0.5 − p)]`, with `p = logistic(Xβ)`, `W = diag[p(1−p)]`, and `h` the diagonal leverage. The implementation uses Fisher scoring with a bounded change in the linear predictor and step-halving. It requires a full-rank design and an adjusted-score maximum below `1e-8`; failed convergence is never silently treated as success.

Coefficient SEs use inverse expected Fisher information. z tests and 95% coefficient intervals are approximate Wald inference, not profile-penalised likelihood intervals. Odds ratios and their 95% limits exponentiate coefficients and coefficient limits.

The global likelihood-ratio test compares the full penalised fit to a restriction setting all slopes to zero, retaining the same full-design Jeffreys penalty. Degrees of freedom equal the number of slopes. McFadden pseudo-R² uses the unpenalised log-likelihood evaluated at the bias-reduced coefficients versus intercept-only ML. `AIC_plug_in` and `BIC_plug_in` use that unpenalised likelihood and the number of model parameters; they are descriptive plug-in diagnostics, not conventional maximum-likelihood model-selection criteria. AIC/BIC should not be compared across thresholds that define different responses.

Method references: [official logistf documentation](https://search.r-project.org/CRAN/refmans/logistf/html/logistf.html), [official penalised LR implementation](https://raw.githubusercontent.com/cran/logistf/master/R/logistftest.R). The former documents Firth logistic fitting and the available Wald versus profile-likelihood inference; the latter shows the constrained fit with the full design retained.

## Average marginal effects

Continuous AMEs average the probability derivative for one SD increase in a standardised predictor in the additive logit. There are no product-rule contributions because the design contains no interactions. Historical centred-interaction results remain in the archived pre-change workbook; they are not current severe-decline estimates.

For a design derivative `D`, `AME = mean[p(1−p) Dβ]`. Its delta-method gradient is `mean[p(1−p)D + p(1−p)(1−2p)(Dβ)X]`. Variance is `g'Cov(β)g`; 95% intervals and z tests use the normal approximation. This is a local average derivative, not a finite one-SD jump in probability.

Ownership uses average counterfactual probability differences from 0 to 1. Sector effects contrast each sector with production while setting the other sector dummies to zero. Discrete-effect gradients are the averaged differences of `p(1−p)X` between the two counterfactual matrices. AMEs are reported in probability units and additionally in percentage points.

## Subsequent additive growth models

P2 outcome: `ngrowth_log_ann_P2 = [ln(sales_2022) − ln(sales_2020)] / 2`. P3 outcome: `ngrowth_log_ann_P3 = [ln(sales_2024) − ln(sales_2022)] / 2`. These are nominal annualised log sales growth, not simple growth rates or log sales levels.

Use the principal OLS `winsor_std` treatment: clip only the outcome at its 1st and 99th estimation-sample percentiles, then standardise it using population SD. All original additive period-specific covariates, ownership, sector controls and lag-growth controls remain present. Add the unstandardised fixed BottomP1 dummy. Neither export × size nor profitability × BottomP1 is estimated. Removing an interaction can change remaining coefficients; these additive refits supersede the archived estimates.

OLS coefficients are in standard deviations of the transformed outcome. The group coefficient is a conditional group difference. In P2, the lag includes P1 growth, so BottomP1 captures an additional threshold association conditional on continuous prior growth. Subsequent starting covariates may be mediators of P1 decline. These coefficients are not causal recovery effects.

Only `profit_margin_start_P2` or `profit_margin_start_P3` is additionally interacted with BottomP1. Form `z_profitability × BottomP1` after standardising profitability, and do not restandardise the new product. Thus β1 is the slope for other firms, β3 the slope difference, and β1+β3 the severe-group slope. Its variance includes both component variances and twice their covariance. Tests and intervals use the OLS residual degrees of freedom and the same `nonrobust` covariance setting as the main OLS model. BottomP1's main effect is evaluated at mean profitability.

## Workbook

Exactly seven sheets are produced, retaining existing sheet names:

1. `00_README`: definitions, samples, timing, method, outcome units, cautions and sources.
2. `01_GROUP_PROFILE`: principal group counts; raw continuous N/mean/median/sample SD and mean differences; ownership, sector, and manufacturing composition.
3. `02_LOGIT_BOTTOMP1`: principal logistic statistics, coefficients, z inference, odds ratios, and both coefficient and odds-ratio intervals.
4. `03_LOGIT_MARGINAL_EFFECTS`: principal continuous and discrete AMEs, SEs, z tests, confidence intervals, and percentage points.
5. `04_P2_GROUP_MODEL`: principal P2 group-model statistics and complete coefficient table.
6. `05_P3_GROUP_MODEL`: principal P3 group-model statistics and complete coefficient table.
7. `07_THRESHOLD_ROBUSTNESS`: group sizes/shares, selected size/profitability/ownership/capital-ratio logistic effects and P2/P3 additive group coefficients across all thresholds.

Retained diagnostics comprise raw growth 5th/95th percentiles and maxima in `01`; separation/sector counts and a diagnostic ranking MLE comparison in `02`; matched-sample P2 without-lag sensitivity in `04`; and additive-model VIF/subgroup variation/outlier influence in `05`. `07` displays capital-ratio AMEs across thresholds. The former `06_SELECTED_INTERACTIONS` tab and interaction-specific robustness tables are removed. Sample display labels match the main uppercase scenario names; the internal shared mask keys remain `Rank2019` and `Rank2019_Manufacturing`. Coefficient tables label z-score versus dummy scale. Continuous AMEs are local derivatives, not finite one-SD probability jumps.

The original `documentation_severe_p1_decline_audit.md` is retained as a historical pre-exclusion audit. All earlier interaction estimates are historical and are superseded by the additive workbook. The pre-change report, including researcher formatting, remains in `archive/results_severe_p1_decline_analysis_2026-10-08.xlsx`. Existing manual exclusions remain; no additional outlier exclusion or predictor winsorisation was introduced. Current influence and VIF diagnostics apply to the additive fits and do not establish broad robustness or causality.

Ranking has no complete/quasi-complete separation and ordinary MLE converges at all thresholds. The current common-Firth choice was retained rather than automatically changed; the audit recommends ordinary MLE for ranking and Firth for manufacturing as a future methodological choice. Approximate Wald/delta covariance in this project uses original expected Fisher information, not current R logistf's augmented-data covariance implementation or profile likelihood. Main OLS and supplementary OLS both use the shared `nonrobust` setting; no robust-covariance replacement was made.

Descriptive means are unweighted, use non-missing raw values, and are not winsorised or standardised. SD uses `ddof=1`; a group with fewer than two observations has missing SD. Profile shares divide by the full selected complete-trajectory sample or the corresponding full group, as labelled; these denominators differ from model-specific complete-case N. Control missingness is an explicit category if present. The workbook retains typed numerical cells, Excel Tables with filters, frozen headers and identifying columns, and readable output formatting.

## Validation

`code_check_severe_p1_decline.py` checks strict cutoff boundaries and missing values; Firth's closed-form add-half solution for a completely separated two-group model; a separate numerical optimiser; the adjusted-score gradient; AMEs and delta-method gradients by finite differences; exact additive OLS sample and design-matrix alignment; identical outcome winsorisation/standardisation; fixed nested groups; a single group indicator per model; absence of all products and interaction-specific sheets; and independent QR coefficients/covariances for all twelve OLS fits. All six actual-data Firth fits and all 37 primary AMEs were independently reproduced on 8 October 2026. Other regression workbooks and canonical datasets remained unchanged.

The current run comprises six additive logistic and twelve additive group OLS primary/threshold fits, plus one ranking MLE and two P2 no-lag diagnostic fits. `code_audit_severe_p1_decline.py` reproduces audit tables without overwriting the supplementary workbook. Main data and workbooks are hashed before and after validation and must remain unchanged. Validation output reports firm counts, model-specific sample sizes, missingness, solver convergence, primary outcome summaries, and required sheets. No supplementary variables or estimates are written into canonical datasets.
