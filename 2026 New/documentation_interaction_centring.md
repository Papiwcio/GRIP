# Mean-centred interaction construction and validation

Implemented 8 October 2026. Scope: shared GRIP interaction construction, all OLS scenarios/periods/variants, and the dependent quantile, severe-P1 and diagnostics runners. Canonical datasets, sample definitions, six manual exclusions, abnormal export ratios, ordinary-regressor preprocessing, outcomes, lag structures and covariance settings were not changed. Existing variable names and result-workbook sheet structures were retained. A separate `results_interaction_centring.xlsx` provides the requested comparison and provenance tables.

## Construction and reference population

`code_config.PERIOD_MODEL_SETTINGS['centre_interaction_inputs']` defaults to `True`. `False` is available only as the original uncentred reference for reproducibility/comparisons. Active products continue to come from `INTERACTION_METADATA`; no new substantive interaction is configured.

`add_interaction_columns()` prepares provisional raw product availability, so existing complete-case rules can run without changing their input schema. Those provisional products do not supply fitted regressors. `get_estimation_sample()` excludes missing outcomes, regressors and categorical controls, then calls `centre_interaction_columns()`. `build_design_matrix()` also invokes the same helper, covering callers such as QuantReg that prepare complete cases directly. Rebuilding uses the original constituent columns and is idempotent; it never subtracts a mean twice.

For each active product and actual scenario-period complete-case sample:

- A continuous/numeric input subtracts its sample mean.
- A binary input is identified by registry metadata (`variable_type='dummy'`) and remains 0/1. No binary type is inferred from observed values. Continuous × binary centres only the continuous component; binary × binary centres neither.
- Multiply those components, then apply the existing product `standardise` metadata exactly once. Standardised variants use the population SD (`ddof=0`); raw variants retain the centred product in raw units. Ordinary regressor columns are not changed by this helper.
- Both constituent main effects must be present. Missing main effects cause a specification error instead of an equivalence claim. Categorical interactions remain unsupported under the established architecture.

FULL still uses P1 starting covariates and the same source-column naming convention. Its reference means are independently computed from FULL complete cases, not copied from the P1 estimation sample. For RANK2019:

| Model | N | Mean export ratio | Mean log sales | Centred product SD |
| --- | ---: | ---: | ---: | ---: |
| P1 | 1,786 | .312178 | 13.469995 | .292139 |
| FULL | 1,794 | .312099 | 13.468768 | .291853 |

The exact full-precision input means, raw input SDs, types, offsets, product means/SDs and reference N for every scenario-period-variant are saved in `Input_Means_SDs`. Product records distinguish source lineage from sample-specific transformations. `Interaction_Details` reports constituent correlations before/after, coefficients, t/p and product VIF; `Predictor_VIF` covers every non-intercept predictor.

## Statistical equivalence and interpretation

With an intercept and both constituent main effects, `(X−mean(X))(Z−mean(Z))` equals `XZ` minus linear main-effect terms plus a constant. Therefore centring is a reparameterisation of the same OLS model space. Continuous × binary has the analogous identity `(X−mean(X))D = XD−mean(X)D`. Raw product coefficients and their t/p tests are invariant. Standardised product coefficients can change substantially because the centred product has a different SD; main effects and intercepts also change interpretation. For continuous × continuous, the main effect is evaluated at the other input's mean. Conditional/marginal effects remain the appropriate economic interpretation.

All 64 OLS model pairs passed: four scenarios × four periods × four variants. Identical input data, complete-case IDs, outcome transformations, controls and covariance were used on both sides. N, predictions, residuals, R², adjusted R², raw interaction coefficients and interaction t/p were checked. Maximum absolute prediction difference was **9.1431e−11**; the prediction/residual tolerance is `atol=1e−8, rtol=1e−7`, fit-statistic tolerance `atol=1e−10`, and interaction/raw-coefficient comparisons use explicitly bounded floating-point tolerances. No expected OLS invariance failed.

RANK2019 winsorised standardised comparison:

| Period | Interaction before | Interaction after | p before/after | Product VIF before | Product VIF after |
| --- | ---: | ---: | ---: | ---: | ---: |
| P1 | −.646117 | −.039603 | .093571 | 282.128 | 1.060 |
| P2 | −.319846 | −.020454 | .368746 | 259.579 | 1.062 |
| P3 | −.686134 | −.047220 | .032316 | 223.473 | 1.058 |
| FULL | −.685654 | −.041977 | .075212 | 282.014 | 1.057 |

All current scenarios improve substantially for this product: principal product VIFs fall from approximately 73.8–401.4 to 1.049–1.191. This removes the avoidable constituent/product collinearity, not every possible relationship among other regressors. The workbook reports all predictor VIFs rather than implying that every source of multicollinearity disappeared.

## Severe analysis and marginal effects

The shared export-size term uses the same centred construction in severe-logit and P2/P3 OLS controls. AME derivatives use the other component minus its fitted centring mean. Binary counterfactual products are regenerated with fitting-sample offsets fixed, so prediction calculations do not redefine centring populations. Numerical probability-gradient checks cover all 37 principal AMEs.

The selected supplementary `profitability_z × BottomP1` already uses a centred continuous variable and a 0/1 indicator; its established product units and non-restandardisation rule remain intact. All six Firth predicted-probability vectors and AME/SE/p tables, and all 24 supplementary OLS fitted-value vectors and profitability contrast estimates/SE/p, were compared with the uncentred reference and passed (`atol≤1e−7` for AMEs/contrasts; `1e−8` for fits/probabilities). No estimator switch was made.

## Quantile numerical caveat

All 48 QuantReg fits rebuild successfully with the same observations, dependent transformation and estimator settings, with zero convergence warnings. Centring preserves the mathematical conditional-quantile model space, but statsmodels IRLS stops using coefficient-change tolerances that depend on parameterisation. At the existing `p_tol=1e−6`, before/after saved fitted quantiles are therefore not bit-for-bit identical.

The largest observed fitted-quantile discrepancy was **.028817 outcome SD** in ALL/FULL/Q90; its mean absolute discrepancy was **.001060 SD**. RANK2019_MANUFACTURING/P2/Q90 showed a maximum .011784 SD (mean .000374); RANK2019/P3/Q50 showed maximum .005848 SD (mean .000406). These are finite-iteration approximations, not changes to the model's variables or sample. No solver tolerance/covariance policy was silently changed, and OLS invariance is not claimed for these approximate quantile outputs. Further precision-focused QuantReg work is separate from this OLS-centering task.

## Reproduction and quality assurance

1. `python3 code_check_interaction_centring.py` runs seven generic automated tests, exports the 64-model comparison workbook, and independently checks severe-model equivalence using temporary outputs.
2. Rebuild active outputs with `code_ols_scenarios.py`, `code_quantile.py`, `code_diagnostics_trajectories.py`, and `code_severe_p1_decline.py`.
3. `python3 code_check_manual_exclusions.py` verifies all saved sample counts, six-company README audits, required sheets, no error cells and excluded-firm absence.
4. `python3 code_check_severe_p1_decline.py` checks thresholds, shared designs, Firth/BFGS fitting, QR OLS reproduction, contrasts and interaction-aware numerical AMEs.

The generic tests cover continuous × continuous, continuous × binary, binary × binary metadata, an observed-0/1 continuous input, missing observations, different reference samples, P1/FULL alignment, multiple simultaneous interactions, no active interactions, raw/standardised/winsorised variants, and missing-main-effect failure. Temporary test-only products are restored and never configured in substantive analyses.

Canonical files were hash-checked unchanged. Four existing reader annotations in trailing OLS comparison columns were retained by scenario/row label; their presence is also verified against the pre-change workbook. Existing local core-workbook and archive edits, including the untracked archive snapshot, were preserved and are outside this commit. The earlier `documentation_export_size_interaction_audit.md` is the historical pre-implementation diagnosis; this document and the current workbooks record the implemented centring.
