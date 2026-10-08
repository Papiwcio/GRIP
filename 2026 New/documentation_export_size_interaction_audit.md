# Export intensity × log-sales interaction audit

Historical pre-implementation diagnosis. Mean-centred construction was subsequently implemented on 8 October 2026; see `documentation_interaction_centring.md` and `results_interaction_centring.xlsx` for the current methodology, all-model equivalence checks and coefficient scales. The tables below retain the original uncentred estimates as the audit baseline.

Audit date: 8 October 2026. Scope: the four RANK2019 coefficients in `results_ols_scenarios.xlsx`, `Compare_Main!G62:J62`, corresponding to P1/P2/P3/FULL standardised winsorised OLS. This was a read-only investigation of existing estimates. The results workbook, canonical files and manual-exclusion policy were not changed. Local workbook annotations and archive edits were preserved.

## Main finding

The displayed values are **regression coefficients**, not financial ratios or percentage growth effects. The coefficients and p-values reproduce from the current source data and shared model code to numerical tolerance. No multiplication, decimal-comma, percent-versus-fraction or scaling error was found in their construction.

The apparent magnitude largely reflects the chosen parameterisation. The interaction is the product of **uncentred** export intensity E and log sales L, then standardised as its own column. It is not the product of the two z-scores:

`Y_z = a + b_E z(E) + b_L z(L) + b_P z(E L) + other controls`.

Because mean L is approximately 13.4–13.8 and its SD only 0.85–0.96, E×L is approximately mean(L)×E. Export intensity and the product correlate at .996–.997. The product and main export regressor contain substantial overlapping variation; their coefficients largely offset each other. A one-SD change in the product while holding E and L separately fixed is not a feasible simple change in a firm's exports. These coefficients should not be read as direct export effects.

## Reproduction and equivalent centring diagnostic

Rebuilt the current nominal, outcome-only 1%/99% winsorised, standardised models from `data_period_2018-2024.parquet`, using the shared six-company exclusions, same complete-case firm IDs, continuous predictors, ownership/sector controls, period lags and nonrobust covariance. P1/FULL use 2019 starting covariates; P2 uses 2020; P3 uses 2022. `export_ratio = exports / sales` is a fraction (0.5 means 50%).

For a diagnostic only, replaced the standardised raw product with `z[(E−mean(E))(L−mean(L))]`, keeping the main effects and every other column and observation unchanged. Since E×L can be decomposed into the centred product, linear main effects and a constant, the designs span the same model space. Predictions differ by less than 1.7e−14; R² and the interaction t-test/p-value are unchanged. The coefficient sizes change because the standardised interaction has a different SD. Centring improves interpretation and removes the avoidable main-effect/product collinearity; it does not create new evidence or change the underlying interaction test.

| Period | N | Existing interaction coefficient | p-value | corr(E, E×L) | VIF E | VIF product | Centred-product coefficient | Centred VIF product |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| P1 | 1,786 | −0.646117 | .093571 | .996897 | 280.63 | 282.13 | −0.039603 | 1.060 |
| P2 | 1,791 | −0.319846 | .368746 | .996679 | 258.72 | 259.58 | −0.020454 | 1.062 |
| P3 | 1,795 | −0.686134 | .032316 | .996143 | 222.17 | 223.47 | −0.047220 | 1.058 |
| FULL | 1,794 | −0.685654 | .075212 | .996904 | 280.60 | 282.01 | −0.041977 | 1.057 |

VIF was calculated using the full design, including an intercept and all controls. See the [official statsmodels VIF definition](https://www.statsmodels.org/stable/generated/statsmodels.stats.outliers_influence.variance_inflation_factor.html). High VIF indicates inflation of coefficient variance from multicollinearity; it is not a test that the underlying data are wrong.

## Economically interpretable combined effects

At a fixed log-sales level L, increasing export intensity by ΔE changes fitted standardised growth by:

`ΔY_z = ΔE [b_E / SD(E) + b_P L / SD(E L)]`.

The standard error uses the full covariance of b_E and b_P. At mean log sales, a **10 percentage-point** increase in export intensity (ΔE=.10) gives:

| Period | Effect on growth, in outcome SD units | SE | p-value | 95% interval |
| --- | ---: | ---: | ---: | --- |
| P1 | −0.000389 | .007517 | .958746 | [−.015132, .014355] |
| P2 | +0.014540 | .007188 | .043246 | [.000442, .028638] |
| P3 | −0.032950 | .007081 | .000004 | [−.046838, −.019061] |
| FULL | −0.028924 | .007504 | .000120 | [−.043642, −.014206] |

These are conditional associations, not causal effects. They combine the main export coefficient with its interaction. They are not percentage growth changes and do not equal the displayed product coefficient. A significant combined export effect and an imprecise interaction test answer different questions and can coexist.

## Separate source-data concerns

Some actual export-intensity values require checking. In the respective RANK2019 complete-case samples:

| Period | Export ratio >100% | Export ratio >101% | Negative export ratio |
| --- | ---: | ---: | ---: |
| P1 | 11 | 3 | 0 |
| P2 | 15 | 6 | 1 |
| P3 | 8 | 1 | 0 |
| FULL | 12 | 4 | 0 |

The counts are model-specific observations, not distinct firms; there are 30 distinct firms across these flagged records. P1 and FULL overlap in their 2019 covariates. Tiny exceedances may reflect rounding or differing reporting scope and must be distinguished from substantial discrepancies.

Notable cases:

- Ernst & Young Usługi Finansowe Audyt sp. z o.o. Polska sp.k. GK, Warszawa, NIP 5260207930: 2019 sales 224,796.38242 versus exports 459,300 gives 204.318%; 2020 sales 257,582.53880 versus exports 519,749 gives 201.780%. These same input values exist in the upstream cleaned Excel file and enriched panel; the regression pipeline did not introduce the mismatch. The 2022 ratio is about 9.11%, indicating an additional reporting-comparability question. The audit has not established which underlying financial field or reporting scope should be corrected.
- East West Spinning sp. z o.o., Łódź, NIP 7290109601: 2019 ratio 114.870%.
- De Rooy Poland sp. z o.o., Warszawa, NIP 9281919804: 2020 ratio 143.052%.
- Iglotex Dystrybucja Polska sp. z o.o., Gdynia, NIP 6790175807: 2020 exports −2,324 against sales 654,301 gives −0.355%. A negative export revenue figure needs review of corrections/reporting scope.

For an audit-only sensitivity, removed observations outside E∈[0,1] from each model, **holding the original outcome winsorisation and all scaling parameters fixed**, and refitted the centred-equivalent design. Nothing was excluded in the saved analyses:

| Period | Restricted N | Centred interaction coefficient | p-value | Principal p-value |
| --- | ---: | ---: | ---: | ---: |
| P1 | 1,775 | −0.033769 | .159430 | .093571 |
| P2 | 1,775 | −0.018486 | .427652 | .368746 |
| P3 | 1,787 | −0.053199 | .015634 | .032316 |
| FULL | 1,782 | −0.033907 | .156669 | .075212 |

Removing only the E&Y record yields interaction p-values .154151/.449393/.032307/.115979 for P1/P2/P3/FULL. Thus P1/FULL's suggestive evidence is sensitive to these records; P3's negative interaction persists in both diagnostics. This does not justify blanket clipping or dropping every above-100% observation without source verification.

## Recommendation

Treat the large displayed interaction coefficients as a parameterisation/collinearity issue rather than automatically as extreme economic effects. For clearer reporting, centre export intensity and log sales before constructing the product, and report conditional export effects at representative firm sizes alongside the interaction. Any adopted change should be applied consistently through the shared model builders and documented across OLS, quantile and severe analysis; it was not implemented during this audit. Review the substantial export/sales inconsistencies against original financial statements before changing data or the exclusion policy. Current samples, coefficients, saved workbooks and canonical files remain unchanged.
