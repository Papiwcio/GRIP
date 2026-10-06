# Shared manual analytical exclusions

Updated 6 October 2026 after the user requested restoration of the four historical company exclusions and a complete rerun of OLS, quantile, severe-P1 and diagnostics.

## Configuration and matching

`code_config.MANUAL_EXCLUSIONS` is the single configuration section. Its `enabled` switch defaults to `True`; `companies` lists the exact requested names and verified NIPs. Every analytical loader applies it before scenario selection, trajectory completeness, model-specific complete cases, outcome winsorisation, standardisation and regression fitting. The shared sample masks also enforce it, so direct callers cannot bypass it accidentally.

A row is excluded when its NIP matches a configured identifier **or** its company name exactly matches a configured name. Only outer whitespace is ignored; there is no substring, fuzzy or case-insensitive matching. NIP matching preserves the same firm exclusion if a company is renamed. Other Orlen-related companies are not excluded simply because their names contain “Orlen”. Missing values do not match an exclusion. Exclusions apply to entire firms in every period, model variant, quantile, supplementary threshold and diagnostic/trajectory output.

The four identifiers were checked against the current period input and the upstream removal rule. The current input contains 2,533 unique firms. Three are removed in memory, leaving 2,530 before trajectory/scenario filters:

| Configured company | Verified NIP | Firms removed in this run | Status |
| --- | --- | ---: | --- |
| Orlen SA GK, Płock | 7740001454 | 0 | Already absent from input; upstream exclusion retained |
| Ignitis Polska sp. z o.o., Warszawa | 5252714003 | 1 | Removed from analysis |
| Elektrobudowa SA w upadłości likwidacyjnej GK, Katowice | 6340135506 | 1 | Removed from analysis |
| Zakłady Mięsne Henryk Kania SA w upadłości | 7440003325 | 1 | Removed from analysis |

The same names, NIPs, matched firm/row counts and statuses are printed at runtime and listed in each results workbook's existing README. A firm already absent upstream is not counted as newly removed. Manual removals are distinguished from subsequent missing-value exclusions.

## Data scope

The exclusion policy is analytical. Canonical panel/core/period files are retained; all five current `data_*` files were hash-verified unchanged from the start of this task. A pre-existing local edit to `data_core_2018-2024.xlsx` was preserved and excluded from this task's Git commit. Archive results were untouched.

Dataset performance cutoffs and performance-band classifications remain those already built in the canonical period dataset. The diagnostic workbook reports those stored bands on the reduced sample; this change does not silently redefine canonical percentile benchmarks. Severe-P1 membership still uses strict raw nominal P1 decline cutoffs; surviving firms keep the same membership.

The existing model definitions were retained: nominal annualised log growth, period covariates/lags, production-reference sector controls, outcome-only winsorisation, within-model complete-case standardisation, and estimator-specific covariance. Firth remains the severe-logit estimator in both selected samples. No predictor winsorisation or additional outlier deletion was introduced.

## Reproduction and checks

Run from this directory:

1. `python3 code_ols_scenarios.py`
2. `python3 code_quantile.py`
3. `python3 code_diagnostics_trajectories.py`
4. `python3 code_severe_p1_decline.py`
5. `python3 code_check_manual_exclusions.py`
6. `python3 code_check_severe_p1_decline.py`

The last command regenerates the severe workbook and performs independent mathematical checks; it leaves the three main workbooks unchanged. The historical audit document records the pre-exclusion findings and is marked as superseded for numerical interpretation.

Validation completed:

- 64 OLS models, zero skipped models and no runner warnings.
- 48 quantile models, zero skipped models and zero convergence warnings.
- 6 severe-logit, 12 severe-group OLS and 12 profitability-interaction OLS fits; plus the existing ranking MLE and two P2 no-lag comparisons.
- Complete diagnostics/trajectory workbook rebuilt, with no empty required tabs.
- Exact analytical/complete-case sample agreement between OLS, quantile and diagnostics; severe samples match corresponding OLS samples.
- Exact-name and NIP matching independently tested, enabled/disabled switching tested, other Orlen companies retained, all four configured firms absent from every analytical mask and firm-level trajectory export.
- All four saved README audits present; expected sheet orders and no Excel error cells verified. Independent QR reproduction of all 24 supplementary OLS coefficients/covariances and numerical checks of all 37 principal AMEs passed. All six actual-data Firth fits were reproduced with a separate BFGS optimiser.

The legacy archived OLS-reference comparison is not equal to the current ALL output; its sample/specification predates the current four-variant design and these exclusions. This is recorded as a historical comparison, not treated as a requirement to restore obsolete samples.

## Current samples

Complete nominal trajectory counts precede model-specific missing-value exclusions. OLS and quantile use the same complete-case N for each scenario-period and all their variants/quantiles:

| Scenario | Complete trajectory N | P1 N | P2 N | P3 N | FULL N |
| --- | ---: | ---: | ---: | ---: | ---: |
| ALL | 2,509 | 2,386 | 2,428 | 2,440 | 2,425 |
| MANUFACTURING | 1,007 | 979 | 995 | 1,000 | 995 |
| RANK2019 | 1,824 | 1,787 | 1,793 | 1,797 | 1,796 |
| RANK2019_MANUFACTURING | 786 | 781 | 782 | 783 | 784 |

Severe groups before model-specific exclusions:

| Sample | Below −15% | Below −20% | Below −25% |
| --- | ---: | ---: | ---: |
| RANK2019 (N=1,824) | 342 (18.75%) | 227 (12.45%) | 154 (8.44%) |
| RANK2019_MANUFACTURING (N=786) | 133 (16.92%) | 87 (11.07%) | 53 (6.74%) |

## Revised severe-P1 results

These coefficients use standardised growth outcomes and, for profitability slopes, the current estimation-sample profitability SD. Exclusions change both observations and scaling; direct comparison with previous standardised coefficients therefore is not a pure deletion with fixed units.

Principal −20% group coefficients and matched P2 diagnostic:

| Sample | P2 with lag (p) | P2 without lag (p) | P3 group effect (p) |
| --- | ---: | ---: | ---: |
| RANK2019 | −0.060205 (.465386) | +0.235968 (.001094) | −0.203520 (.002991) |
| RANK2019_MANUFACTURING | +0.020160 (.884231) | +0.101289 (.373410) | −0.099925 (.321755) |

Principal profitability-interaction results:

| Sample | Period | Slope difference (p) | Severe-group slope (p) | VIF profitability | VIF interaction |
| --- | --- | ---: | ---: | ---: | ---: |
| RANK2019 | P2 | −0.153193 (.001097) | −0.120218 (.001363) | 1.909 | 1.828 |
| RANK2019 | P3 | +1.624456 (.000081) | +1.602435 (.000079) | 1.422 | 1.316 |
| RANK2019_MANUFACTURING | P2 | −0.080940 (.320542) | −0.133027 (.068116) | 1.823 | 1.603 |
| RANK2019_MANUFACTURING | P3 | +0.206862 (.010196) | +0.143884 (.057676) | 1.533 | 1.306 |

The manufacturing P3 severe-group slope changes sign relative to the superseded −0.235960 estimate; the earlier VIF above 2,300 disappears. The current slope is positive but imprecise at the 5% level, despite the slope difference being significant. The direction/precision also vary across thresholds: manufacturing severe slopes are +0.081888 (p=.226758), +0.143884 (p=.057676), and +0.180129 (p=.021863) at −15%, −20%, and −25%. They should not be described as uniformly precise.

Ranking P3 group effects remain negative across −15%/−20%/−25%: −0.100594 (p=.086873), −0.203520 (p=.002991), −0.137245 (p=.095355). Only −20% excludes zero at 5%; the earlier claim that all three are significant at 5% no longer applies. Manufacturing P3 group effects remain imprecise and change sign.

**Remaining influence limitation:** Globus sp. z o.o., Warszawa, NIP 7773261746, has a 2022 profit margin of 160.060010 and accounts for 99.56% of ranking P3 profitability variation after the requested exclusions. It is outside manufacturing. Ranking P3 profitability slopes reverse direction versus the previous sample and should not be described as broadly robust merely because their p-values are small or VIF is low. This firm was not added to the four authorised exclusions. Its current influence is reported in the supplementary workbook, without altering the requested exclusion policy.
