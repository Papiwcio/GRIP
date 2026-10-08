# Shared manual analytical exclusions

Updated 6 October 2026 after the user restored the four historical company exclusions, added Globus and Ordipol, and requested explicit reason codes. All OLS, quantile, severe-P1 and diagnostic results have been rerun for the six-company policy.

## Configuration and matching

`code_config.MANUAL_EXCLUSIONS` is the single configuration section. Its `enabled` switch defaults to `True`; `companies` lists the exact requested names, verified NIPs and `reason_code`. `MANUAL_EXCLUSION_REASONS` defines the corresponding descriptions. Missing or unknown reason codes fail validation. Every analytical loader applies exclusions before scenario selection, trajectory completeness, model-specific complete cases, outcome winsorisation, standardisation and regression fitting. The shared sample masks also enforce them, so direct callers cannot bypass them accidentally.

A row is excluded when its NIP matches a configured identifier **or** its company name exactly matches a configured name. Only outer whitespace is ignored; there is no substring, fuzzy or case-insensitive matching. NIP matching preserves the same firm exclusion if a company is renamed. Other Orlen-related companies are not excluded simply because their names contain “Orlen”. Missing values do not match an exclusion. Exclusions apply to entire firms in every period, model variant, quantile, supplementary threshold and diagnostic/trajectory output.

The six identifiers were checked against the current period input and the upstream removal rule. The current input contains 2,533 unique firms. Five are removed in memory, leaving 2,528 before trajectory/scenario filters:

| Configured company | Verified NIP | Reason code | Firms removed in this run | Status |
| --- | --- | --- | ---: | --- |
| Orlen SA GK, Płock | 7740001454 | M_AND_A | 0 | Already absent from input; upstream exclusion retained |
| Ignitis Polska sp. z o.o., Warszawa | 5252714003 | RANK2019_MISCLASSIFIED | 1 | Removed from analysis |
| Elektrobudowa SA w upadłości likwidacyjnej GK, Katowice | 6340135506 | LIQUIDATION | 1 | Removed from analysis |
| Zakłady Mięsne Henryk Kania SA w upadłości | 7440003325 | LIQUIDATION | 1 | Removed from analysis |
| Globus sp. z o.o., Warszawa | 7773261746 | HOLDING_NONCONSOLIDATED | 1 | Removed from analysis |
| Ordipol sp. z o.o. (w upadłości), Bielany Wrocławskie | 6772001669 | LIQUIDATION | 1 | Removed from analysis |

| Reason code | Description |
| --- | --- |
| M_AND_A | Non-comparable financial statements due to M&A activities. |
| LIQUIDATION | Non-comparable financial statements due to liquidation. |
| RANK2019_MISCLASSIFIED | Non-comparable financial statements: energy trading company incorrectly classified as eligible in the 2019 ranking. |
| HOLDING_NONCONSOLIDATED | Non-comparable financial statements: holding company reports non-consolidated financial statements. |

All exclusion reasons reflect the user's stated comparability/eligibility criteria. Globus is excluded because it is a holding company reporting non-consolidated financial statements, making its statements non-comparable. This supersedes the earlier provisional outlier-based reason. The earlier audit also found an extreme 2022 profit margin of 160.060010 (net profit 16,728.85280 / sales 104.51613), accounting for 99.56% of ranking P3 profitability variation before exclusion; that is a statistical finding, not the formal exclusion reason. Ordipol uses the same liquidation reason as Kania and Elektrobudowa; the exclusion applies to the whole firm across every analysis period, not only its 2024 observation.

The same names, NIPs, reason codes/descriptions, matched firm/row counts and statuses are printed at runtime and listed in each results workbook's existing README. A firm already absent upstream is not counted as newly removed. Manual removals are distinguished from subsequent missing-value exclusions.

As requested on 8 October 2026, the manual-exclusion block appears at the end of every results README as supplementary information. Analysis purpose, outcomes, specification and interpretation appear first. This presentation change does not alter exclusions, estimation samples or numerical results.

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
- Exact-name and NIP matching independently tested, enabled/disabled switching and unknown-reason rejection tested, other Orlen companies retained, all six configured firms absent from every analytical mask and firm-level trajectory export.
- All four saved README audits present; expected sheet orders and no Excel error cells verified. Independent QR reproduction of all 24 supplementary OLS coefficients/covariances and numerical checks of all 37 principal AMEs passed. All six actual-data Firth fits were reproduced with a separate BFGS optimiser.

The legacy archived OLS-reference comparison is not equal to the current ALL output; its sample/specification predates the current four-variant design and these exclusions. This is recorded as a historical comparison, not treated as a requirement to restore obsolete samples.

## Current samples

Complete nominal trajectory counts precede model-specific missing-value exclusions. OLS and quantile use the same complete-case N for each scenario-period and all their variants/quantiles:

| Scenario | Complete trajectory N | P1 N | P2 N | P3 N | FULL N |
| --- | ---: | ---: | ---: | ---: | ---: |
| ALL | 2,507 | 2,385 | 2,426 | 2,438 | 2,423 |
| MANUFACTURING | 1,007 | 979 | 995 | 1,000 | 995 |
| RANK2019 | 1,822 | 1,786 | 1,791 | 1,795 | 1,794 |
| RANK2019_MANUFACTURING | 786 | 781 | 782 | 783 | 784 |

Severe groups before model-specific exclusions:

| Sample | Below −15% | Below −20% | Below −25% |
| --- | ---: | ---: | ---: |
| RANK2019 (N=1,822) | 341 (18.72%) | 226 (12.40%) | 153 (8.40%) |
| RANK2019_MANUFACTURING (N=786) | 133 (16.92%) | 87 (11.07%) | 53 (6.74%) |

## Revised severe-P1 results

These coefficients use standardised growth outcomes and, for profitability slopes, the current estimation-sample profitability SD. Exclusions change both observations and scaling; direct comparison with previous standardised coefficients therefore is not a pure deletion with fixed units.

Principal −20% group coefficients and matched P2 diagnostic:

| Sample | P2 with lag (p) | P2 without lag (p) | P3 group effect (p) |
| --- | ---: | ---: | ---: |
| RANK2019 | −0.032640 (.709540) | +0.267927 (.000204) | −0.179089 (.008869) |
| RANK2019_MANUFACTURING | +0.020160 (.884231) | +0.101289 (.373410) | −0.099925 (.321755) |

Principal profitability-interaction results:

| Sample | Period | Slope difference (p) | Severe-group slope (p) | VIF profitability | VIF interaction |
| --- | --- | ---: | ---: | ---: | ---: |
| RANK2019 | P2 | −0.160495 (.000637) | −0.125326 (.000893) | 1.915 | 1.827 |
| RANK2019 | P3 | +0.039473 (.463051) | +0.110152 (.026012) | 1.421 | 1.340 |
| RANK2019_MANUFACTURING | P2 | −0.080940 (.320542) | −0.133027 (.068116) | 1.823 | 1.603 |
| RANK2019_MANUFACTURING | P3 | +0.206862 (.010196) | +0.143884 (.057676) | 1.533 | 1.306 |

The manufacturing P3 severe-group slope changes sign relative to the superseded −0.235960 estimate; the earlier VIF above 2,300 disappears. The current slope is positive but imprecise at the 5% level, despite the slope difference being significant. The direction/precision also vary across thresholds: manufacturing severe slopes are +0.081888 (p=.226758), +0.143884 (p=.057676), and +0.180129 (p=.021863) at −15%, −20%, and −25%. They should not be described as uniformly precise.

Ranking P3 group effects remain negative across −15%/−20%/−25%: −0.086993 (p=.137294), −0.179089 (p=.008869), −0.100013 (p=.224590). Only −20% excludes zero at 5%; the earlier pre-exclusion claim that all three are significant at 5% no longer applies. Manufacturing P3 group effects remain imprecise and change sign. Manufacturing estimates are unchanged by adding Globus and Ordipol, neither of which is a manufacturing firm. Ordipol was in each ranking complete-case sample, so excluding it reduces each ranking period's N by one.

**Influence after Ordipol's exclusion:** Its earlier 87.96% share of ranking P3 profitability variation is removed. The largest remaining observation accounts for 13.14%, with leverage .2144 and Cook's D .0160 in the principal model; profitability VIFs are 1.421/1.340. The severe-group slope is +0.110152 (p=.026012), but its difference from other firms is imprecise (+0.039473, p=.463051). These diagnostics address the earlier dominance; they do not establish causal interpretation or robustness to every specification. A liquidation reason code for specified firms does not imply automatically removing every other company with “w upadłości” in its name.
