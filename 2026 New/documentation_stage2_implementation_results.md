# Stage 2 implementation results


Created `code_common_samples.py`, `code_report_quality_samples.py`, `code_run_quantitative_pipeline.py`, `code_check_common_samples.py` and `code_check_common_sample_policy.py`. Updated OLS, interaction, quantile, diagnostic and severe runners, audit entry points, manual-exclusion/namespace checks and model-specific documentation. Production runners receive central membership; period-specific missing-value lists and independent routine selection are superseded.

Current ALL flow reconciles 2,533 firms: 5 firm-wide manual exclusions, 133 firms with required invalid observations, 32 lacking eligibility in every model, and 31 further common-alignment losses leave 2,332. Categories are exclusive and include pre-existing exclusions. Other populations reconcile independently in the central workbook. The unique company register has 201 rows; no source company is removed from either canonical dataset.

Validation: 11 policy tests, 9 compact-interaction tests, 8 active full-report tests, 23 independent data-quality tests including the archived-workbook check, 3 namespace/consolidated-register tests and 7 centring tests passed. Two optional pre-split migration tests were skipped because their unequal-sample reference is not applicable; matched-sample equivalence is tested instead. Manual NIP/exact-name matching and central reasons passed. Actual-company alignment, source/output hashes, ZIP/XML integrity, all populated tabs and absence of Excel error cells passed. Forty-eight matched-sample OLS/interaction fits reproduce the preserved engine exactly. All six severe Firth fits, 12 additive group fits and 37 principal marginal effects pass independent numerical checks. All 48 quantile fits converged without warnings.

The ten central sheets and the revised workflow/exclusion views were visually inspected. Original audits and every superseded analytical workbook are archived. Original data and builder logic remain unchanged.

Researcher attention: export reporting boundaries and small exceedances remain source questions; the approved [0,1] policy is not proof of an accounting error. Missing original ownership retains the documented Domestic mapping and review flag. Unit/reporting-boundary questions remain review findings. Common sampling conditions earlier analyses on later data availability; it does not remove selection or survivorship bias.

Final common company samples: {'ALL': 2332, 'MANUFACTURING': 949, 'RANK2019': 1748, 'RANK2019_MANUFACTURING': 754}

Unique consolidated excluded companies: 201. Annual and derived review findings remain separate from automatic eligibility exclusions.

| scenario               | period   | model             |   observations_old |   observations_new |   R_squared_old |   R_squared_new |
|:-----------------------|:---------|:------------------|-------------------:|-------------------:|----------------:|----------------:|
| ALL                    | P1       | P1_baseline       |               2385 |               2332 |       0.317355  |       0.258986  |
| ALL                    | P1       | P1_baseline_std   |               2385 |               2332 |       0.317355  |       0.258986  |
| ALL                    | P1       | P1_winsor         |               2385 |               2332 |       0.105172  |       0.0969741 |
| ALL                    | P1       | P1_winsor_std     |               2385 |               2332 |       0.105172  |       0.0969741 |
| ALL                    | P2       | P2_baseline       |               2426 |               2332 |       0.190952  |       0.164531  |
| ALL                    | P2       | P2_baseline_std   |               2426 |               2332 |       0.190952  |       0.164531  |
| ALL                    | P2       | P2_winsor         |               2426 |               2332 |       0.18749   |       0.169076  |
| ALL                    | P2       | P2_winsor_std     |               2426 |               2332 |       0.18749   |       0.169076  |
| ALL                    | P3       | P3_baseline       |               2438 |               2332 |       0.164765  |       0.249837  |
| ALL                    | P3       | P3_baseline_std   |               2438 |               2332 |       0.164765  |       0.249837  |
| ALL                    | P3       | P3_winsor         |               2438 |               2332 |       0.146333  |       0.150668  |
| ALL                    | P3       | P3_winsor_std     |               2438 |               2332 |       0.146333  |       0.150668  |
| ALL                    | FULL     | FULL_baseline     |               2423 |               2332 |       0.296484  |       0.235502  |
| ALL                    | FULL     | FULL_baseline_std |               2423 |               2332 |       0.296484  |       0.235502  |
| ALL                    | FULL     | FULL_winsor       |               2423 |               2332 |       0.276349  |       0.16913   |
| ALL                    | FULL     | FULL_winsor_std   |               2423 |               2332 |       0.276349  |       0.16913   |
| MANUFACTURING          | P1       | P1_baseline       |                979 |                949 |       0.116753  |       0.115763  |
| MANUFACTURING          | P1       | P1_baseline_std   |                979 |                949 |       0.116753  |       0.115763  |
| MANUFACTURING          | P1       | P1_winsor         |                979 |                949 |       0.143498  |       0.142638  |
| MANUFACTURING          | P1       | P1_winsor_std     |                979 |                949 |       0.143498  |       0.142638  |
| MANUFACTURING          | P2       | P2_baseline       |                995 |                949 |       0.294708  |       0.205924  |
| MANUFACTURING          | P2       | P2_baseline_std   |                995 |                949 |       0.294708  |       0.205924  |
| MANUFACTURING          | P2       | P2_winsor         |                995 |                949 |       0.202032  |       0.178912  |
| MANUFACTURING          | P2       | P2_winsor_std     |                995 |                949 |       0.202032  |       0.178912  |
| MANUFACTURING          | P3       | P3_baseline       |               1000 |                949 |       0.292287  |       0.312476  |
| MANUFACTURING          | P3       | P3_baseline_std   |               1000 |                949 |       0.292287  |       0.312476  |
| MANUFACTURING          | P3       | P3_winsor         |               1000 |                949 |       0.244704  |       0.277279  |
| MANUFACTURING          | P3       | P3_winsor_std     |               1000 |                949 |       0.244704  |       0.277279  |
| MANUFACTURING          | FULL     | FULL_baseline     |                995 |                949 |       0.381231  |       0.184366  |
| MANUFACTURING          | FULL     | FULL_baseline_std |                995 |                949 |       0.381231  |       0.184366  |
| MANUFACTURING          | FULL     | FULL_winsor       |                995 |                949 |       0.287879  |       0.187531  |
| MANUFACTURING          | FULL     | FULL_winsor_std   |                995 |                949 |       0.287879  |       0.187531  |
| RANK2019               | P1       | P1_baseline       |               1786 |               1748 |       0.0687635 |       0.073465  |
| RANK2019               | P1       | P1_baseline_std   |               1786 |               1748 |       0.0687635 |       0.073465  |
| RANK2019               | P1       | P1_winsor         |               1786 |               1748 |       0.0717586 |       0.073104  |
| RANK2019               | P1       | P1_winsor_std     |               1786 |               1748 |       0.0717586 |       0.073104  |
| RANK2019               | P2       | P2_baseline       |               1791 |               1748 |       0.104988  |       0.108144  |
| RANK2019               | P2       | P2_baseline_std   |               1791 |               1748 |       0.104988  |       0.108144  |
| RANK2019               | P2       | P2_winsor         |               1791 |               1748 |       0.137575  |       0.142635  |
| RANK2019               | P2       | P2_winsor_std     |               1791 |               1748 |       0.137575  |       0.142635  |
| RANK2019               | P3       | P3_baseline       |               1795 |               1748 |       0.14006   |       0.143441  |
| RANK2019               | P3       | P3_baseline_std   |               1795 |               1748 |       0.14006   |       0.143441  |
| RANK2019               | P3       | P3_winsor         |               1795 |               1748 |       0.184407  |       0.191884  |
| RANK2019               | P3       | P3_winsor_std     |               1795 |               1748 |       0.184407  |       0.191884  |
| RANK2019               | FULL     | FULL_baseline     |               1794 |               1748 |       0.0649695 |       0.0584249 |
| RANK2019               | FULL     | FULL_baseline_std |               1794 |               1748 |       0.0649695 |       0.0584249 |
| RANK2019               | FULL     | FULL_winsor       |               1794 |               1748 |       0.066243  |       0.0624263 |
| RANK2019               | FULL     | FULL_winsor_std   |               1794 |               1748 |       0.066243  |       0.0624263 |
| RANK2019_MANUFACTURING | P1       | P1_baseline       |                781 |                754 |       0.105838  |       0.105914  |
| RANK2019_MANUFACTURING | P1       | P1_baseline_std   |                781 |                754 |       0.105838  |       0.105914  |
| RANK2019_MANUFACTURING | P1       | P1_winsor         |                781 |                754 |       0.116721  |       0.116576  |
| RANK2019_MANUFACTURING | P1       | P1_winsor_std     |                781 |                754 |       0.116721  |       0.116576  |
| RANK2019_MANUFACTURING | P2       | P2_baseline       |                782 |                754 |       0.146566  |       0.153197  |
| RANK2019_MANUFACTURING | P2       | P2_baseline_std   |                782 |                754 |       0.146566  |       0.153197  |
| RANK2019_MANUFACTURING | P2       | P2_winsor         |                782 |                754 |       0.16982   |       0.179323  |
| RANK2019_MANUFACTURING | P2       | P2_winsor_std     |                782 |                754 |       0.16982   |       0.179323  |
| RANK2019_MANUFACTURING | P3       | P3_baseline       |                783 |                754 |       0.20137   |       0.197465  |
| RANK2019_MANUFACTURING | P3       | P3_baseline_std   |                783 |                754 |       0.20137   |       0.197465  |
| RANK2019_MANUFACTURING | P3       | P3_winsor         |                783 |                754 |       0.322656  |       0.332234  |
| RANK2019_MANUFACTURING | P3       | P3_winsor_std     |                783 |                754 |       0.322656  |       0.332234  |
| RANK2019_MANUFACTURING | FULL     | FULL_baseline     |                784 |                754 |       0.0659527 |       0.0666215 |
| RANK2019_MANUFACTURING | FULL     | FULL_baseline_std |                784 |                754 |       0.0659527 |       0.0666215 |
| RANK2019_MANUFACTURING | FULL     | FULL_winsor       |                784 |                754 |       0.103127  |       0.103999  |
| RANK2019_MANUFACTURING | FULL     | FULL_winsor_std   |                784 |                754 |       0.103127  |       0.103999  |

## RANK2019 winsor-standardised coefficient changes

| period   | raw_variable          |   coefficient_old |   coefficient_new |   p-value_old |   p-value_new |
|:---------|:----------------------|------------------:|------------------:|--------------:|--------------:|
| P1       | ln_sales_start_P1     |        0.0148886  |       0.0131089   |   0.531688    |   0.585635    |
| P1       | export_ratio_start_P1 |        0.00137467 |       0.000130748 |   0.958698    |   0.996092    |
| P2       | ln_sales_start_P2     |        0.0160112  |       0.0193927   |   0.503881    |   0.422835    |
| P2       | export_ratio_start_P2 |        0.0520486  |       0.0572219   |   0.0406123   |   0.0252      |
| P3       | ln_sales_start_P3     |        0.0202865  |       0.0266723   |   0.395598    |   0.268062    |
| P3       | export_ratio_start_P3 |       -0.111339   |      -0.10504     |   7.44317e-06 |   2.39886e-05 |
| FULL     | ln_sales_start_P1     |        0.0217479  |       0.0257822   |   0.360704    |   0.285955    |
| FULL     | export_ratio_start_P1 |       -0.09942    |      -0.0971709   |   0.000182214 |   0.000296283 |

Every recorded estimator and diagnostic uses the same company-ID set per population. Source datasets are unchanged. Coefficients and fit statistics change because company membership, sample means/SDs, interaction centring and within-sample winsor cutoffs change together under the existing formulas. Statistical specifications and estimation settings are unchanged. Independent matched-sample checks are documented in the validation tests.
