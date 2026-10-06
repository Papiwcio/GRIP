# Severe P1 Decline — methodological and technical audit

Audit date: 6 October 2026. The requested source name `run_severe_p1_decline_analysis.py` is not present; the actual maintained source is `code_severe_p1_decline.py`, following the agreed naming convention. The audited output is `Results_severe_P1_decline_analysis.xlsx`.

The original code and workbook were inspected and reproduced to temporary files before project changes. Findings were reported to the user before changes. Primary specifications, estimator choices, financial data, and main OLS outputs were preserved.

Reproduce the audit diagnostics with `python3 code_audit_severe_p1_decline.py`. Reproduce the numerical tests and supplementary workbook with `python3 code_check_severe_p1_decline.py`.

## A. Confirmed correct

### A1. Samples, IDs, thresholds and fixed membership

The input contains 2,533 rows and 2,533 unique, non-missing nip identifiers, with no duplicate IDs. Only RANK2019 and RANK2019_MANUFACTURING are estimated. Their masks exactly match the existing OLS ranking/manufacturing masks and nominal complete-trajectory requirement. Manufacturing is nested within ranking. The nominal price basis was retained. Group membership uses raw `ngrowth_P1`, not log growth or winsorised growth.

| sample | threshold | total_N | bottom_N | BottomP1 % |
| --- | --- | --- | --- | --- |
| RANK2019 | -0.15 | 1827 | 344 | 18.8287% |
| RANK2019 | -0.2 | 1827 | 229 | 12.5342% |
| RANK2019 | -0.25 | 1827 | 156 | 8.5386% |
| RANK2019_MANUFACTURING | -0.15 | 787 | 134 | 17.0267% |
| RANK2019_MANUFACTURING | -0.2 | 787 | 88 | 11.1817% |
| RANK2019_MANUFACTURING | -0.25 | 787 | 54 | 6.8615% |

Model-specific complete-case counts differ from the analytical counts above. These exclusions follow the corresponding main OLS model; severe groups are not redefined when observations are excluded. Each period uses the same observation IDs and predictor scaling at all thresholds.

| sample | period | N_entering | complete_case_N | dropped | Bottom15_N | Bottom20_N | Bottom25_N |
| --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | P1 | 1827 | 1790 | 37 | 334 | 222 | 149 |
| RANK2019 | P2 | 1827 | 1796 | 31 | 334 | 221 | 149 |
| RANK2019 | P3 | 1827 | 1800 | 27 | 336 | 222 | 150 |
| RANK2019_MANUFACTURING | P1 | 787 | 782 | 5 | 133 | 87 | 53 |
| RANK2019_MANUFACTURING | P2 | 787 | 783 | 4 | 133 | 87 | 54 |
| RANK2019_MANUFACTURING | P3 | 787 | 784 | 3 | 134 | 88 | 54 |

Strict `<` is implemented. At exactly −15%, Bottom15=0, Bottom20=0, Bottom25=0; at exactly −20%, Bottom15=1, Bottom20=0, Bottom25=0; at exactly −25%, Bottom15=1, Bottom20=1, Bottom25=0. Values immediately below/above each floating-point cutoff were tested. Missing or nonfinite P1 growth is unknown membership, not 0. There are no actual selected firms within 1e−12 of a cutoff. All P2/P3 indicators are carried forward from the same firm IDs and original P1 definitions.

### A2. Numerical correspondence with main OLS

The audit rebuilt the design matrices from raw columns and shared metadata, and reconstructed clipping and standardisation independently. It verified observation IDs, every predictor column and value, y values, clipping limits, production-reference sector coding, and covariance type against both code and saved main-OLS summaries. The primary supplement adds only the fixed BottomP1 indicator; interaction models additionally add standardised profitability × that indicator. Independent QR-based OLS reproduced every group and interaction coefficient and covariance matrix across all twelve sample × period × threshold combinations.

P2 dependent variable: `ngrowth_log_ann_P2 = [ln(sales_2022) − ln(sales_2020)]/2`. P3: `ngrowth_log_ann_P3 = [ln(sales_2024) − ln(sales_2022)]/2`. Both outcomes are winsorised at 1%/99% within their estimation sample, then standardised. Continuous predictors and the existing raw export-ratio × log-size product are standardised with ddof=0; ownership and sector dummies are unchanged. Profitability, asset turnover, sales per employee, and other predictors are **not winsorised**.

The actual shared covariance setting is **nonrobust**, not HC/cluster-robust. The supplement matches that setting; changing it would be a methodological change and was not done.

### A3. Profitability interaction units and combined inference

The fitted main-effect column retains its original name (`profit_margin_start_P2/P3`) but contains its within-estimation-sample z-score. The product uses that same fitted column multiplied by the fixed 0/1 indicator. It is not raw profitability × an independently scaled predictor, and the product is not restandardised. The variance is computed as a full linear contrast: Var(β1+β3)=Var(β1)+Var(β3)+2Cov(β1,β3); SE, Student-t statistic, p-value and 95% interval were reproduced independently from QR coefficients/covariances and checked against statsmodels t_test.

| sample | period | threshold | main_effect_column | main_effect_values | interaction_source_column | interaction_column | identical_units | combined_effect_valid | var_profitability | var_interaction | covariance_main_interaction | combined_coefficient | combined_variance | combined_SE | combined_p_value |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | P2 | 15 | profit_margin_start_P2 | Within-model-sample z-score | profit_margin_start_P2 | profitability_z_x_BottomP1_15 | Yes | Yes | 0.180854 | 0.178397 | -0.179186 | 0.157985 | 0.000879941 | 0.0296638 | 1.13322e-07 |
| RANK2019 | P2 | 20 | profit_margin_start_P2 | Within-model-sample z-score | profit_margin_start_P2 | profitability_z_x_BottomP1_20 | Yes | Yes | 0.162723 | 0.159831 | -0.160834 | 0.156942 | 0.000885464 | 0.0297567 | 1.49722e-07 |
| RANK2019 | P2 | 25 | profit_margin_start_P2 | Within-model-sample z-score | profit_margin_start_P2 | profitability_z_x_BottomP1_25 | Yes | Yes | 0.161213 | 0.158187 | -0.159258 | 0.151798 | 0.000882835 | 0.0297125 | 3.59001e-07 |
| RANK2019 | P3 | 15 | profit_margin_start_P3 | Within-model-sample z-score | profit_margin_start_P3 | profitability_z_x_BottomP1_15 | Yes | Yes | 0.000755873 | 0.00446692 | -0.000633227 | -0.163287 | 0.00395633 | 0.0628994 | 0.00950915 |
| RANK2019 | P3 | 20 | profit_margin_start_P3 | Within-model-sample z-score | profit_margin_start_P3 | profitability_z_x_BottomP1_20 | Yes | Yes | 0.00075265 | 0.00445366 | -0.00063079 | -0.164653 | 0.00394473 | 0.0628071 | 0.00882755 |
| RANK2019 | P3 | 25 | profit_margin_start_P3 | Within-model-sample z-score | profit_margin_start_P3 | profitability_z_x_BottomP1_25 | Yes | Yes | 0.000755235 | 0.00447681 | -0.000631231 | -0.159065 | 0.00396959 | 0.0630046 | 0.0116676 |
| RANK2019_MANUFACTURING | P2 | 15 | profit_margin_start_P2 | Within-model-sample z-score | profit_margin_start_P2 | profitability_z_x_BottomP1_15 | Yes | Yes | 2.08822 | 2.03887 | -2.06112 | 0.0951435 | 0.00484786 | 0.0696266 | 0.172191 |
| RANK2019_MANUFACTURING | P2 | 20 | profit_margin_start_P2 | Within-model-sample z-score | profit_margin_start_P2 | profitability_z_x_BottomP1_20 | Yes | Yes | 2.01558 | 1.96217 | -1.98645 | 0.0912656 | 0.00484849 | 0.0696311 | 0.190353 |
| RANK2019_MANUFACTURING | P2 | 25 | profit_margin_start_P2 | Within-model-sample z-score | profit_margin_start_P2 | profitability_z_x_BottomP1_25 | Yes | Yes | 1.97787 | 1.92243 | -1.9478 | 0.105452 | 0.00470877 | 0.0686205 | 0.124773 |
| RANK2019_MANUFACTURING | P3 | 15 | profit_margin_start_P3 | Within-model-sample z-score | profit_margin_start_P3 | profitability_z_x_BottomP1_15 | Yes | Yes | 2.13372 | 2.16675 | -2.14899 | -0.241269 | 0.00247884 | 0.049788 | 1.5265e-06 |
| RANK2019_MANUFACTURING | P3 | 20 | profit_margin_start_P3 | Within-model-sample z-score | profit_margin_start_P3 | profitability_z_x_BottomP1_20 | Yes | Yes | 2.05054 | 2.08431 | -2.06618 | -0.23596 | 0.00248807 | 0.0498805 | 2.66782e-06 |
| RANK2019_MANUFACTURING | P3 | 25 | profit_margin_start_P3 | Within-model-sample z-score | profit_margin_start_P3 | profitability_z_x_BottomP1_25 | Yes | Yes | 2.04974 | 2.08597 | -2.06659 | -0.232911 | 0.00251648 | 0.0501645 | 4.0428e-06 |

All twelve models have compatible units and mathematically valid combined effects. The manufacturing P3 principal example has β1=−2.800541, β3=2.564581 and β1+β3=−0.235960. Its covariance term is approximately −2.066182; this cancellation makes its combined SE about 0.049881. Combining individual SEs without covariance would be wrong; the code does not do that.

### A4. Standardisation provenance for every predictor/outcome

The following table records the actual raw variable, winsorised expression if applicable, retained model-column name, documented standardised symbol, mean, SD and sample N. The same scaling is reused across thresholds and group/interaction versions. It intentionally differs between samples and periods because main OLS standardises within each scenario-period complete-case sample. It never uses separate Bottom/non-Bottom scaling. Predictor columns retain source names despite containing standardised values; coefficient tables now state the scale explicitly. For outcome rows, mean/SD refer to the winsorised response before its z-score.

| sample | period | raw_variable | model_column | winsorised_variable | standardised_symbol | mean_raw | SD_raw_ddof0 | SD_divisor | N_scaling | model_mean | model_SD_ddof0 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | P1 | ln_sales_start_P1 | ln_sales_start_P1 | None: predictor not winsorised | z(ln_sales_start_P1) | 13.4663 | 0.854474 | 0.854474 | 1790 | 2.16338e-15 | 1 |
| RANK2019 | P1 | profit_margin_start_P1 | profit_margin_start_P1 | None: predictor not winsorised | z(profit_margin_start_P1) | 0.0378722 | 0.115376 | 0.115376 | 1790 | -4.36646e-17 | 1 |
| RANK2019 | P1 | export_ratio_start_P1 | export_ratio_start_P1 | None: predictor not winsorised | z(export_ratio_start_P1) | 0.311571 | 0.353417 | 0.353417 | 1790 | 0 | 1 |
| RANK2019 | P1 | asset_turnover_start_P1 | asset_turnover_start_P1 | None: predictor not winsorised | z(asset_turnover_start_P1) | 2.43205 | 2.88182 | 2.88182 | 1790 | 1.98476e-17 | 1 |
| RANK2019 | P1 | capital_ratio_start_P1 | capital_ratio_start_P1 | None: predictor not winsorised | z(capital_ratio_start_P1) | 0.422 | 0.275734 | 0.275734 | 1790 | -7.14512e-17 | 1 |
| RANK2019 | P1 | sales_per_employee_start_P1 | sales_per_employee_start_P1 | None: predictor not winsorised | z(sales_per_employee_start_P1) | 4617.02 | 22692.6 | 22692.6 | 1790 | 7.93902e-18 | 1 |
| RANK2019 | P1 | owner_num | owner_num | None: predictor not winsorised | owner_num | 0.481006 | 0.499639 | 1 | 1790 | 0.481006 | 0.499639 |
| RANK2019 | P1 | export_ratio_x_ln_sales_start_P1 | export_ratio_x_ln_sales_start_P1 | None: predictor not winsorised | z(export_ratio_x_ln_sales_start_P1) | 4.19295 | 4.76423 | 4.76423 | 1790 | 2.38171e-17 | 1 |
| RANK2019 | P1 | lag_ngrowth_log_ann_P1 | lag_ngrowth_log_ann_P1 | None: predictor not winsorised | z(lag_ngrowth_log_ann_P1) | 0.0828444 | 0.34542 | 0.34542 | 1790 | -1.5878e-17 | 1 |
| RANK2019 | P2 | ln_sales_start_P2 | ln_sales_start_P2 | None: predictor not winsorised | z(ln_sales_start_P2) | 13.4257 | 0.901251 | 0.901251 | 1796 | 1.61415e-15 | 1 |
| RANK2019 | P2 | profit_margin_start_P2 | profit_margin_start_P2 | None: predictor not winsorised | z(profit_margin_start_P2) | -0.00630316 | 1.587 | 1.587 | 1796 | 0 | 1 |
| RANK2019 | P2 | export_ratio_start_P2 | export_ratio_start_P2 | None: predictor not winsorised | z(export_ratio_start_P2) | 0.309036 | 0.353432 | 0.353432 | 1796 | 3.95625e-17 | 1 |
| RANK2019 | P2 | asset_turnover_start_P2 | asset_turnover_start_P2 | None: predictor not winsorised | z(asset_turnover_start_P2) | 2.29917 | 2.47552 | 2.47552 | 1796 | 1.14731e-16 | 1 |
| RANK2019 | P2 | capital_ratio_start_P2 | capital_ratio_start_P2 | None: predictor not winsorised | z(capital_ratio_start_P2) | 0.430581 | 0.337303 | 0.337303 | 1796 | -1.06819e-16 | 1 |
| RANK2019 | P2 | sales_per_employee_start_P2 | sales_per_employee_start_P2 | None: predictor not winsorised | z(sales_per_employee_start_P2) | 4842.71 | 29339.3 | 29339.3 | 1796 | 3.95625e-18 | 1 |
| RANK2019 | P2 | owner_num | owner_num | None: predictor not winsorised | owner_num | 0.482739 | 0.499702 | 1 | 1796 | 0.482739 | 0.499702 |
| RANK2019 | P2 | export_ratio_x_ln_sales_start_P2 | export_ratio_x_ln_sales_start_P2 | None: predictor not winsorised | z(export_ratio_x_ln_sales_start_P2) | 4.15214 | 4.75783 | 4.75783 | 1796 | 3.95625e-18 | 1 |
| RANK2019 | P2 | lag_ngrowth_log_ann_P2 | lag_ngrowth_log_ann_P2 | None: predictor not winsorised | z(lag_ngrowth_log_ann_P2) | -0.0360351 | 0.343846 | 0.343846 | 1796 | 6.42891e-18 | 1 |
| RANK2019 | P2 | ngrowth_log_ann_P2 | dependent growth response | winsor(ngrowth_log_ann_P2, .01, .99) | z(winsor(ngrowth_log_ann_P2)) | 0.180566 | 0.167953 | 0.167953 | 1796 | 1.93856e-16 | 1 |
| RANK2019 | P3 | ln_sales_start_P3 | ln_sales_start_P3 | None: predictor not winsorised | z(ln_sales_start_P3) | 13.7729 | 1.02868 | 1.02868 | 1800 | -6.71068e-16 | 1 |
| RANK2019 | P3 | profit_margin_start_P3 | profit_margin_start_P3 | None: predictor not winsorised | z(profit_margin_start_P3) | 0.169493 | 4.17369 | 4.17369 | 1800 | 5.92119e-18 | 1 |
| RANK2019 | P3 | export_ratio_start_P3 | export_ratio_start_P3 | None: predictor not winsorised | z(export_ratio_start_P3) | 0.311379 | 0.350492 | 0.350492 | 1800 | -2.76322e-17 | 1 |
| RANK2019 | P3 | asset_turnover_start_P3 | asset_turnover_start_P3 | None: predictor not winsorised | z(asset_turnover_start_P3) | 2.54079 | 2.92026 | 2.92026 | 1800 | -5.72382e-17 | 1 |
| RANK2019 | P3 | capital_ratio_start_P3 | capital_ratio_start_P3 | None: predictor not winsorised | z(capital_ratio_start_P3) | 0.419973 | 0.348841 | 0.348841 | 1800 | 1.02634e-16 | 1 |
| RANK2019 | P3 | sales_per_employee_start_P3 | sales_per_employee_start_P3 | None: predictor not winsorised | z(sales_per_employee_start_P3) | 7479.74 | 52424.7 | 52424.7 | 1800 | 7.89492e-18 | 1 |
| RANK2019 | P3 | owner_num | owner_num | None: predictor not winsorised | owner_num | 0.483889 | 0.49974 | 1 | 1800 | 0.483889 | 0.49974 |
| RANK2019 | P3 | export_ratio_x_ln_sales_start_P3 | export_ratio_x_ln_sales_start_P3 | None: predictor not winsorised | z(export_ratio_x_ln_sales_start_P3) | 4.29602 | 4.83254 | 4.83254 | 1800 | -9.4739e-17 | 1 |
| RANK2019 | P3 | lag_ngrowth_log_ann_P3 | lag_ngrowth_log_ann_P3 | None: predictor not winsorised | z(lag_ngrowth_log_ann_P3) | 0.173951 | 0.246595 | 0.246595 | 1800 | -4.93432e-17 | 1 |
| RANK2019 | P3 | ngrowth_log_ann_P3 | dependent growth response | winsor(ngrowth_log_ann_P3, .01, .99) | z(winsor(ngrowth_log_ann_P3)) | -0.0288002 | 0.18767 | 0.18767 | 1800 | -3.15797e-17 | 1 |
| RANK2019_MANUFACTURING | P1 | ln_sales_start_P1 | ln_sales_start_P1 | None: predictor not winsorised | z(ln_sales_start_P1) | 13.4195 | 0.753813 | 0.753813 | 782 | -1.39474e-15 | 1 |
| RANK2019_MANUFACTURING | P1 | profit_margin_start_P1 | profit_margin_start_P1 | None: predictor not winsorised | z(profit_margin_start_P1) | 0.0448101 | 0.11101 | 0.11101 | 782 | -5.90605e-17 | 1 |
| RANK2019_MANUFACTURING | P1 | export_ratio_start_P1 | export_ratio_start_P1 | None: predictor not winsorised | z(export_ratio_start_P1) | 0.500944 | 0.351286 | 0.351286 | 782 | -4.54311e-18 | 1 |
| RANK2019_MANUFACTURING | P1 | asset_turnover_start_P1 | asset_turnover_start_P1 | None: predictor not winsorised | z(asset_turnover_start_P1) | 1.68522 | 0.949755 | 0.949755 | 782 | 2.3397e-16 | 1 |
| RANK2019_MANUFACTURING | P1 | capital_ratio_start_P1 | capital_ratio_start_P1 | None: predictor not winsorised | z(capital_ratio_start_P1) | 0.512493 | 0.278 | 0.278 | 782 | 9.54054e-17 | 1 |
| RANK2019_MANUFACTURING | P1 | sales_per_employee_start_P1 | sales_per_employee_start_P1 | None: predictor not winsorised | z(sales_per_employee_start_P1) | 1466.84 | 2114.8 | 2114.8 | 782 | 4.54311e-18 | 1 |
| RANK2019_MANUFACTURING | P1 | owner_num | owner_num | None: predictor not winsorised | owner_num | 0.575448 | 0.494275 | 1 | 782 | 0.575448 | 0.494275 |
| RANK2019_MANUFACTURING | P1 | export_ratio_x_ln_sales_start_P1 | export_ratio_x_ln_sales_start_P1 | None: predictor not winsorised | z(export_ratio_x_ln_sales_start_P1) | 6.74533 | 4.76216 | 4.76216 | 782 | -8.1776e-17 | 1 |
| RANK2019_MANUFACTURING | P1 | lag_ngrowth_log_ann_P1 | lag_ngrowth_log_ann_P1 | None: predictor not winsorised | z(lag_ngrowth_log_ann_P1) | 0.0536995 | 0.211548 | 0.211548 | 782 | 4.54311e-18 | 1 |
| RANK2019_MANUFACTURING | P2 | ln_sales_start_P2 | ln_sales_start_P2 | None: predictor not winsorised | z(ln_sales_start_P2) | 13.3824 | 0.800035 | 0.800035 | 783 | 6.94208e-16 | 1 |
| RANK2019_MANUFACTURING | P2 | profit_margin_start_P2 | profit_margin_start_P2 | None: predictor not winsorised | z(profit_margin_start_P2) | -0.0399755 | 2.39751 | 2.39751 | 783 | 4.53731e-18 | 1 |
| RANK2019_MANUFACTURING | P2 | export_ratio_start_P2 | export_ratio_start_P2 | None: predictor not winsorised | z(export_ratio_start_P2) | 0.496146 | 0.351877 | 0.351877 | 783 | 1.15701e-16 | 1 |
| RANK2019_MANUFACTURING | P2 | asset_turnover_start_P2 | asset_turnover_start_P2 | None: predictor not winsorised | z(asset_turnover_start_P2) | 1.59692 | 0.950904 | 0.950904 | 783 | 1.13433e-17 | 1 |
| RANK2019_MANUFACTURING | P2 | capital_ratio_start_P2 | capital_ratio_start_P2 | None: predictor not winsorised | z(capital_ratio_start_P2) | 0.511242 | 0.393975 | 0.393975 | 783 | 1.19104e-16 | 1 |
| RANK2019_MANUFACTURING | P2 | sales_per_employee_start_P2 | sales_per_employee_start_P2 | None: predictor not winsorised | z(sales_per_employee_start_P2) | 1545.93 | 4476.64 | 4476.64 | 783 | 3.62985e-17 | 1 |
| RANK2019_MANUFACTURING | P2 | owner_num | owner_num | None: predictor not winsorised | owner_num | 0.574713 | 0.494387 | 1 | 783 | 0.574713 | 0.494387 |
| RANK2019_MANUFACTURING | P2 | export_ratio_x_ln_sales_start_P2 | export_ratio_x_ln_sales_start_P2 | None: predictor not winsorised | z(export_ratio_x_ln_sales_start_P2) | 6.66158 | 4.75365 | 4.75365 | 783 | 2.88119e-16 | 1 |
| RANK2019_MANUFACTURING | P2 | lag_ngrowth_log_ann_P2 | lag_ngrowth_log_ann_P2 | None: predictor not winsorised | z(lag_ngrowth_log_ann_P2) | -0.0369365 | 0.273002 | 0.273002 | 783 | -6.80596e-18 | 1 |
| RANK2019_MANUFACTURING | P2 | ngrowth_log_ann_P2 | dependent growth response | winsor(ngrowth_log_ann_P2, .01, .99) | z(winsor(ngrowth_log_ann_P2)) | 0.190857 | 0.135019 | 0.135019 | 783 | -1.58806e-16 | 1 |
| RANK2019_MANUFACTURING | P3 | ln_sales_start_P3 | ln_sales_start_P3 | None: predictor not winsorised | z(ln_sales_start_P3) | 13.7573 | 0.903101 | 0.903101 | 784 | -9.78809e-16 | 1 |
| RANK2019_MANUFACTURING | P3 | profit_margin_start_P3 | profit_margin_start_P3 | None: predictor not winsorised | z(profit_margin_start_P3) | 0.143867 | 2.69005 | 2.69005 | 784 | -1.81261e-17 | 1 |
| RANK2019_MANUFACTURING | P3 | export_ratio_start_P3 | export_ratio_start_P3 | None: predictor not winsorised | z(export_ratio_start_P3) | 0.492985 | 0.35145 | 0.35145 | 784 | -6.79728e-17 | 1 |
| RANK2019_MANUFACTURING | P3 | asset_turnover_start_P3 | asset_turnover_start_P3 | None: predictor not winsorised | z(asset_turnover_start_P3) | 1.8231 | 1.13774 | 1.13774 | 784 | 1.13288e-16 | 1 |
| RANK2019_MANUFACTURING | P3 | capital_ratio_start_P3 | capital_ratio_start_P3 | None: predictor not winsorised | z(capital_ratio_start_P3) | 0.48883 | 0.419838 | 0.419838 | 784 | -1.11589e-16 | 1 |
| RANK2019_MANUFACTURING | P3 | sales_per_employee_start_P3 | sales_per_employee_start_P3 | None: predictor not winsorised | z(sales_per_employee_start_P3) | 2412.7 | 6608.58 | 6608.58 | 784 | 2.71891e-17 | 1 |
| RANK2019_MANUFACTURING | P3 | owner_num | owner_num | None: predictor not winsorised | owner_num | 0.575255 | 0.494304 | 1 | 784 | 0.575255 | 0.494304 |
| RANK2019_MANUFACTURING | P3 | export_ratio_x_ln_sales_start_P3 | export_ratio_x_ln_sales_start_P3 | None: predictor not winsorised | z(export_ratio_x_ln_sales_start_P3) | 6.80124 | 4.86335 | 4.86335 | 784 | -2.17513e-16 | 1 |
| RANK2019_MANUFACTURING | P3 | lag_ngrowth_log_ann_P3 | lag_ngrowth_log_ann_P3 | None: predictor not winsorised | z(lag_ngrowth_log_ann_P3) | 0.187921 | 0.169419 | 0.169419 | 784 | 0 | 1 |
| RANK2019_MANUFACTURING | P3 | ngrowth_log_ann_P3 | dependent growth response | winsor(ngrowth_log_ann_P3, .01, .99) | z(winsor(ngrowth_log_ann_P3)) | -0.0478483 | 0.154839 | 0.154839 | 784 | -4.53152e-18 | 1 |

### A5. Sector reference and missing categories

Production is the omitted/reference sector in every model. The categories below apply identically at all three thresholds and to the group and interaction versions. Categories absent from the manufacturing sample relative to ranking are energy; media/telecommunications/IT; retail trade; transport; wholesale trade. No observed selected-sample sector disappears after complete-case filtering in these data.

| sample | period | reference | included_sectors | sectors_absent_from_estimation |
| --- | --- | --- | --- | --- |
| RANK2019 | P1 | production | automotive; chemicals; construction; energy; food; fuels; health and pharma; media, telecommunications, and IT; mining and metallurgy; retail trade; services; transport; wholesale trade | None |
| RANK2019 | P2 | production | automotive; chemicals; construction; energy; food; fuels; health and pharma; media, telecommunications, and IT; mining and metallurgy; retail trade; services; transport; wholesale trade | None |
| RANK2019 | P3 | production | automotive; chemicals; construction; energy; food; fuels; health and pharma; media, telecommunications, and IT; mining and metallurgy; retail trade; services; transport; wholesale trade | None |
| RANK2019_MANUFACTURING | P1 | production | automotive; chemicals; construction; food; fuels; health and pharma; mining and metallurgy; services | None |
| RANK2019_MANUFACTURING | P2 | production | automotive; chemicals; construction; food; fuels; health and pharma; mining and metallurgy; services | None |
| RANK2019_MANUFACTURING | P3 | production | automotive; chemicals; construction; food; fuels; health and pharma; mining and metallurgy; services | None |

### A6. Logistic estimates, confidence intervals and AMEs

All six Firth coefficient vectors were reproduced using a separate BFGS optimiser of the penalised likelihood; maximum absolute differences were below 5.2e−7. Closed-form add-half estimates for a separated two-group model and numerical derivatives of the adjusted score were also checked. Odds ratios equal exp(coefficient); both odds-ratio confidence limits equal exponentiated coefficient confidence limits.

The implementation is project-local adjusted-score Fisher scoring with backtracking, using NumPy/SciPy, not execution of the R logistf package. Coefficient covariance is inverse **original expected Fisher information**, with approximate normal Wald inference. This is neither ordinary-MLE fitting nor profile-penalised inference. Current logistf versions have their own augmented-data covariance implementation; this project does not claim numerical equivalence to that package. Wald and delta-method intervals should be treated cautiously in sparse/separated categories.

All 37 principal AMEs, SEs and p-values were independently reproduced from perturbed raw-predictor probabilities and numerical parameter gradients: maximum AME difference 5.7e−13, SE difference 7.1e−11 and p-value difference 1.3e−9. Continuous effects are average infinitesimal derivatives in one-SD units, including the chain rule for the existing export-size product. Ownership is an average discrete 0→1 change. Sectors are mutually exclusive category-versus-production counterfactuals, with all other sector dummies set to zero. Their delta-method covariance is appropriate to the stated approximate Fisher-information inference. Wald tests are not invariant to nonlinear transformations: coefficient and discrete-AME p-values need not coincide, particularly for sparse categories; significance should not be selected opportunistically between those scales.

Runtime checked: Python 3.12.3, NumPy 2.4.4, pandas 3.0.2, SciPy 1.17.1, statsmodels 0.14.6. Method references: [logistf fitting and Wald/profile inference](https://search.r-project.org/CRAN/refmans/logistf/html/logistf.html), [current package covariance caveat](https://search.r-project.org/CRAN/refmans/logistf/html/logistf-package.html), [penalised LR restriction](https://raw.githubusercontent.com/cran/logistf/master/R/logistftest.R).

### A7. Original workbook integrity

All eighteen original Excel Tables and their results were reproduced and checked. No numerical mismatches, duplicated table rows, stale result cells, formula errors or Excel error cells were found. The sole raw comparison difference was the deliberately changed temporary output filename in the audit reproduction README, excluded from substantive matching. There were no formulas in the original file; output cells are reproducible saved estimates. Seven numeric-looking strings were categorical 0/1 codes, not numerical result cells. Nominal growth labels and typed rate/number formats were checked. Eight supplementary tabs were retained after corrections; requested diagnostics extend existing tabs.

## B. Problems found

### B1. Extreme manufacturing P3 profitability — genuine fit, fragile inference

The suspicious coefficients reproduce exactly. There is no scaling mismatch, sample change, response-preprocessing change or interaction coding error. The data contain a 2022 profit margin of 75.392857 for nip 7440003325, Zakłady Mięsne Henryk Kania SA w upadłości: net_profit=2111, sales=28, so 2111/28=75.392857. This is the canonical ratio, not a failed division or a newly introduced value. It reflects a tiny nonzero sales denominator. Whether the underlying financial observation has the intended economic interpretation needs source-data review; it was not changed.

This firm is in every severe group and accounts for 99.8079% of manufacturing P3 profitability’s centred sum of squares. In the principal interaction model, its leverage is 0.999902 and Cook’s distance approximately 1989.15. Non-Bottom profitability SD is only 0.024448 in whole-sample z units, versus 2.969493 within BottomP1. The full-sample one-SD scale is 2.690049 raw profit-margin units, far beyond typical non-Bottom variation. Consequently the large non-Bottom slope is expressed in an outlier-dominated unit and estimated alongside an almost collinear product. The matrix has full rank (20/20), so this is not a singular-matrix coding failure.

| sample | period | threshold | VIF_profitability | VIF_profitability_interaction | condition_number | matrix_rank | parameters | profit_SD_z_other | profit_SD_z_bottom | outlier_nip | outlier_profit_SS_share | outlier_leverage | outlier_Cooks_D |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | P2 | 20 | 335.428 | 327.909 | 42.8016 | 25 | 25 | 0.0617438 | 2.83739 | 7440003325 | 0.993763 | 0.997998 | 232.886 |
| RANK2019 | P3 | 20 | 1.61915 | 1.75869 | 30.5598 | 25 | 25 | 0.965007 | 1.21937 | 7773261746 | 0.815328 | 0.999675 | 646.29 |
| RANK2019_MANUFACTURING | P2 | 20 | 1870.84 | 1815.52 | 111.106 | 20 | 20 | 0.0281047 | 2.98138 | 7440003325 | 0.997758 | 0.9998 | 883.289 |
| RANK2019_MANUFACTURING | P3 | 20 | 2339.86 | 2372.24 | 123.485 | 20 | 20 | 0.024448 | 2.96949 | 7440003325 | 0.998079 | 0.999902 | 1989.15 |

An audit-only delete-one diagnostic, holding the original scaling fixed, changes the manufacturing P3 severe-group profitability slope from −0.235960 to +5.640997 (SE=2.968047). The primary data/model retain the firm. Thus the original coefficients are algebraically valid, but their apparent highly precise severe-group profitability relationship is influence-sensitive, not established robust heterogeneity. P2 profitability interactions also have severe collinearity (VIFs around 335 in ranking and 1870 in manufacturing). Ranking P3 has low profitability VIF (~1.62/1.76), yet still contains large financial-ratio observations; threshold stability alone cannot establish immunity to outliers.

### B2. Logistic estimator rationale and sector diagnostic

Ordinary MLE converges normally for ranking at −15%, −20%, and −25%, and linear-programming tests find neither complete nor quasi-complete separation. Firth is not technically necessary for ranking; it was deliberately used for both samples for estimator consistency. Manufacturing has quasi-complete, not globally complete, separation at all thresholds. Health/pharma has 15 observations and zero events; all other manufacturing sectors contain both response classes.

| sample | threshold | N | bottom_N | separation | single_class_sectors | estimator_used |
| --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | 15 | 1790 | 334 | None | None | Firth (primary model retained) |
| RANK2019 | 20 | 1790 | 222 | None | None | Firth (primary model retained) |
| RANK2019 | 25 | 1790 | 149 | None | None | Firth (primary model retained) |
| RANK2019_MANUFACTURING | 15 | 782 | 133 | Quasi-complete | health and pharma (N=15, severe=0) | Firth (primary model retained) |
| RANK2019_MANUFACTURING | 20 | 782 | 87 | Quasi-complete | health and pharma (N=15, severe=0) | Firth (primary model retained) |
| RANK2019_MANUFACTURING | 25 | 782 | 53 | Quasi-complete | health and pharma (N=15, severe=0) | Firth (primary model retained) |

| sample | sector | N_total | Bottom15_N | Bottom20_N | Bottom25_N |
| --- | --- | --- | --- | --- | --- |
| RANK2019_MANUFACTURING | automotive | 126 | 55 | 34 | 20 |
| RANK2019_MANUFACTURING | chemicals | 26 | 2 | 2 | 1 |
| RANK2019_MANUFACTURING | construction | 72 | 2 | 1 | 1 |
| RANK2019_MANUFACTURING | food | 174 | 14 | 11 | 7 |
| RANK2019_MANUFACTURING | fuels | 9 | 2 | 2 | 1 |
| RANK2019_MANUFACTURING | health and pharma | 15 | 0 | 0 | 0 |
| RANK2019_MANUFACTURING | mining and metallurgy | 23 | 9 | 4 | 2 |
| RANK2019_MANUFACTURING | production | 322 | 42 | 29 | 18 |
| RANK2019_MANUFACTURING | services | 20 | 8 | 5 | 4 |

The original ranking `single_class_sectors` cell was an empty string (Q7); the manufacturing cell Q8 was `health and pharma`. There is no literal numeric/string 159 anywhere in the original saved workbook. It is not a sector identifier or index in the actual diagnostic. Its external origin cannot be established from this file. Empty diagnostics were ambiguous, so the ranking entry now explicitly displays `None`; the appended sector-count and separation tables use sector names.

Ordinary-versus-Firth comparison for principal ranking follows. Point estimates retain their substantive directions. Capital-ratio evidence is similar (coefficient p=.0133 MLE versus .0115 Firth); foreign ownership remains weak (about .096). Asset turnover and health/pharma coefficient tests cross the conventional .10 threshold between estimators, so these are borderline findings, not stable discoveries.

| sample | variable | coefficient_MLE | std_error_MLE | p_value_MLE | odds_ratio_MLE | coefficient_Firth | std_error_Firth | p_value_Firth | odds_ratio_Firth | AME_MLE | AME_p_value_MLE | AME_Firth | AME_p_value_Firth |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | const | -2.35349 | 0.210173 | 4.17759e-29 | 0.0950372 | -2.32867 | 0.207782 | 3.75525e-29 | 0.0974255 | — | — | — | — |
| RANK2019 | ln_sales_start_P1 | -0.0278852 | 0.0937296 | 0.76608 | 0.9725 | -0.0250635 | 0.0921687 | 0.785675 | 0.975248 | -0.00407869 | 0.597328 | -0.00371219 | 0.634547 |
| RANK2019 | profit_margin_start_P1 | -0.12495 | 0.0827162 | 0.130893 | 0.882541 | -0.106906 | 0.079093 | 0.176486 | 0.89861 | -0.0128593 | 0.130593 | -0.0113196 | 0.176182 |
| RANK2019 | export_ratio_start_P1 | 0.268996 | 1.1441 | 0.814119 | 1.30865 | 0.240405 | 1.12742 | 0.831144 | 1.27176 | 0.00681373 | 0.429229 | 0.00713004 | 0.415129 |
| RANK2019 | asset_turnover_start_P1 | 0.107971 | 0.0655092 | 0.0993149 | 1.11402 | 0.104697 | 0.0651446 | 0.108024 | 1.11037 | 0.0111119 | 0.0988309 | 0.0110856 | 0.107506 |
| RANK2019 | capital_ratio_start_P1 | -0.215612 | 0.0870847 | 0.0132906 | 0.806048 | -0.217182 | 0.0859217 | 0.0114822 | 0.804784 | -0.0221898 | 0.0132451 | -0.0229959 | 0.0114169 |
| RANK2019 | sales_per_employee_start_P1 | 0.0518191 | 0.0656642 | 0.430023 | 1.05319 | 0.0413741 | 0.0636484 | 0.515666 | 1.04224 | 0.00533299 | 0.4298 | 0.00438082 | 0.515514 |
| RANK2019 | owner_num | 0.266871 | 0.160161 | 0.0956608 | 1.30587 | 0.26226 | 0.157736 | 0.0963834 | 1.29986 | 0.027497 | 0.0957741 | 0.0278037 | 0.0965029 |
| RANK2019 | export_ratio_x_ln_sales_start_P1 | -0.202614 | 1.14621 | 0.85969 | 0.816593 | -0.17289 | 1.12948 | 0.878343 | 0.84123 | — | — | — | — |
| RANK2019 | lag_ngrowth_log_ann_P1 | 0.00624442 | 0.0705155 | 0.929437 | 1.00626 | 0.0115223 | 0.0687731 | 0.866944 | 1.01159 | 0.000642648 | 0.929438 | 0.00122002 | 0.866949 |
| RANK2019 | sector_en_automotive | 0.778029 | 0.252505 | 0.00206139 | 2.17718 | 0.772828 | 0.250454 | 0.00203066 | 2.16588 | 0.0934427 | 0.00280888 | 0.0941186 | 0.00277019 |
| RANK2019 | sector_en_chemicals | 0.0660789 | 0.48713 | 0.892098 | 1.06831 | 0.134218 | 0.472375 | 0.776308 | 1.14364 | 0.0060644 | 0.893986 | 0.0128688 | 0.783863 |
| RANK2019 | sector_en_construction | 0.222494 | 0.300448 | 0.458973 | 1.24919 | 0.228933 | 0.297052 | 0.440895 | 1.25726 | 0.0216977 | 0.467445 | 0.0227666 | 0.449687 |
| RANK2019 | sector_en_energy | 0.685202 | 0.55944 | 0.220651 | 1.98417 | 0.763049 | 0.545406 | 0.161799 | 2.14481 | 0.0795735 | 0.306269 | 0.0926042 | 0.248963 |
| RANK2019 | sector_en_food | -0.733361 | 0.387196 | 0.058221 | 0.480292 | -0.696731 | 0.378562 | 0.0656993 | 0.498211 | -0.0493353 | 0.036393 | -0.0484067 | 0.043245 |
| RANK2019 | sector_en_fuels | 0.927641 | 0.376349 | 0.0137072 | 2.52854 | 0.952041 | 0.372593 | 0.0106133 | 2.59099 | 0.117451 | 0.0365387 | 0.123427 | 0.0304535 |
| RANK2019 | sector_en_health and pharma | -1.11009 | 0.625531 | 0.0759572 | 0.329528 | -0.946587 | 0.580755 | 0.103117 | 0.388063 | -0.0648399 | 0.0156367 | -0.0598399 | 0.0347599 |
| RANK2019 | sector_en_media, telecommunications, and IT | -0.233366 | 0.416495 | 0.575268 | 0.791863 | -0.19049 | 0.406603 | 0.639434 | 0.826554 | -0.0190524 | 0.555682 | -0.0160977 | 0.625638 |
| RANK2019 | sector_en_mining and metallurgy | 0.856819 | 0.491511 | 0.081293 | 2.35565 | 0.897837 | 0.482157 | 0.0625856 | 2.45429 | 0.105831 | 0.158386 | 0.114252 | 0.133659 |
| RANK2019 | sector_en_retail trade | 0.697626 | 0.373602 | 0.0618599 | 2.00898 | 0.710593 | 0.369435 | 0.0544225 | 2.0352 | 0.0813844 | 0.10017 | 0.0846293 | 0.0906551 |
| RANK2019 | sector_en_services | 0.832986 | 0.319249 | 0.00907529 | 2.30018 | 0.84117 | 0.316249 | 0.00781789 | 2.31908 | 0.102024 | 0.0197209 | 0.104948 | 0.0174613 |
| RANK2019 | sector_en_transport | 0.451009 | 0.343999 | 0.189832 | 1.5699 | 0.464483 | 0.339782 | 0.171624 | 1.59119 | 0.0480034 | 0.224697 | 0.0505206 | 0.2062 |
| RANK2019 | sector_en_wholesale trade | -0.350252 | 0.34785 | 0.31398 | 0.70451 | -0.32015 | 0.342318 | 0.349664 | 0.72604 | -0.0273198 | 0.294936 | -0.0257227 | 0.332303 |

**Recommendation A for a future specification decision:** ordinary MLE for the primary ranking sample, Firth for manufacturing. Ranking has no separation and this restores the originally expected estimator policy; secondary bias reduction addresses a demonstrated issue. Trade-off: estimators differ across samples. **Option B**, the current common-Firth policy, is defensible for consistency but penalises ranking unnecessarily and does not remove sparse-data inference limitations. No estimator was changed in this audit because that is methodological, not a confirmed coding error. The workbook adds the MLE comparison as a diagnostic only.

### B3. Labels and missing diagnostic provenance

The original fitted columns retained raw-looking source names despite containing z-scores, making the valid interaction easy to misread. Continuous AMEs were labelled “One SD increase”, which could be mistaken for a finite probability change; the method is a local derivative. Sample display names differed from the main uppercase scenario names. The original file did not include the specifically requested no-lag P2 sensitivity, subgroup variation/VIF diagnostics, or tail distribution checks. These are reporting/diagnostic gaps; they did not change existing numerical fits. The nip validation checked duplicates but did not explicitly reject missing IDs; no IDs were actually missing.

### B4. Raw descriptive means and inference limitations

Descriptive means are raw and correctly calculated. There are no p-values in the original group-profile table to validate, and none were invented. Some means are highly distorted: non-Bottom ranking P1 mean 71.57% versus median 1.27%, maximum 106,336.59%; excluding the largest observation diagnostically gives mean 5.03%. Non-Bottom ranking P3 mean 10.39% versus median 0.34%, maximum 13,696.91%; excluding that largest observation gives 1.82%. This does not justify silently winsorising the descriptive table. It is flagged using appended quantiles and maxima. P2/P3 simple growth is cumulative over each two-year interval; regression outcomes instead use annualised log growth.

Nonrobust OLS SEs, approximate Firth Wald/delta inference, and plug-in AIC/BIC are methodological limitations explicitly retained to avoid redesign. AIC/BIC at bias-reduced coefficients are descriptive, not conventional MLE criteria; thresholds define different responses and should not be selected using such comparisons. Complete-trajectory sampling excludes firms with missing later outcomes. None of the group coefficients or no-lag sensitivity coefficients identifies a causal recovery effect.

| sample | group | period | N | unit | mean | median | p05 | p95 | maximum | max_contribution_to_mean | mean_without_largest |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | BottomP1_20 = 0 | P1 | 1598 | rate | 0.715742 | 0.0126727 | -0.164613 | 0.321777 | 1063.37 | 0.665435 | 0.0503376 |
| RANK2019 | BottomP1_20 = 0 | P2 | 1598 | rate | 0.507337 | 0.40019 | -0.107831 | 1.31495 | 30.8273 | 0.0192912 | 0.488351 |
| RANK2019 | BottomP1_20 = 0 | P3 | 1598 | rate | 0.103943 | 0.00339816 | -0.45575 | 0.49722 | 136.969 | 0.0857129 | 0.0182417 |
| RANK2019 | BottomP1_20 = 1 | P1 | 229 | rate | -0.369668 | -0.307821 | -0.783966 | -0.204681 | -0.200841 | -0.000877037 | -0.370408 |
| RANK2019 | BottomP1_20 = 1 | P2 | 229 | rate | 0.919951 | 0.587765 | -0.67038 | 3.53578 | 18.8365 | 0.0822553 | 0.84137 |
| RANK2019 | BottomP1_20 = 1 | P3 | 229 | rate | -0.00775521 | -0.0322969 | -0.81022 | 0.699629 | 3.70524 | 0.0161801 | -0.0240403 |
| RANK2019_MANUFACTURING | BottomP1_20 = 0 | P1 | 699 | rate | 0.0256432 | -0.00200097 | -0.159793 | 0.28358 | 2.13643 | 0.00305641 | 0.0226191 |
| RANK2019_MANUFACTURING | BottomP1_20 = 0 | P2 | 699 | rate | 0.527584 | 0.446802 | -0.000689557 | 1.30843 | 7.54944 | 0.0108003 | 0.517524 |
| RANK2019_MANUFACTURING | BottomP1_20 = 0 | P3 | 699 | rate | -0.0345421 | -0.0731999 | -0.45713 | 0.406124 | 5.39892 | 0.00772378 | -0.0423264 |
| RANK2019_MANUFACTURING | BottomP1_20 = 1 | P1 | 88 | rate | -0.311917 | -0.272688 | -0.498478 | -0.203462 | -0.201542 | -0.00229025 | -0.313185 |
| RANK2019_MANUFACTURING | BottomP1_20 = 1 | P2 | 88 | rate | 0.55137 | 0.507559 | -0.293004 | 1.74134 | 3.20963 | 0.036473 | 0.520815 |
| RANK2019_MANUFACTURING | BottomP1_20 = 1 | P3 | 88 | rate | -0.0200292 | -0.053893 | -0.516663 | 0.50439 | 3.70524 | 0.042105 | -0.0628484 |

## C. Corrections made

- `code_severe_p1_decline.py`: reject missing nip IDs; state predictor units in coefficient tables; label continuous AMEs as local derivatives; make no-single-class-sector diagnostics explicit; display sample names as RANK2019/RANK2019_MANUFACTURING; append audit and interpretation warnings. Corrected the overly broad statement that Firth necessarily moves every fitted probability toward one half. Added only the requested diagnostic fits/tables; all original primary/threshold coefficients remain unchanged.
- `code_audit_severe_p1_decline.py`: reproducible complete-case/scaling provenance, separation LPs, ordinary ranking MLE comparison, all-model interaction-unit and covariance checks, VIF/subgroup variation/influence statistics, raw growth tail summaries and matched-sample P2 no-lag diagnostic.
- `code_check_severe_p1_decline.py`: boundary tests at all three cutoffs, intended-sample assertions, separation assertions, matched-sample no-lag validation and interaction-unit checks; independent QR coefficients/covariances for all 24 OLS fits; numerical probability and parameter-gradient reproduction of all 37 principal AMEs.
- `Results_severe_P1_decline_analysis.xlsx`: regenerated with the same eight tabs; no unnecessary technical tabs. No primary model, estimator, covariance policy, winsorisation, or outlier exclusion was changed.
- `documentation_severe_p1_decline.md`, `documentation_project.md`, and this audit report: method, audit outcome and interpretation updated.

Canonical data and all existing main OLS, quantile, and diagnostics workbooks remain unchanged; file hashes were verified before and after. Final saved-workbook comparison confirmed all 2,858 original numerical table values unchanged, eight tabs and 24 native tables, no error cells, no empty expected tables and no duplicate table rows.

## D. Audited substantive results

### D1. Primary vulnerability predictors

The retained estimator is Firth in both samples, necessary for manufacturing separation and deliberate for ranking consistency. Approximate AME interpretation is preferred for magnitude. Ranking: a one-SD capital-ratio increase is associated with about −2.30 percentage points in severe-decline probability (p=.0114); foreign ownership is about +2.78 points (p=.0965), weak evidence. Automotive, fuels and services sector contrasts are positive relative to production; sparse-sector and nonlinear-Wald caveats apply. Manufacturing: capital ratio is about −2.46 points (p=.0547), suggestive; automotive is +17.88 points (p=.000366), construction −7.05 points (p=.00956). Profitability, size, export intensity and lag growth do not show reliable principal vulnerability evidence. These are associations within selected survivor/complete-trajectory samples, not causal probabilities.

In the table below, AMEs are displayed in percentage points; standard errors and interval endpoints remain in probability units, matching the workbook (multiply them by 100 for percentage points).

| sample | variable | AME_percentage_points | std_error | p_value | CI_lower | CI_upper |
| --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | capital_ratio_start_P1 | -2.29959 | 0.00909046 | 0.0114169 | -0.0408129 | -0.00517892 |
| RANK2019 | owner_num | 2.78037 | 0.0167286 | 0.0965029 | -0.00498368 | 0.0605911 |
| RANK2019 | sector_en_automotive | 9.41186 | 0.0314552 | 0.00277019 | 0.0324676 | 0.15577 |
| RANK2019 | sector_en_food | -4.84067 | 0.0239479 | 0.043245 | -0.0953436 | -0.00146974 |
| RANK2019 | sector_en_fuels | 12.3427 | 0.0570326 | 0.0304535 | 0.0116448 | 0.235209 |
| RANK2019 | sector_en_health and pharma | -5.98399 | 0.0283448 | 0.0347599 | -0.115395 | -0.00428518 |
| RANK2019 | sector_en_retail trade | 8.46293 | 0.0500187 | 0.0906551 | -0.0134056 | 0.182664 |
| RANK2019 | sector_en_services | 10.4948 | 0.0441541 | 0.0174613 | 0.0184072 | 0.191488 |
| RANK2019_MANUFACTURING | capital_ratio_start_P1 | -2.45512 | 0.0127771 | 0.05467 | -0.0495939 | 0.000491591 |
| RANK2019_MANUFACTURING | sector_en_automotive | 17.8775 | 0.0501644 | 0.00036554 | 0.0804549 | 0.277096 |
| RANK2019_MANUFACTURING | sector_en_construction | -7.05015 | 0.0272044 | 0.00955457 | -0.123821 | -0.0171818 |
| RANK2019_MANUFACTURING | sector_en_food | -4.54697 | 0.0246092 | 0.0646497 | -0.0937028 | 0.00276339 |
| RANK2019_MANUFACTURING | sector_en_services | 17.3956 | 0.102962 | 0.091121 | -0.0278461 | 0.375758 |

Capital-ratio AMEs remain negative at all three cutoffs, but precision changes. For RANK2019, the −15%/−20%/−25% effects are −1.59/−2.30/−1.58 percentage points (p=.145/.0114/.0352). Manufacturing effects are −1.12/−2.46/−1.72 points (p=.439/.0547/.0947). This supports a consistent direction, not uniformly precise threshold robustness. These effects are now included in the workbook's threshold-comparison table.

### D2. Subsequent performance and the requested P2 sensitivity

| sample | variant | N | bottom_N | coefficient | SE | p_value | R2 | CI_lower | CI_upper |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | With continuous P1-growth lag (primary) | 1796 | 221 | -0.0627837 | 0.0819629 | 0.443778 | 0.140564 | -0.223538 | 0.0979705 |
| RANK2019 | Without continuous P1-growth lag (diagnostic) | 1796 | 221 | 0.225126 | 0.0710107 | 0.00154881 | 0.118233 | 0.085852 | 0.364399 |
| RANK2019_MANUFACTURING | With continuous P1-growth lag (primary) | 783 | 87 | 0.0388561 | 0.137456 | 0.777498 | 0.176711 | -0.23098 | 0.308692 |
| RANK2019_MANUFACTURING | Without continuous P1-growth lag (diagnostic) | 783 | 87 | 0.14195 | 0.111428 | 0.203081 | 0.174946 | -0.0767913 | 0.360691 |

Ranking changes from −0.062784 (p=.443778) with continuous P1 growth controlled to +0.225126 (p=.001549) without that lag, on identical N=1796. The retained lag explains part of the severity–subsequent-growth association: the primary threshold coefficient asks whether crossing the cutoff adds information beyond actual prior growth. Without the lag, the dummy also captures the broader association between prior contraction and later growth, including regression-to-the-mean mechanisms. Manufacturing changes from +0.038856 (p=.777498) to +0.141950 (p=.203081), both imprecise.

P3 contains `lag_ngrowth_log_ann_P3`, which is prior **P2 (2020–2022)** annualised log growth, not P1 growth or a FULL trajectory outcome. P1 membership is not tautologically part of the P3 growth outcome. P3 starting covariates and the P2 lag can still mediate earlier decline, so coefficients remain conditional associations.

### D3. Threshold verification — group coefficients

| sample | threshold | total_N | bottom_N | N_P2 | Bottom_coefficient_P2 | p_P2 | N_P3 | Bottom_coefficient_P3 | p_P3 | estimator |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | -0.15 | 1827 | 344 | 1796 | -0.0804251 | 0.249633 | 1800 | -0.124106 | 0.0354842 | Firth logit; nonrobust winsor_std OLS |
| RANK2019 | -0.2 | 1827 | 229 | 1796 | -0.0627837 | 0.443778 | 1800 | -0.235358 | 0.000623955 | Firth logit; nonrobust winsor_std OLS |
| RANK2019 | -0.25 | 1827 | 156 | 1796 | 0.030865 | 0.753829 | 1800 | -0.188849 | 0.021975 | Firth logit; nonrobust winsor_std OLS |
| RANK2019_MANUFACTURING | -0.15 | 787 | 134 | 783 | 0.0258228 | 0.831937 | 784 | 0.0774969 | 0.373826 | Firth logit; nonrobust winsor_std OLS |
| RANK2019_MANUFACTURING | -0.2 | 787 | 88 | 783 | 0.0388561 | 0.777498 | 784 | -0.0976468 | 0.331364 | Firth logit; nonrobust winsor_std OLS |
| RANK2019_MANUFACTURING | -0.25 | 787 | 54 | 783 | -0.0424664 | 0.796528 | 784 | -0.0347569 | 0.779717 | Firth logit; nonrobust winsor_std OLS |

Ranking P3 is negative at all thresholds: −0.124106 (p=.035484), −0.235358 (p=.000624), −0.188849 (p=.021975). Direction is stable and all intervals exclude zero, while magnitudes and precision vary. It is threshold-stable conditional evidence of poorer later growth, not proof of causal persistence or broad robustness. Manufacturing P3 changes sign and remains imprecise; there is no stable group-effect conclusion. P2 primary group effects are imprecise and change sign across thresholds in both samples.

### D4. Profitability interactions and severe-group slopes

| sample | threshold | period | N | coefficient | SE | p_value | bottom_profit_slope | bottom_profit_p_value |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANK2019 | 15 | P2 | 1796 | -0.267474 | 0.422371 | 0.526639 | 0.157985 | 1.13322e-07 |
| RANK2019 | 20 | P2 | 1796 | -0.307542 | 0.399789 | 0.441841 | 0.156942 | 1.49722e-07 |
| RANK2019 | 25 | P2 | 1796 | -0.360345 | 0.397727 | 0.365054 | 0.151798 | 3.59001e-07 |
| RANK2019 | 15 | P3 | 1800 | -0.184676 | 0.066835 | 0.00578352 | -0.163287 | 0.00950915 |
| RANK2019 | 20 | P3 | 1800 | -0.186513 | 0.0667358 | 0.00524918 | -0.164653 | 0.00882755 |
| RANK2019 | 25 | P3 | 1800 | -0.181348 | 0.066909 | 0.00678559 | -0.159065 | 0.0116676 |
| RANK2019_MANUFACTURING | 15 | P2 | 783 | 1.4959 | 1.42789 | 0.295141 | 0.0951435 | 0.172191 |
| RANK2019_MANUFACTURING | 20 | P2 | 783 | 1.51347 | 1.40077 | 0.280283 | 0.0912656 | 0.190353 |
| RANK2019_MANUFACTURING | 25 | P2 | 783 | 1.26721 | 1.38652 | 0.36103 | 0.105452 | 0.124773 |
| RANK2019_MANUFACTURING | 15 | P3 | 784 | 2.25411 | 1.47199 | 0.1261 | -0.241269 | 1.5265e-06 |
| RANK2019_MANUFACTURING | 20 | P3 | 784 | 2.56458 | 1.44371 | 0.0760685 | -0.23596 | 2.66782e-06 |
| RANK2019_MANUFACTURING | 25 | P3 | 784 | 2.74118 | 1.44429 | 0.0580801 | -0.232911 | 4.0428e-06 |

At −20%, ranking P2 slope difference −0.307542 (p=.441841), severe slope +0.156942 (p=1.50e−7); ranking P3 difference −0.186513 (p=.005249), severe slope −0.164653 (p=.008828). Manufacturing P2 difference +1.513467 (p=.280283), severe slope +0.091266 (p=.190353); manufacturing P3 difference +2.564581 (p=.076069), severe slope −0.235960 (p=2.67e−6).

Ranking P3 interaction differences are negative and similar across thresholds (~−0.181 to −0.187, p≈.005–.007), but financial-ratio outlier sensitivity remains a separate issue. Ranking P2 differences are negative but poorly determined under high collinearity. Manufacturing P2 and P3 differences are positive across thresholds; P2 is imprecise, and P3 inference ranges from p=.126 to .058. Its stable severe-group negative slope is driven by the same extreme firm retained in every severe group. **The earlier interaction outputs were mathematically valid and were not caused by inconsistent scaling. Their substantive robustness, especially manufacturing P3, is undermined by extreme predictor influence and very small non-Bottom variation.**
