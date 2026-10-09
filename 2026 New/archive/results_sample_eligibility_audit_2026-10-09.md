# Sample eligibility and data-quality audit

Audit date: 9 October 2026. Audit only: no production scripts, source data, regression specifications or existing Excel outputs changed.

## Exact primary OLS counts

| scenario               | period   |   current_N |   valid_N |   newly_excluded_N |   common_sample_N |   additional_common_loss_N | additional_common_loss_share   |
|:-----------------------|:---------|------------:|----------:|-------------------:|------------------:|---------------------------:|:-------------------------------|
| ALL                    | P1       |        2385 |      2370 |                 15 |              2332 |                         38 | 1.60%                          |
| ALL                    | P2       |        2426 |      2406 |                 20 |              2332 |                         74 | 3.08%                          |
| ALL                    | P3       |        2438 |      2429 |                  9 |              2332 |                         97 | 3.99%                          |
| ALL                    | FULL     |        2423 |      2406 |                 17 |              2332 |                         74 | 3.08%                          |
| MANUFACTURING          | P1       |         979 |       970 |                  9 |               949 |                         21 | 2.16%                          |
| MANUFACTURING          | P2       |         995 |       982 |                 13 |               949 |                         33 | 3.36%                          |
| MANUFACTURING          | P3       |        1000 |       992 |                  8 |               949 |                         43 | 4.33%                          |
| MANUFACTURING          | FULL     |         995 |       986 |                  9 |               949 |                         37 | 3.75%                          |
| RANK2019               | P1       |        1786 |      1775 |                 11 |              1748 |                         27 | 1.52%                          |
| RANK2019               | P2       |        1791 |      1775 |                 16 |              1748 |                         27 | 1.52%                          |
| RANK2019               | P3       |        1795 |      1787 |                  8 |              1748 |                         39 | 2.18%                          |
| RANK2019               | FULL     |        1794 |      1782 |                 12 |              1748 |                         34 | 1.91%                          |
| RANK2019_MANUFACTURING | P1       |         781 |       772 |                  9 |               754 |                         18 | 2.33%                          |
| RANK2019_MANUFACTURING | P2       |         782 |       771 |                 11 |               754 |                         17 | 2.20%                          |
| RANK2019_MANUFACTURING | P3       |         783 |       776 |                  7 |               754 |                         22 | 2.84%                          |
| RANK2019_MANUFACTURING | FULL     |         784 |       775 |                  9 |               754 |                         21 | 2.71%                          |

Strict model-relevant proposed rules remove 38 distinct firms from at least one current primary OLS model (not the sum of scenario-period counts). All newly proposed removals are export-ratio bound failures. No other rule adds removals beyond current complete cases. These firms remain in the canonical datasets.

| scenario               |   distinct_newly_removed |   distinct_additional_common_loss |
|:-----------------------|-------------------------:|----------------------------------:|
| ALL                    |                       38 |                               121 |
| MANUFACTURING          |                       26 |                                53 |
| RANK2019               |                       30 |                                54 |
| RANK2019_MANUFACTURING |                       24 |                                31 |

Distinct additional common loss counts firms eligible in at least one proposed-valid period but not all four. It is not a marginal loss for any single period.

## Rules and existing controls

- Required finite inputs; source prerequisites only for variables used by exact model.
- Export [0,1] is a proposed conditional comparability restriction; unresolved reporting boundaries retained.
- No 0–1 constraint for capital ratio, profitability, turnover or productivity.
- 2018 sales only, used solely for P1 lag; no 2018 employment/export/assets requirement.
- Dependent-only winsorisation at 1%/99%; no new extreme-value exclusions.
- Reconstruction tolerance rtol=atol=1e-10, not an economic cleaning threshold.

Canonical period input: 2,533 unique firms; annual core: 17,731 firm-years, 2018–2024. Keys are unique. Shared manual configuration removes 5 firms present in the input; Orlen is already absent.

Selection order: upstream source preparation → shared manual exclusions (NIP or exact stripped name) → ranking/manufacturing scenario → complete nominal P1/P2/P3 trajectory → model-specific complete cases → outcome winsorisation → scaling. FULL starts in 2019, ends in 2024 and has no lag. P1/P2/P3 lags and controls remain exactly as configured. Missing/non-numeric numeric inputs are coerced to NaN; no zero imputation. The current model helper uses dropna, without an explicit infinity screen.

Upstream notebook manual removal list: Orlen 7740001454 and holding-duplicate IDs 5260152146B, 5262297860B, 7521457964B. The notebook also retains firms with all observed panel years and strictly positive 2019 sales; these existing source-population selections are separate from proposed rules. Notebook code inspection is not proof that every changed upstream file was regenerated in that order. These absent records are not reinstated. Current canonical/analysis scripts contain no additional sector exclusion list: sector is a categorical control; manufacturing/ranking are scenario filters. This audit cannot recover firms absent from upstream input.

Core builder: coerces annual numeric inputs, drops missing NIP/year keys, and resolves duplicate firm-years deterministically by greatest non-missing coverage then company name. Missing source columns remain missing. Ratios require a nonzero denominator, but current safe-ratio logic permits negative nonzero denominators. Logs require positive arguments. Period builder requires positive endpoint sales for log/trajectory calculations; copies covariates from 2019/2020/2022, computes P1 lag from 2018–2019 and later lags from prior defined periods. Proposed prerequisites are evaluated only where a value is used in the exact model; a bad unused annual observation does not remove an entire firm automatically.

## Diagnostics and model alignment

Primary OLS is additive. Diagnostics and quantile retain the active export × size product; the interaction supplement has separate export × size and profitability × manufacturing specifications. Their exact missingness effects are displayed separately in 01_SAMPLE_COUNTS. Ordinary predictor transformations and dependent-only winsorisation do not change selected company IDs. All four OLS variants use the same selector; saved primary Ns agree for all 64 model rows. Quantile Q10/Q50/Q90 share complete cases. Severe decline uses additive P1/P2/P3 complete cases in the two ranking scenarios, plus the observed P1 group indicator; it has no FULL model. No severe refit was performed.

Regression descriptives (31_SCENARIO_DIAGNOSTICS), winsor impact (04), and correlations (20–25, 90–91) use exact period complete cases under the diagnostics configuration. Missingness (03 and missingness columns in 31) uses pre-complete-case rows. Trajectory summaries, growth/path summaries, sector/ownership profiles, median covariate profiles, performance-band profiles (02, 10–16), and firm trajectories (92) use the broader complete-trajectory population. A variable-specific profile ignores missing values without requiring all other controls. 30_SCENARIO_SUMMARY describes this pre-model scope. These are intentionally different estimands; align only reports meant to describe regressions.

Saved diagnostics Ns and saved primary regression Ns match independently reconstructed current company sets. The saved diagnostic workbook does not store every estimation ID; membership is reconstructed from the source and exact current helper, with SHA256 set fingerprints in 02_SAMPLE_OVERLAP. Saved 92_FIRM_TRAJECTORIES supplies the broader firm list.

All four scenarios currently use different firms across periods. Baseline/winsorised and raw/standardised variants have identical company sets within a scenario-period. Active interaction specifications introduce **zero additional missing-case exclusions**; diagnostics complete-case firm IDs equal primary OLS IDs for all 16 scenario-period combinations. Saved quantile Ns (48 rows) and compact interaction Ns (48 rows) also agree with reconstruction.

## Export interpretation

Exports/sales is calculated without clipping as exports divided by nonzero sales. A proposed [0,1] eligibility rule therefore changes current practice; it is conditional on identical reporting scope and definitions. Exports above sales do not alone prove a mathematical error in the stored ratio. Negative exports may represent source adjustments; researcher review remains necessary. The exclusion register displays original numerator/denominator, value, source year and exact model/diagnostic membership. No source correction or cap is applied.

Specific cases: Xiaomi Technology (Polska), NIP 5213838221, export ratio **5.018632** in 2019 (exports 41,491; sales 8,267.39314): included in trajectory/covariate profiles; excluded from P1 regression/correlation because the 2018–2019 lag is missing; **included in FULL regression/correlation**, which has no lag. E&Y, NIP 5260207930, **2.043182 in 2019** and **2.017796 in 2020**, enters P1/FULL and P2 regressions/correlations respectively. Vicis, NIP 5242617178, **6.565700 in 2019**, has an incomplete trajectory and enters none of these analyses. Iglotex Dystrybucja Polska, NIP 6790175807, has negative exports in 2020: the P2 export ratio is **−0.003552**, currently included in P2 regression/correlation. Some excess ratios are very close to 1 and may reflect rounding; strict proposed counts deliberately use the requested inclusive [0,1] bound without adding a grace threshold.

## Recommendation

A common sample is feasible with modest numerical loss: **RANK2019 retains 1,748 firms**, losing a further **27–39 (1.52–2.18%)** per period after the proposed validity screen. **RANK2019_MANUFACTURING retains 754**, losing **17–22 (2.20–2.84%)**. ALL retains 2,332 (further 1.60–3.99% loss), and ALL manufacturing retains 949 (2.16–4.33%). These are small enough to justify a common-sample sensitivity analysis. They do not establish that selection is ignorable: compare retained/excluded firms and coefficients before making the common sample primary. No arbitrary acceptance cutoff is introduced.

Recommend introducing a common sample as a labelled sensitivity analysis after researcher approval of the conditional export rule, and using its exact company set for matching regression descriptives/correlations. Preserve broader trajectory summaries with explicit sample labels. Common growth trajectories already hold; incremental losses arise from control/lag missingness and proposed exclusions across periods. Implementation is deliberately deferred.

## Reproduction and validation

Run `python code_audit_sample_eligibility.py`, then the companion `code_write_sample_eligibility_audit.mjs` with `/tmp/grip_sample_eligibility_audit.json`, the absolute audit workbook destination and a temporary preview directory (bundled artifact-tool Node dependencies). Run `python code_check_sample_eligibility_audit.py` after export. The project Python environment is used because the bundled Python lacks a Parquet engine. Existing files are hashed before/after; unique keys, independent complete-case ID reconstruction, saved 64 OLS Ns, 48 quantile Ns, 48 interaction Ns, saved diagnostic/correlation Ns, severe primary Ns and actual set intersections are checked. No model is fitted. Workbook counts are static audit results regenerated by the audit, not editable cleaning decisions.

Protected input SHA256: `680428dab4fe24f6af460d6a69500cf3b99061be241247d9554ac042379e1e0b` (period) and `902edfd781c9661a62f382fe4c9944a3b2f008134b182b67bd02ed8a09fac7e1` (core).
