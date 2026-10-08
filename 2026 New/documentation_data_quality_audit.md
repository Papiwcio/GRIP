# GRIP independent data-quality audit

Implementation date: 8 October 2026. The original design is preserved in `documentation_data_quality_audit_specification.md`; this document describes the implemented reporting-only workflow and approved departures.

## Purpose and scope

The audit examines all canonical firms in `data_core_2018-2024.parquet` and `data_period_2018-2024.parquet`, regardless of ranking, trajectory completeness, regression complete cases or manual analytical exclusions. It does not estimate regressions or modify builders, datasets, financial values, model configuration or exclusion policy.

2018 is the approved **sales-only** year. It supplies the 2018→2019 P1 lag; its existing `sales`/`przychody` source mapping is preserved. Other 2018 financial/employment values are EXPECTED_UNAVAILABLE. No additional 2018 ownership observation is required. Annual financial checks use 2019–2024; sales/CPI/log/lag integrity also covers 2018. The 2023 operating-result block, absent tax/maturity-specific liabilities and year-specific alternative liability blocks are treated separately from individual missing observations. None are imputed or reconstructed.

The implementation imports side-effect-free builder constants and shared manual-exclusion metadata. It **independently recomputes** formulas rather than calling producer calculation functions. Tests use disposable synthetic fixtures produced by existing functions to exercise the independent auditor; no canonical builder run is performed.

## Files and reproducibility

- `code_audit_data_quality.py`: reusable `audit_frames(core, period, source=None, statistical=True)` plus command-line workflow.
- `code_write_data_quality_audit.py`: native-table XLSX export fallback for this large report; no data-quality calculations or decisions.
- `code_write_data_quality_audit.mjs`: bounded artifact-tool visual inspection of the same content/layout; no data-quality calculations or decisions.
- `code_check_data_quality_audit.py`: synthetic and actual-data acceptance tests.
- `results_data_quality_audit.xlsx`: six requested sheets, complete finding tables and ranked review views.
- `results_data_quality_audit_metadata.json`: run manifest, protected-input hashes, variable inventory, coverage, independent performance thresholds, rule execution contexts and statistical references.
- `data_quality_audit_decisions.csv`: persistent researcher log keyed by stable finding IDs.

Run from this directory using the established project Python environment with pandas, numpy and a Parquet engine (PyArrow):

```sh
python code_check_data_quality_audit.py
python code_audit_data_quality.py
```

Optional `--directory` selects existing input files, `--output-directory` writes audit artifacts elsewhere, `--previews /tmp/grip_audit_previews` creates inspection images, and `--no-statistical` disables exploratory statistical screens while retaining integrity/domain/comparability checks. Do not compare a no-statistical run's totals with the complete audit without identifying that difference.

The full report has over one million populated/styled cells. The initial artifact-tool full-size export exceeded its memory budget; increasing the budget still left an impractically slow export, which was stopped. The implementation therefore uses the existing project's XlsxWriter library as a large-table fallback, preserving every finding and native Excel tables. The bundled Node/artifact-tool runtime renders bounded leading ranges with the same data/layout for visual inspection; native features and complete row counts are checked in the saved XLSX itself. This is an explicit export-tool departure, not a reduction in audit scope.

`GRIP_AUDIT_NODE` and `GRIP_AUDIT_NODE_MODULES` may specify the bundled preview runtime on another installation. Node is needed only when `--previews` is requested. The bundled Python runtime inspected on this host lacks a Parquet engine, so the existing project Python environment supplies Parquet analysis and XlsxWriter export. No dependency folder is modified. Other hosts require pandas, numpy, a Parquet engine and XlsxWriter; versions and executable code hashes are recorded in metadata.

Input files are read only. SHA-256 hashes cover existing datasets, workbooks, the original specification and protected builder/model/configuration files before and after execution; a changed hash raises an error rather than claiming non-destructiveness. Concurrent external edits during a run can also trigger that assertion. Audit output workbooks are excluded from this protected-input set. Preview files and temporary JSON/Node module links are not canonical artifacts.

## Interpretation and implemented controls

Classification and severity are separate:

- **INVALID**: demonstrated structural, mathematical or confirmed-domain violation. Examples: duplicate canonical keys, infinity, wrong stored ratio, numeric log of nonpositive sales, non-adjacent observations labelled YOY.
- **SUSPICIOUS**: unresolved financial/source/accounting/identity question. Examples: exports above sales, negative exports, asset reconciliation discrepancy, unit-factor change.
- **EXTREME**: unusual but potentially valid magnitude or distribution observation. These findings never authorise exclusion.

Severity is critical, high, moderate or low. Low export exceedances and high material discrepancies are different review priorities, not different proofs of invalidity. Executions use PASS, FLAG, NOT_EVALUABLE and EXPECTED_UNAVAILABLE; unavailable inputs are not fabricated zeros.

The Rule_Register retains all 49 design rules and marks implemented, covered-by-another-rule and deferred checks. Core/source findings and Period findings have distinct grains. Summary distinguishes finding counts, unique companies and unique Core firm-years; several rules and propagated period screens can refer to the same original economic issue.

| Rules | Implemented behaviour |
| --- | --- |
| D01/D02/D04 | Required schemas/types/keys, unexpected years, all duplicate candidates, invalid finite-number representation. Missing numeric and zero remain distinct. |
| D05/D06 | Expected-block-aware annual missingness and missing period endpoint lineage. Growth unavailable for nonpositive sales is distinguished from missing sales. |
| D07/D08/D09 | Identifier patterns/shared REGON/KRS, cross-dataset membership, annual descriptor conflicts, missing ownership mapped to Domestic, independent mapping and Period descriptor selection. No padding/fuzzy merges or removals. |
| D10/D11 | Optional existing local panel: pre-deduplication candidates and original financial-value/source fallback comparison. Unavailable/ambiguous source keys are explicitly not evaluable. |
| A01/A02/A03/A04/A13 | Negative FTE versus suspicious negative assets/revenue/costs; zero denominators with activity; wage/employment disagreements. Signed profits and equity are not subject to blanket positivity. |
| A05/A06/A12 | All 12 ratios/per-employee measures, logs, availability flags and algebraic ratio identities. Missing/zero denominator and missing/nonpositive log input handling independently verified. |
| A07/A14/A15 | Small-denominator context, exploratory ratio thresholds and equity/assets >1 conditional review. Negative equity/capital ratio alone is not an automatic finding. |
| A08 | Asset partition/component comparisons, with review-only relative tolerance 1e-6 of maximum participating magnitude; residual >1% is high priority, smaller discrepancies moderate. Unknown units/precision/exhaustive coverage prevent invalidity claims. |
| E01/E02/E03 | Export/sales >1; above 1.01 high priority, smaller exceedances low; negative exports with actual quotient displayed; >50 percentage-point jumps or tenfold export changes without comparable sales changes. |
| E06 | Export arithmetic and starting-ratio copies covered by A05/P02; no duplicate findings for the same calculation. |
| T01/T02/T03/T04 | Fivefold changes in positive levels; signed movement/sign reversal; factors near 100/1,000/1,000,000 and reciprocals; transient tenfold spikes/troughs. These are review hypotheses, never rescaling instructions. |
| T05 | Explicit exploratory MAD/IQR screening; finite/domain eligibility, minimum reference n and recorded reference statistics. |
| T06/T07 | CPI/real-sales and real/nominal growth integrity; genuine consecutive-calendar YOY independently calculated from retained unique records, with optional upstream duplicate evidence. |
| P01/P02/P03/P04/P05/P06/P07 | Positive-endpoint period/annualised/full growth, lags and 45 exact start covariates; trajectory signs/patterns/labels/groups/indices; SGrowth/performance logic and overlap; length-aware growth screening and chained/full-horizon reconciliation. |

All numerical reproduction uses `abs(stored-expected) ≤ 1e-10 + 1e-8×abs(expected)`, with missingness checked separately. Categories/keys/flags compare exactly. Ordinary Core ratios allow negative nonzero denominators; Core simple YOY requires a positive lag but permits current zero/negative sales, matching current documented producer logic. Period growth/log/CAGR requires both endpoints positive. A numeric value incorrectly substituted for an undefined result is INVALID; the raw zero or valid loss is not thereby invalid.

## Statistical screening and eligibility

The audit evaluates raw unmodified canonical values, not winsorised model variables. For eligible sectors with ≥30 finite/domain-valid observations, references are sector×year in Core and sector×variable in Period. Smaller sectors use the pooled same-year/Period-variable reference if its n≥30. Missing sectors use their explicit Unknown group or pooled fallback; manual analytical exclusions do not change references. A technically invalid stored value is not included in its variable's statistical reference.

Positive monetary levels, employment and per-employee measures use natural logs. Signed profits/equity and ratios use raw values. MAD score is `abs(x-median)/(1.4826×MAD)>5`; if MAD=0, use Q1−3×IQR / Q3+3×IQR. If IQR is also zero, mark NOT_EVALUABLE rather than inventing a scale. Minimum-n exclusions and exact reference statistics are in metadata and each statistical finding's related values.

Fixed CPI lookups, dummy indicators, categorical variables and constant 2019 index bases are not distribution-screened. Broad economic ratio limits are separate EXTREME screens: absolute profit/operating margin/ROA >1, absolute ROE >5, wage/depreciation intensity >1, asset turnover >20, absolute equity multiplier >100. They are exploratory thresholds rather than universal feasible ranges. No universal absolute per-employee currency threshold is used while currency/scale remains unresolved.

MAD can flag a large share when a reference is highly concentrated, zero-inflated or heavy-tailed. The zero-dispersion rule can instead leave such a distribution without a statistical flag. Repeated Core/Period representations and overlapping nominal/real measures increase finding counts without representing additional companies or independent source errors. Interpret these screens using their raw values, denominator and reference distribution; do not infer an error rate from total findings.

## Deferred/partial checks and departures from the design

| Rule / element | Reason and current treatment |
| --- | --- |
| D03 | Original numeric-token parsing and mixed separators need a verified tracker-to-panel mapping. Canonical types/infinity checked; raw parsing not replayed. |
| D12 | Existing canonical Excel workbooks contain researcher edits. They are protected, not overwritten; Parquet is the audit authority. A dedicated approved cell-by-cell representation audit remains possible. |
| A02C | Confirmed nonnegative balance-sheet definitions/sign conventions are not supplied; negative assets/liabilities remain SUSPICIOUS under A02. |
| A09/A10/A11 | Liability balancing dictionary, maturity components or tax not verified/available. Record NOT_EVALUABLE; no inferred debt/tax or alternative aggregate substitution. |
| E04/E05/E07 | Per-value currency/units/scope and annual S/J meaning, original export subset relation, organisational-boundary evidence unavailable. No proof of a specific E&Y correction or new exclusion. |
| D07 partial | No checksum-based rejection, surrogate reassignment or fuzzy identity resolution; unusual patterns/shared registration IDs only generate review findings. |
| D10 partial | Panel duplicates checked; pre-pivot conflicting Polish source cells are not reconstructed from cleaned data. |
| T03/T04 partial | Individual suspected unit factors and temporal spikes/troughs checked. Multi-field unit corroboration and repeated-vector copying require a verified field/precision dictionary and remain deferred. |
| P04 partial | Independently verify canonical classes and recompute all thresholds/means/tail counts in metadata. Do not read researcher-edited source workbook thresholds as authoritative audit inputs in this run. |
| Workbook structure | Use the six requested sheets. Coverage/inventory/references/contexts live in supporting metadata; detailed values are visible in finding tables/related-values cells. |
| Workflow integration | Standalone runner only. Builders and regressions remain unmodified; adding an optional builder audit hook is not necessary or authorised as a side effect. |

The approved 2018 source mapping is accepted as given. It is checked against the local panel fallback where available, not reclassified as a missing-data problem or redesigned. The original specification's proposed numeric limits are exploratory settings in the audit module; they do not become production cleaning policy. Original design documentation is preserved byte-for-byte.

## Source values and analytical relevance

Finding tables show company/NIP, observation year/period, variable/rule/classification/severity, original/calculated values, numerator/denominator, actual exports/sales, profit/employment/assets/equity and related values. `source_year` identifies the displayed Core start-year amounts for a Period finding; endpoint details remain in `related_values`. Unknown currency/consolidation metadata is not guessed.

`affected_variables` describes possible downstream dependencies, not confirmed invalidity or eligibility. For example, 2019 exports/export ratio links to P1 starting export intensity and FULL's same starting covariate; 2020 links to P2, 2022 to P3. Sales can affect logs, ratios, growth endpoints, lags, full-horizon classifications and trajectory indices. A 2021/2023 problem can affect annual comparisons even though it is not a Period endpoint. No firm-level exclusion is inferred from one flag.

## Researcher decisions and prioritisation

Stable IDs depend on dataset, firm key, year/period, variable and rule, not measured values or run time. Structural duplicate/missing-key findings additionally include a row reference. Duplicate IDs for different manifestations of one observation/rule are consolidated. All raw candidate records for duplicate-key findings remain distinct. Findings sort deterministically; screen references are reproducible for fixed inputs.

On first run, the CSV log receives one Unresolved row per finding. Reruns append only genuinely new IDs and preserve all existing log bytes and prior decisions. Researchers may edit the decision/evidence fields or append dated history rows; the final row for an ID supplies the displayed review outcome. Accepted outcomes: Retain, Correction proposed, Variable unusable, Period comparability issue, Firm exclusion proposed, Unresolved. Unknown outcomes/schema errors stop the run without rewriting the log. Disappeared findings remain in the log as history.

Prior decisions are not automatic authority for data/sample changes. If source values/hashes change, researchers must reconsider affected decisions; stable IDs preserve history rather than certify a past decision remains appropriate. Evidence, reviewer, date, scope and separate approval status are required before any subsequent action.

The top-20 ranking is lexicographic: maximum severity (critical > high > moderate > low), number of distinct flagged rule IDs, number of analytically relevant findings, total finding count, then NIP ascending. It is not an automatic exclusion recommendation. Company_Review contains **all** affected companies with rule reasons and source-amount examples; Summary puts the top 20 before detailed grouped counts. Existing manual exclusions are supplementary metadata at the end of README, not audit-population filters.

## Validation and current results

The full-population run covers 17,731 Core firm-years and 2,533 Period firms. The supporting manifest records the input/code hashes and complete check/reference metadata.

| Measure | Result |
| --- | ---: |
| Total findings | 31,372 |
| Unique affected companies | 1,987 |
| Affected Core firm-years | 7,850 |
| Unresolved high-priority findings | 746 |
| INVALID findings | 0 |
| Affected company share | 78.44% |

| Finding classification / severity | Count |
| --- | ---: |
| EXTREME/moderate | 28,471 |
| SUSPICIOUS/moderate | 2,094 |
| SUSPICIOUS/high | 746 |
| SUSPICIOUS/low | 61 |

There are 22,522 Core/source findings and 8,850 Period findings. No current canonical formula/domain/key failure was found, including annual YOY and period/lag/start-covariate calculations. This is not assurance of correct original source amounts: unresolved scope, units and conditional accounting definitions remain. 25,300 findings are T05 distribution screens; 100 are export/sales >100%, one is negative exports, and 390 are export discontinuity screens. These overlap firms/years and must not be summed into a source-error rate.

Known cases reproduce as SUSPICIOUS: E&Y NIP 5260207930 in 2019 (204.3182%) and 2020 (201.7796%), and Iglotex NIP 6790175807 in 2020 (exports −2,324; ratio −0.355188%). No source replacement amount or new exclusion is inferred.

The 20 highest-priority firms under the stated ranking are:

| Rank | Company | NIP | Highest severity | Distinct rules | Findings | Rule IDs |
| ---: | --- | --- | --- | ---: | ---: | --- |
| 1 | Archigrest sp. z o.o., Warszawa | 7010676783 | high | 12 | 158 | A04, A07, A08, A13, A14, A15, D05, P06, T01, T03, T04, T05 |
| 2 | Wexler Development sp. z o.o., Katowice | 6431767287 | high | 11 | 107 | A03, A04, A07, A13, A14, E03, P06, T01, T02, T04, T05 |
| 3 | Vicis New Investments SA, Warszawa | 5242617178 | high | 11 | 84 | A04, A07, A08, A13, A14, E01, E03, P06, T01, T04, T05 |
| 4 | Stellantis Gliwice sp. z o.o., Gliwice | 9691633993 | high | 9 | 111 | A04, A07, A14, E01, E03, P06, T01, T03, T05 |
| 5 | PBG SA w Restrukturyzacji GK, Wysogotowo | 7772194746 | high | 9 | 121 | A04, A07, A08, A13, A14, P06, T01, T03, T05 |
| 6 | Przedsiębiorstwo Inwestycyjne Investel sp. z o.o., Białe Błota | 5540232811 | high | 9 | 111 | A04, A07, A08, A14, A15, P06, T01, T04, T05 |
| 7 | Saxdor Shipyard sp. z o.o., Ełk | 8481875985 | high | 9 | 86 | A04, A07, A08, A14, D05, P06, T01, T02, T05 |
| 8 | Milkpol Polska sp. z o.o., Warszawa | 5222863856 | high | 9 | 78 | A04, A07, A13, A14, E03, P06, T01, T04, T05 |
| 9 | Globus sp. z o.o., Warszawa | 7773261746 | high | 8 | 111 | A04, A07, A14, P06, T01, T02, T03, T05 |
| 10 | Zorn Development sp. z o.o., Warszawa | 5252670903 | high | 8 | 114 | A04, A07, A14, P06, T01, T02, T04, T05 |
| 11 | Agroimeks sp. z o.o., Warszawa | 5272796501 | high | 8 | 98 | A04, A14, D05, P06, T01, T02, T04, T05 |
| 12 | ISD Huta Częstochowa sp. z o.o. (w upadłości), Częsochowa | 9491827824 | high | 8 | 91 | A04, A07, A08, A13, A14, T01, T03, T05 |
| 13 | EME-AERO sp. z o.o., Jasionka | 5170385680 | high | 8 | 76 | A04, A07, A14, E01, E03, P06, T01, T05 |
| 14 | Linarite Company sp. z o.o. | 7010399384 | high | 8 | 67 | A04, A07, A14, P06, T01, T03, T04, T05 |
| 15 | Polrentcar sp. z o.o. | 5842716173 | high | 8 | 75 | A04, A07, A13, A14, P06, T01, T02, T05 |
| 16 | Bidcorp Poland sp. z o.o. GK, Szczecin | 5272605317 | high | 8 | 41 | A04, A07, A14, D05, P06, T01, T03, T05 |
| 17 | Gobarto Hodowca sp. z o.o., Warszawa | 6991938413 | high | 8 | 48 | A07, A08, A14, A15, T01, T02, T04, T05 |
| 18 | 7R Development Management sp. z o.o., Kraków | 6762556093 | high | 8 | 35 | A04, A07, A13, A14, T01, T02, T04, T05 |
| 19 | TATENEN sp. z o.o., Jarosław | 7922305680 | high | 7 | 78 | A04, A08, D05, E03, P06, T01, T05 |
| 20 | Xiaomi Technology (Polska) sp. z o.o., Warszawa | 5213838221 | high | 7 | 66 | A04, A14, E01, E03, P06, T01, T05 |

All 26 pre-task protected local input/result/specification files retain their original SHA-256 hashes. The original design, canonical Excel annotations, historical results and existing source edits were preserved. The audit leaves original financial values and the six-company exclusion configuration intact; Globus remains visible in this independent review although it is already excluded from regression samples.

Validation covers signed profits/equity, fractional/negative FTE, negative and above-100% exports, zero/negative/missing sales (including 2018), denominators, duplicate and nonconsecutive records, discarded-duplicate dependence, deliberately wrong ratios/growth/lags/indices/start covariates, expected source blocks, MAD/IQR/minimum n, stable IDs and persistent decisions. The saved workbook was inspected for all six sheets, complete row counts, unique IDs, exact known-source amounts, typed numbers, native tables/filters and frozen panes. Bounded visual ranges were reviewed across all six sheets. No regression estimation or correction/exclusion action was run.

**23 automated tests passed**, including saved-workbook acceptance. A second complete run reproduced identical totals, statistical reference populations/statistics and performance thresholds, while preserving the decision log byte-for-byte. Its manifest verifies unchanged protected inputs and records the exact implementation hashes. The workbook is a reproducible snapshot, not a live financial model: refreshing requires rerunning the audit; generated summary cells are saved values. Workbook creation timestamps/run metadata may differ across reruns even when finding content is identical.

There are 39 directly implemented rule families, one shared calculation check covered by A05/P02 (E06), and nine deferred documentary/source checks. Partial elements are identified above rather than silently treated as completed. Outstanding researcher decisions concern original export definitions, annual consolidated/separate boundaries, monetary units/rounding, exhaustive asset/liability/tax definitions and surrogate identifiers. They require review before any data correction, variable restriction or sample change.
