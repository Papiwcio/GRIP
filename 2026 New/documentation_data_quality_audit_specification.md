# GRIP data-quality audit specification

Prepared: 8 October 2026. Status: **proposed rules for researcher review; not implemented**.

## 1. Scope, evidence and operating principles

The proposed audit examines financial observations and their lineage independently of OLS, quantile and severe-decline models. Its primary population is **every firm in the canonical datasets**, including firms currently excluded from regression analysis. It must neither alter canonical files nor create an automatic exclusion mask. Statistical unusualness is not evidence of incorrect data.

This specification was prepared by reading the builders, configuration, metadata and documentation and inspecting existing files in read-only mode. No builder, regression or exclusion procedure was run. The requested specification is the only new deliverable; `results_data_quality_audit.xlsx` is a proposed future output, not a workbook produced during this task.

Reviewed sources:

- `code_build_core.py`, `code_build_period.py`: authoritative current calculations and transformations.
- `code_config.py`, `code_helpers.py`, `code_ols_scenarios.py`, `code_quantile.py`, `code_severe_p1_decline.py`, `code_diagnostics_trajectories.py`: analytical metadata, filtering and transformations.
- `documentation_core_2018-2024.md`, `documentation_period_2018-2024.md`, `documentation_project.md`, `documentation_manual_exclusions.md`, `documentation_export_size_interaction_audit.md`, `documentation_interaction_centring.md`: definitions and existing investigations.
- `../0. Source Data/Data Preparation 2018-2024.ipynb`: upstream header reconstruction, translation, parsing, duplicate handling, screening and removals. This is an upstream provenance source, not the canonical transformation script.
- Existing `data_panel_2018-2024.parquet`, canonical Core/Period Parquet files and selected cells in `../0. Source Data/Data_clean_2018-2024.xlsx` and headers in `Source Tracker.xlsx`, sheet `2018-2024`.

Observed canonical dimensions are Core **17,731 rows, 2,533 firms, 59 columns**, and Period **2,533 rows, 134 columns**. Each year currently has 2,533 Core rows. These are a dated inspection baseline, not fixed counts that future builds must reproduce. Parquet is the calculation input; existing Excel workbooks may contain user edits and annotations and must not be overwritten by an audit. A separate read-only equivalence check can distinguish Excel display differences from changes in stored values.

Interpret accounting comparisons only after establishing common entity/group scope, currency, reporting units, dates, definitions and gross/net presentation. The IFRS Foundation distinguishes consolidated statements representing a parent and subsidiaries as one economic entity from separate statements; this explains why combining amounts from different boundaries can invalidate an analytical ratio without proving either original amount false. These references provide context, not proof that every GRIP firm reports under IFRS: [IFRS 10](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-10-consolidated-financial-statements/), [IAS 27](https://www.ifrs.org/issued-standards/list-of-standards/ias-27-separate-financial-statements/) and [IAS 1](https://www.ifrs.org/issued-standards/list-of-standards/ias-1-presentation-of-financial-statements.html/).

## 2. Variable inventory and lineage

### 2.1 Original annual financial and employment variables: Data Core

All 18 columns below are retained separately. They come from identically named columns in `data_panel_2018-2024.parquet`, except the explicit sales fallback. Polish source labels below are those in the upstream translation or header logic; they do not establish a uniform unit or reporting scope.

| Core column | Definition / original source label | Observed coverage and interpretation |
| --- | --- | --- |
| `sales` | Sales revenue; `Przychody ze sprzedaży`. If panel sales is missing, use panel `przychody`. | Available 2018–2024. All 2,533 2018 values use the fallback. Same conceptual sales definition for that fallback requires source confirmation. |
| `operating_result` | Operating result; `Wynik operacyjny`. Signed profit/loss flow. | Observed 2019–2022 and 2024; entirely missing in 2018 and 2023. |
| `profit_before_tax` | Profit/loss before tax; `Wynik finansowy brutto`. | Observed 2019–2024; signed. |
| `income_tax` | Income tax; `Podatek dochodowy`. | Retained but entirely unavailable in the present panel; no tax reconciliation is executable. |
| `net_profit` | Net profit/loss; `Wynik finansowy netto`. | Observed 2019–2024; signed. |
| `depreciation` | Depreciation/amortisation flow; `Amortyzacja`. | Observed 2019–2024; confirm cost sign convention before asserting non-negativity. |
| `exports` | Export revenue; both `Przychody z eksportu` and `Przychody z eksportu (sprzedaż po zagranicę kraju)` map here. | Observed 2019–2024. The label changes do not establish identical scope, period or gross/net measurement. |
| `employment` | Annual average full-time equivalents; `Zatrudnienie (średnioroczne w etatach)`. | Observed 2019–2024. Fractional FTEs are valid; this is not necessarily year-end headcount. |
| `wages_total` | Wages including charges; `Wynagrodzenia z narzutami` / `Wynagrodzenia (z narzutami)`. | Observed 2019–2024; do not confuse with employee take-home pay. |
| `total_assets` | Total assets/balance-sheet total; `Suma bilansowa` / `Aktywa`. | Observed 2019–2024; stock at a reporting date. |
| `fixed_assets` | Fixed/non-current assets; `Aktywa trwałe`. | Observed 2019–2024; verify coverage before assuming exhaustive asset partition. |
| `current_assets` | Current assets; `Aktywa obrotowe`. | Observed 2019–2024; same partition caveat. |
| `equity` | Equity; `Kapitały własne`. | Observed 2019–2024; zero or negative equity can be valid. |
| `zobowiazania_i_rezerwy_na_zobowiazania` | Literal retained source field for liabilities and provisions; space-separated Polish label is slugified. | Observed only in 2024, 2,520 values. |
| `zobowiazania_dlugoterminowe` | Long-term liabilities, literal canonical spelling. | Entirely unavailable; no substitute is used. |
| `zobowiazania_krotkoterminow` | Short-term liabilities, literal canonical spelling. | Entirely unavailable; retain the existing spelling. |
| `liabilities_provisions` | `Zobowiązania_i_rezerwy_na_zobowiązania` maps to this name in upstream translation. | Observed only 2020–2023; 2,526 / 2,526 / 2,525 / 2,524 values respectively. |
| `total_liabilities` | `Zobowiązania razem`. | Observed only in 2019, 2,525 values. Do not assume this is the complete equity-balancing aggregate without checking the source definition. |

The three alternative aggregate liability columns are **not interchangeable by default**. Their year-dependent coverage is a source-schema transition, not evidence of zero debt. Long-term plus short-term debt need not equal an aggregate that also includes provisions or other items.

All non-sales annual financial/employment values are missing in 2018. In addition, `income_tax` and the two maturity-specific liability columns are missing in every year. These are expected source-unavailability categories, reported in coverage summaries rather than thousands of individual invalid-data findings. The 2023 operating-result block must be explicitly labelled `SOURCE_BLOCK_ABSENT`, not imputed from another profit measure.

Export coverage gives an example of variable-specific completeness: 2019–2024 nonmissing counts are **2,502; 2,506; 2,510; 2,512; 2,514; 2,506**. Missing exports must not become zero exports.

Units are currently not encoded per financial observation in the canonical schema. Header inspection alone did not establish a universal monetary scale/currency. Confirm currency and units before using absolute monetary thresholds; do not presume that every amount is PLN thousands. Ratios cancel units only when numerator and denominator have matching units and currency.

### 2.2 Identifiers, descriptors and source boundaries

Core has two keys (`nip`, `year`) and 16 descriptor columns:

`company`, `rank_2019`, `in_rank_2019`, `pkd`, `pkd_description`, `sector`, `sector_en`, `manufacturing`, `owner_type`, `owner`, `owner_num`, `city`, `regon`, `krs`, `legal_form`, `sj`.

- `nip` is the existing text firm key. Some stored values are shorter than ten digits (for example `111962` for Pepco Poland and `1011527` for Anro-Trade). They require identification review; they must not be padded, replaced, merged or rejected as firms solely because they fail a tax-number pattern.
- `regon`, `krs`, `pkd`, `owner_type` and ranks are numerically coerced and rounded by Core. Leading zeros, source formatting and non-integer input are therefore recoverable only from upstream material.
- `in_rank_2019 = 1` when rank is nonmissing; absence means 0, not a validated claim about ranking eligibility.
- `owner = Foreign` if rounded ownership-code text begins with `5`; otherwise `Domestic`; `owner_num` is 1/0 accordingly. **Missing ownership codes currently fall into Domestic**, so missing source ownership must be separately visible.
- `manufacturing` uses the first two digits of the padded, rounded PKD code, divisions 10–33. `sector_en` uses the 14-entry `SECTOR_EN_MAP`; unmapped sectors remain missing. Sector and manufacturing status are different concepts.
- Missing stable descriptors in Core are filled using the first observed nonmissing 2019–2024 value in input order. Existing nonmissing values are not overwritten. Period selects the first nonmissing value after sorting by firm/year from 2019 onward. Neither establishes that a value was historically true in 2018 or that a firm's descriptors never changed.
- `sj` comes from a **single static source `S/J` column**, then is repeated/filled. Observed values are `S`, `J` and missing. Confirm the code dictionary before treating them as consolidated/separate. Even with that confirmation, a static code cannot prove the reporting boundary for every variable in every year. `GK` in a name is supporting evidence only.

Period retains `nip` plus 13 descriptors: `company`, `rank_2019`, `in_rank_2019`, `pkd`, `pkd_description`, `sector`, `sector_en`, `manufacturing`, `owner_type`, `owner`, `owner_num`, `city`, `legal_form`. It does not retain `sj`, `regon` or `krs`; boundary reviews must join to Core and upstream sources without changing Period.

### 2.3 Annual derived variables: Data Core

There are **23** derived columns. Ordinary ratios divide raw numerator by denominator; missing inputs or a zero denominator give missing output. Negative nonzero denominators are currently allowed. Logs require a strictly positive argument. No canonical variable is winsorised.

| Column(s) | Actual builder formula / dependency |
| --- | --- |
| `price_index` | Fixed year lookup, 2019 base 1: 2018 .977517106549; 2019 1; 2020 1.034; 2021 1.086734; 2022 1.243223696; 2023 1.384951196; 2024 1.434809439. |
| `sales_real` | `sales / price_index`; same source monetary scale, expressed in 2019-price units. |
| `ln_sales`, `ln_total_assets`, `ln_employment` | Natural log of the corresponding positive raw value; otherwise missing. |
| `profit_margin` | `net_profit / sales`. |
| `operating_margin` | `operating_result / sales`. |
| `export_ratio` | `exports / sales`; decimal fraction, not percent points. |
| `asset_turnover` | `sales / total_assets`; flow divided by closing stock, not average assets. |
| `capital_ratio` | `equity / total_assets`; do not substitute another financing measure. |
| `equity_multiplier` | `total_assets / equity`. |
| `roa` | `net_profit / total_assets`, closing-asset denominator. |
| `roe` | `net_profit / equity`, closing-equity denominator. |
| `depreciation_ratio` | `depreciation / total_assets`. |
| `wage_intensity` | `wages_total / sales`. |
| `sales_per_employee` | `sales / employment`, with annual-average FTE denominator. |
| `assets_per_employee` | `total_assets / employment`. |
| `sales_growth_yoy` | `sales / preceding observed firm-row sales - 1`, only if that lag is positive. Current sales need not be positive. |
| `sales_real_growth_yoy` | Same calculation using `sales_real`. |
| `sales_log_growth_yoy` | Difference between current and preceding observed row's `ln_sales`; both underlying sales must be positive. |
| `has_sales`, `has_assets`, `has_employment` | 1 when respective raw value is positive, otherwise 0; missing, zero and negative values are conflated in these flags. |

**Annual-growth implementation caveat:** Core sorts by firm/year and uses a row shift without explicitly requiring a one-year gap. It also derives variables **before** dropping invalid keys and deduplicating firm-years. Consequently, a retained growth value could depend on a discarded duplicate or a non-adjacent year. The currently balanced output does not eliminate the need to check the pre-cleaning input. The audit should independently recompute genuine adjacent-year growth from the canonical unique rows and report a mismatch; any future correction to the builder requires approval.

### 2.4 Period financial and analytical variables: complete family inventory

The following families account for all **134** current columns: 14 identifiers/descriptors, 18 period outcomes, 18 lag measures, 45 start covariates, 6 availability indicators, 22 trajectory fields, 5 full-horizon/classification fields, and 6 performance classifications. Templates below enumerate every family member by replacing `P` with `P1`, `P2`, `P3` and `f` with `r`, `n`; they are not proposed new columns.

Actual current names use `rgrowth_*` and `ngrowth_*`. Repository instructions also contain older `growth_real_*` / `growth_log_*` examples and prohibit trajectory fields, whereas the current builder and dataset explicitly contain both families and trajectory descriptors. This is a documentation/governance discrepancy for researcher reconciliation, **not an instruction to rename or remove fields during the audit**. The audit must inventory the actual builder schema.

Let `S_t = sales` from Core year t, `R_t = S_t / price_index_t`, and `X_t = S_t` for nominal (`n`) or `R_t` for real (`r`). Endpoints: P1 2019→2020 (d=1), P2 2020→2022 (d=2), P3 2022→2024 (d=2). Intermediate 2021/2023 values do not enter these endpoint calculations but remain relevant to temporal checks.

| Family and number | Definition / original-source lineage |
| --- | --- |
| `rgrowth_P1/P2/P3`, `ngrowth_P1/P2/P3` (6) | `X_end / X_start - 1`, both endpoints strictly positive; otherwise missing. These are total period simple rates, not annualised. |
| `rgrowth_log_P1/P2/P3`, `ngrowth_log_P1/P2/P3` (6) | `ln(X_end) - ln(X_start)`, both endpoints positive. |
| `rgrowth_log_ann_P1/P2/P3`, `ngrowth_log_ann_P1/P2/P3` (6) | Corresponding log growth divided by d. Log points per year, not CAGR percentage. |
| `lag_rgrowth_P1`, `lag_rgrowth_log_P1`, `lag_rgrowth_log_ann_P1`, and nominal analogues (6) | Same three growth definitions for 2018→2019, duration 1. 2018 sales comes through the `przychody` fallback. |
| `lag_{f}growth_P2`, `lag_{f}growth_log_P2`, `lag_{f}growth_log_ann_P2` (6) | Exact copies of respective P1 outcomes, separately for real/nominal. |
| `lag_{f}growth_P3`, `lag_{f}growth_log_P3`, `lag_{f}growth_log_ann_P3` (6) | Exact copies of respective P2 outcomes; annualised lag retains P2 duration 2. |
| `{base}_start_P1/P2/P3` (45) | Direct copies of the 15 Core variables listed below at 2019, 2020, 2022 respectively. No averaging, filling from other years or independent ratio recalculation. Raw inputs and ratio/log formula are those in §2.3. |
| `has_rP1_data/has_rP2_data/has_rP3_data`, nominal `has_nP1_data/has_nP2_data/has_nP3_data` (6) | 1 exactly when both respective endpoint sales are present and positive. |
| `has_complete_rtrajectory`, `has_complete_ntrajectory` (2) | All three respective simple period growth values nonmissing. |
| `rP1_sign/rP2_sign/rP3_sign`, `nP1_sign/nP2_sign/nP3_sign` (6) | D if growth <0, G if growth ≥0; missing if growth missing. Zero is G. |
| `rtrajectory_3step`, `ntrajectory_3step` (2) | Join all three signs with hyphens only when complete. |
| `rtrajectory_label`, `ntrajectory_label` (2) | D-D-D Persistent decline; D-G-G Recovery; D-D-G Late recovery; G-G-G Consistent growth; G-D-D Deterioration; D-G-D Instability; G-D-G Volatile growth; G-G-D Late deterioration. |
| `rtrajectory_group`, `ntrajectory_group` (2) | D-D-D Persistent decline; G-G-G Consistent growth; other six patterns Interrupted trajectory. |
| `rindex_2019/2020/2022/2024`, `nindex_2019/2020/2022/2024` (8) | Start at 100 only for complete trajectory; recursively multiply by `1 + growth_P1`, `1 + growth_P2`, `1 + growth_P3`. Thus equal `100 × X_t / X_2019` on complete trajectories; otherwise all missing. |
| `rgrowth_ann_2019_2024`, `ngrowth_ann_2019_2024` (2) | Five-year CAGR `(X_2024 / X_2019)^(1/5) - 1`, positive endpoints. |
| `rgrowth_log_ann_2019_2024`, `ngrowth_log_ann_2019_2024` (2) | `(ln(X_2024) - ln(X_2019))/5`, positive endpoints. |
| `SGrowth_NR` (1) | Positive nominal and real 2019/2024 endpoints required. R2 if real CAGR ≥0 and nominal CAGR >.20; R1 if real ≥0 and nominal ≤.20; N1 if real <0 and nominal ≥0; N2 if nominal <0. Reproduce assignment order as coded. |
| `RPerf_Q_all/rank2019/manu2019`, `NPerf_Q_all/rank2019/manu2019` (6) | Classification of real/nominal five-year CAGR using fixed benchmark p10/p90. All valid endpoint firms receive classifications, including firms outside a particular threshold benchmark. See below. |

The 15 start-covariate bases are `ln_sales`, `ln_total_assets`, `ln_employment`, `profit_margin`, `operating_margin`, `export_ratio`, `asset_turnover`, `capital_ratio`, `equity_multiplier`, `roa`, `roe`, `depreciation_ratio`, `wage_intensity`, `sales_per_employee`, `assets_per_employee`. This yields all 45 actual `{base}_start_P*` columns.

Performance benchmark construction uses positive nominal/real 2019/2024 endpoints: `all` includes all valid firms; `rank2019` adds `in_rank_2019 == 1`; **`manu2019` adds both ranking membership and manufacturing**, not all manufacturing firms. Manual analytical exclusions are not used by this builder. Quantiles use pandas' default linear interpolation; p10/p90 are cutoffs, not tail means.

The workbook's separate `Performance_Thresholds` sheet has six rows and nine columns: `growth_type`, `benchmark_group`, `performance_column`, `p10`, `p90`, `n_used_for_threshold`, `mean_growth_bottom10`, `mean_growth_top10`, `mean_growth_population`. Tail means use values ≤p10 / ≥p90, population mean uses all eligible benchmark observations; all are unweighted, untrimmed CAGR means. Ties can produce more than 10% in a tail.

Performance assignments are sequential: Bottom 10% for `g ≤ p10`; Top 10% for `g ≥ p90`; Moderate decline for `p10 < g < 0`; Moderate growth for `0 ≤ g < p90`. These conditions need not form a disjoint partition for every possible distribution (e.g. negative p90 or positive p10). Audit both reproduction of **actual assignment order** and a separate check of overlapping conditions. No new priority rule is imposed by this specification.

## 3. Existing quality controls and limitations

| Layer | Existing control | Audit treatment / limitation |
| --- | --- | --- |
| Upstream parsing | Rebuild two-row headers; recognise missing markers; remove spaces/NBSP; decimal comma conversion; unparseable numeric strings become NaN. Strings containing both comma and dot have commas removed. | Preserve original strings to identify coercion loss and ambiguous mixed-separator formats; conversion is not proof of correctness. |
| Upstream reshaping | Translate source labels; `pivot_table(..., aggfunc='first')`; descriptor duplicates keep first NIP. | Distinct labels mapped to one variable/year or duplicate entities may collapse silently. Review before-pivot source cells; final canonical data cannot reveal all lost alternatives. |
| Upstream selection | Notebook explicitly removes NIPs `7740001454`, `5260152146B`, `5262297860B`, `7521457964B` (Orlen/holding duplicates); retains full-year-coverage firms and firms with positive 2019 sales. | These are existing source-population selections, distinct from shared analytical exclusions. Record them and reconcile lineage; inspecting notebook code does not by itself establish the complete executed provenance of every supplied file. |
| Upstream sanity checks | Reports exports >sales, negative employment/assets/wages, equity >assets, current/fixed assets >total; missingness, duplicates and year coverage. | Reuse rule definitions/report lineage where appropriate; these diagnostics do not implement those financial-value exclusions. Reclassify conditional checks carefully rather than treating notebook labels as proof of invalidity. |
| Core | Required input columns; expected output schema/year set; some 2018 sales/CPI required; sector reference required; numeric coercion; strings trimmed; missing-key rows dropped; duplicates retain most nonmissing then company sort. | Key/year coverage tests are global, not assurance of firm-by-firm completeness. Capture rejected/coerced/duplicate source records in a read-only comparison rather than repeating their removal. |
| Core calculations | Zero-denominator protection; positive-log domain; positive-lag simple growth; positive-value availability indicators; CPI lookup. | No general finite-value assertion, accounting reconciliation, reporting-unit or source-boundary validation. Derivation-before-deduplication and row-shift gaps need explicit audit checks. |
| Period | Duplicate input keys rejected; exact global years and 2018 sales required; year extracts and joins checked one-to-one; duplicate output firms/columns rejected; required families/sector/lag order checked. | Independently verify formula lineage and firm coverage; do not duplicate schema tests under several rule IDs. |
| Period calculations | Strictly positive endpoints for growth/logs/CAGR; unavailable calculations stay missing; counts of nonpositive endpoints; empty benchmark samples rejected. | Positive-domain ineligibility is not an economic-data exclusion or proof that raw zero sales is invalid. Inf may pass simple positivity checks, requiring a finite-value audit. |
| Analysis policy | Six shared manual exclusions before scenario/trajectory/complete-case filtering; numerical coercion and model-specific missing-value removal. | Audit records policy membership without applying it; missing regressors and unknown source values remain separate from suspected errors. |
| Analysis transformations | OLS has baseline/std/winsor/winsor_std; quantile uses winsor_std; severe growth models use principal winsor_std, binary logit response is not winsorised. Only continuous growth outcomes are clipped at sample-specific 1st/99th percentiles; predictors are not winsorised. Continuous standardisation and interaction centring use estimation samples. | Model transformations do not validate raw financial data. Audit untransformed canonical values; do not reuse regression winsorisation bounds as invalidity thresholds. VIF, leverage and regression influence stay model diagnostics. |

The six shared analytical exclusions and reason codes are Orlen (`7740001454`, M_AND_A), Ignitis (`5252714003`, RANK2019_MISCLASSIFIED), Elektrobudowa (`6340135506`, LIQUIDATION), Henryk Kania (`7440003325`, LIQUIDATION), Globus (`7773261746`, HOLDING_NONCONSOLIDATED), Ordipol (`6772001669`, LIQUIDATION). Shared matching uses verified NIP **or exact company name**, trimming outer whitespace. Canonical files are not filtered by this configuration; Orlen is already absent upstream. Display reason codes as existing researcher policy, not as automatically proven accounting errors or a mandate to remove other liquidation/holding firms.

## 4. Classification, severity, prerequisites and tolerances

Every actual finding has exactly one classification per rule:

| Classification | Meaning | Permitted consequence in this audit |
| --- | --- | --- |
| INVALID | Demonstrated violation of a key/schema, mathematical domain, or confirmed documented variable constraint. | Report the specific offending stored value or representation and evidence. No correction or deletion. |
| SUSPICIOUS | Potential accounting, source, identity or comparability problem requiring review. | Request evidence/decision; retain observation. |
| EXTREME | Unusual value or change under an explicit statistical/economic screening threshold, potentially valid. | Retain observation; provide context and possible sensitivity use. |

Rule type is separate: **H = hard constraint**, **C = conditional accounting/comparability check**, **S = statistical screening**. Severity is separate too: **critical** (schema/key/formula integrity), **high** (material source/scope inconsistency), **moderate** (coverage/economic review), **low** (minor discrepancy or screening). An EXTREME high-priority finding does not become INVALID because it might affect a regression.

Execution statuses are `PASS`, `FLAG`, `NOT_EVALUABLE`, `EXPECTED_UNAVAILABLE`. The last two are coverage statuses, not additional finding classifications. Store the reason when inputs, metadata or reference samples are absent. Missing optional/unavailable fields do not fail hard constraints. A record can have several findings with different rule IDs; link related findings instead of double-counting distinct observations/firms.

Proposed numerical conventions, subject to approval:

- Formula reproduction: `abs(stored - expected) ≤ 1e-10 + 1e-8 × abs(expected)`; compare missingness separately and reject infinity. Categories/keys/flags require exact agreement. Small finite-rounding discrepancies below this bound pass; display actual residuals.
- Accounting/rounding tolerance: where reported rounding steps `q_i` are known, use half the sum of the participating steps plus a numerical tolerance. Unknown precision uses a **review-only** fallback of `1e-6 × max(abs(participating values))`, with zero tolerance for an all-zero comparison. Unknown precision prevents declaring an accounting discrepancy INVALID. This fallback is not a materiality standard.
- Materiality for prioritising an accounting discrepancy: residual greater than 1% of the maximum absolute participating amount is high priority; smaller residual beyond tolerance is low/moderate review. An all-zero comparison has no percentage residual.
- Statistical references use finite, mathematically defined, unmodified canonical values; confirmed technical failures are ineligible for reference statistics, but manual regression exclusions do not remove firms from the audit. Record every eligibility criterion. References are variable×year for Core and variable×period/family for Period. Prefer sector strata with n≥30; otherwise pool the year/period, n≥30. Report both sector and pooled screens as distinct contexts, not duplicate rule hits. No automatic distribution screen when neither reference has n≥30.
- Robust score: `abs(x - median)/(1.4826 × MAD) > 5`. For strictly positive monetary levels, employment and per-employee measures, use `ln(x)`; signed profits/equity and signed ratios use raw values. If MAD=0, use outer fences `Q1 - 3×IQR`, `Q3 + 3×IQR`; if IQR=0, mark statistical screen not evaluable and use separate exact repetition/absolute rules. Do not invent a scale epsilon or winsorise the references.
- p1/p99 are descriptive context only. They do not classify 2% of every distribution as erroneous or independently justify exclusions.

## 5. Proposed rule catalogue

All thresholds below are proposals, not activated policy. Conditional rules flag SUSPICIOUS until their metadata and accounting assumptions are verified. A **separate confirmed-constraint rule** may classify INVALID after documentary verification; no automated reclassification based on researcher silence.

### 5.1 Schema, coverage, identity and lineage

| ID | Rule / threshold and justification | Class; type; severity | Location |
| --- | --- | --- | --- |
| D01 | Missing/extra required canonical columns, duplicate column labels, incompatible declared column type, missing required input file, or unexpected year outside 2018–2024. Exact schema from builders; nonintegral source year is not silently rounded for audit comparison. | INVALID; H; critical | Core/Period, dataset-level |
| D02 | Missing/blank/literal `nan`/`None` firm key, missing/nonintegral year, duplicate Core `(nip,year)` or Period `nip`. Distinguish identical duplicates from conflicting financial records and report all alternatives. Duplicate means invalid grain, not invalid financial amounts. | INVALID; H; critical | Both and pre-cleaning source |
| D03 | Original nonmissing financial token becomes NaN after parsing, unexpected text, or decimal/grouping separators have ambiguous interpretation. Coercion loss is an unresolved measurement, not proof of the underlying amount. | SUSPICIOUS; C; high | Source→Core |
| D04 | ±infinity in raw/derived numerical fields or nonfinite computed results stored as usable numbers. NaN is evaluated through missingness rules, not this rule. | INVALID; H; high | Both |
| D05 | Missing financial observation where a field is observed for that year in the source; report variable/year coverage and missing run. Exempt documented unavailable blocks (§2); flag disappearance/reappearance inside an otherwise observed firm series. | SUSPICIOUS; C; moderate | Core; propagate dependency to Period |
| D06 | Missing endpoint/start-year row or value makes a requested Period calculation unavailable. Record missing source year/field and blocked output names; distinguish missing, zero, negative and source-block absent. | SUSPICIOUS; C; moderate | Period lineage |
| D07 | NIP fails ten-digit pattern/checksum where declared to be a tax NIP, or appears to be a surrogate/KRS-like value; one REGON/KRS across several keys, one key across incompatible identities, near-matching names/addresses. Formatting/name similarity is a review lead only. | SUSPICIOUS; C; high | Core/source, Period joins |
| D08 | Conflicting company/REGON/KRS/legal-form/ownership/PKD/sj descriptors across years; first-nonmissing/fill selection obscures a conflict or source code was missing but derived ownership is Domestic. Retain original variants and fill provenance. | SUSPICIOUS; C; moderate/high if entity/scope changes | Core→Period |
| D09 | Present output descriptor/indicator differs from actual mapping/fill/selection rule; undocumented numeric rounding changes identifiers or source codes. Mapping mismatch is invalid; ambiguous source rounding is D03/D08 review. | INVALID; H; high | Both |
| D10 | Source labels collapse to the same firm-year metric through `aggfunc='first'`, or duplicate candidates differ beyond rounding tolerance. Existing first-value/most-complete resolution is not source verification. | SUSPICIOUS; C; high | Upstream/Core lineage |
| D11 | Canonical value differs from the documented selected panel input or permitted sales fallback beyond reproduction tolerance; key lost/added without an explained existing transformation. | INVALID; H; critical | Source→Core→Period |
| D12 | Available Excel data cells differ from canonical Parquet beyond numeric tolerance or exact categorical equality. Compare by key/variable; exclude formatting and unrelated annotation tabs. Neither version is automatically overwritten. | SUSPICIOUS; C; high | Both representations |

### 5.2 Domain, numerator–denominator and accounting checks

| ID | Rule / threshold and justification | Class; type; severity | Location |
| --- | --- | --- | --- |
| A01 | Employment <0, when confirmed as annual-average FTE; an FTE count cannot be negative. Fractional and zero values are allowed. | INVALID; H; high | Core |
| A02 | Negative total/fixed/current assets or liability aggregates, given unresolved source presentation. If a confirmed field definition excludes signed adjustments, create A02C with that evidence. | SUSPICIOUS; C; high | Core |
| A02C | Negative value demonstrably violates a confirmed nonnegative asset/liability variable definition with matching source scope; explicit documentation prerequisite. | INVALID; H; high | Core, disabled until definitions approved |
| A03 | Sales, exports, wages or depreciation <0. Revenue/cost adjustments or sign conventions may explain values; investigate instead of imposing positivity. | SUSPICIOUS; C; high for sales/exports; moderate otherwise | Core |
| A04 | Zero sales/assets/employment alongside nonzero exports, profits, wages or other substantive activity. Display all relevant amounts; zero raw values can be valid, but undefined ratios and comparability need review. | SUSPICIOUS; C; moderate/high | Core→Period |
| A05 | A ratio has numeric output despite missing numerator, missing denominator or zero denominator; or defined finite inputs produce the wrong ratio/missingness. Check each of the 12 ratio/per-employee formulas in §2.3. | INVALID; H; high | Core and copied Period covariates |
| A06 | Log is numeric with nonpositive/missing input, or positive finite input does not reproduce its log. Missing output caused by nonpositive input is expected domain restriction, not INVALID raw data. | INVALID; H; high | Both |
| A07 | Small denominator: positive sales/assets/employment below the peer reference p1, **and** its associated ratio exceeds a review threshold in A14/T05. For equity additionally `abs(equity)/abs(total_assets) < .01` with positive assets and `abs(roe)>1` or `abs(equity_multiplier)>100`. | SUSPICIOUS; C; moderate | Core; linked Period |
| A08 | `fixed_assets + current_assets` differs from total assets beyond accounting tolerance, or either component exceeds total assets beyond tolerance. Evaluate only common scope/date/units; missing components mean not evaluable. | SUSPICIOUS; C; high if >1% residual | Core |
| A09 | `total_assets - equity - L` differs beyond tolerance, where L is a **confirmed complete** liabilities/provisions aggregate selected through a year-specific source dictionary. Never sum alternative aggregate columns or treat missing ones as zero. | SUSPICIOUS; C; high if >1% residual | Core; dictionary approval required |
| A10 | `long-term + short-term liabilities` inconsistent with confirmed exhaustive liability partition, or component exceeds confirmed encompassing total. Provisions/other items must be separately documented. Presently not evaluable because maturity fields are absent. | SUSPICIOUS; C; moderate/high | Core |
| A11 | `profit_before_tax - income_tax - net_profit` differs beyond tolerance when all terms have a confirmed compatible definition/sign and any additional reconciling items are known. Presently not evaluable: tax absent. Do not substitute implied tax as observed tax. | SUSPICIOUS; C; moderate/high | Core |
| A12 | Algebraic identities fail reproduction tolerance on defined finite inputs: `roa = profit_margin × asset_turnover`; `roe = roa × equity_multiplier`; `capital_ratio × equity_multiplier = 1`; `sales_per_employee / assets_per_employee = asset_turnover`. Skip zero/missing intermediate denominators. | INVALID; H; high | Core; Period start covariates |
| A13 | Positive wages with zero employment, positive employment with zero wages, or large positive assets/sales with zero employment. Outsourcing, contractors, timing and reporting scope can explain these. | SUSPICIOUS; C; moderate | Core |
| A14 | Economic screening thresholds: `abs(profit_margin)>1`, `abs(operating_margin)>1`, `abs(roa)>1`, `abs(roe)>5`, `wage_intensity>1`, `depreciation_ratio>1`, `asset_turnover>20`, `abs(equity_multiplier)>100`. These broad limits prioritise review; they are not universal feasible bounds. A separate source/scope rule is needed to establish a suspected measurement problem. | EXTREME; S; moderate | Core; linked Period |
| A15 | `capital_ratio>1` merits a conditional balance-sheet/sign/boundary review against liabilities. Negative capital ratio is recorded as negative-equity context, not automatically a data-quality finding; an unusual magnitude can qualify separately under T05. No hard interval [0,1] and no automatic deletion. | SUSPICIOUS; C; moderate | Core; linked Period |

Profits, losses, equity and tax credits are signed quantities; there is no blanket rule that they must be positive. Margins/ROA/ROE may exceed ±100% when non-sales income, tiny denominators, distressed equity or holding activity is involved. Per-employee monetary plausibility limits cannot be specified in absolute currency units until metadata is confirmed. Use distribution screens and show raw amounts meanwhile.

### 5.3 Export intensity and reporting-boundary checks

| ID | Rule / threshold and justification | Class; type; severity | Location |
| --- | --- | --- | --- |
| E01 | `exports / sales > 1` for finite positive sales. Flag all exceedances; display `exports-sales` and relative excess. Within known source rounding tolerance is low-priority; >1.01 is high-priority review. Neither 1 nor 1.01 proves invalidity without scope confirmation. | SUSPICIOUS; C; low/high as stated | Core; propagate 2019/2020/2022 starts to Period |
| E02 | Negative exports (and resulting ratio), independent of sales sign. Verify corrections, gross/net basis, units and transcription. | SUSPICIOUS; C; high | Core; linked Period |
| E03 | Export ratio jumps by >.50 in absolute decimal units (50 percentage points) between consecutive years, or positive exports rise/fall by a factor >10 while positive sales change by less than factor 2. Ratios in fractions, not displayed percentages. | SUSPICIOUS; C; high | Core time series |
| E04 | Numerator and denominator have documented different entity/group boundaries, currencies, periods or units. Flag ratio comparability even if the quotient is within [0,1]. Missing metadata alone is `NOT_EVALUABLE`, not proof of different boundaries. | SUSPICIOUS; C; high | Core/source; Period lineage |
| E05 | Source documents affirm exports are a nonnegative subset of the same total sales on the same scope/units/period, but recorded exports <0 or >sales beyond documented rounding tolerance. The invalid object is the inconsistent measurement pair, not an assumed choice of which amount is wrong. | INVALID; C; high; documentary prerequisites mandatory | Core/source; disabled until verified |
| E06 | Quotient, copied starting ratio, percent/fraction conversion or displayed source units do not match the documented calculation. Stored ratio 2.04 means 204%, not 2.04%; checking workbook number formats alone is insufficient. | INVALID; H; high | Core→Period; export representation |
| E07 | `sj`/GK name/source status disagree; different variables or years demonstrably use consolidated versus separate accounts; M&A, liquidation, restatement or changed financial-year duration creates a boundary break. Names/status are leads; documentary mismatch gets high priority. | SUSPICIOUS; C; moderate/high | Core/source; all affected Period dependencies |

**No cap at 100%, replacement with 1, removal of negative exports, or new E&Y exclusion is proposed as an automatic action.** E01/E02 also cover observations that may be valid under a different reporting boundary. E05 is executable only after evidence establishes the subset relationship.

### 5.4 Temporal extremes, unit errors and Period integrity

| ID | Rule / threshold and justification | Class; type; severity | Location |
| --- | --- | --- | --- |
| T01 | For consecutive years and positive levels, `x_t/x_(t-1)>5` or `<.2` (increase >400% / decline >80%) in sales, assets, exports, employment or wages. Apply separately to nominal and real sales. Large valid expansion/distress is possible. | EXTREME; S; moderate | Core; link affected Period outcomes |
| T02 | For signed profits/equity use `abs(x_t-x_prev)/max(abs(x_t),abs(x_prev)) >1.5`, provided at least one value is nonzero; additionally show sign changes and movement scaled by available positive sales/assets. Near-zero percentage changes are not used. | EXTREME; S; moderate | Core |
| T03 | Possible units shift: consecutive positive magnitude ratio within ±5% of 100, 1,000, 1,000,000 or their reciprocals. Test isolated financial fields and at least three monetary fields changing together while ratios remain broadly stable. Test employment separately for FTE/headcount scale changes. Never rescale automatically. | SUSPICIOUS; C; high | Core/source |
| T04 | Temporary spike: positive `x_t/x_prev>10` and `x_t/x_next>10`, or temporary trough with both inverse comparisons >10; known annual periods required. Exact repeated vectors of ≥3 financial fields across ≥3 successive years, or two different firms in one year, also prompt copying/source review. Stable figures are not invalid. | SUSPICIOUS; C; moderate | Core/source |
| T05 | Robust score >5 or outer-fence fallback under §4. Covers all raw amounts, annual derived numeric variables and Period numeric families. Fixed lookup values and dummy/index base constants are excluded from meaningless distribution screens. | EXTREME; S; moderate | Core/Period, linked findings |
| T06 | Nominal/real sales do not reproduce CPI lookup/deflation, or positive-endpoint `1+real_growth` differs from `(1+nominal_growth) × CPI_start/CPI_end`. For log growth, nominal minus real equals `ln(CPI_end/CPI_start)`; annualised version divides by duration. | INVALID; H; high | Both |
| T07 | Annual growth uses a previous row from the same year/a non-adjacent year, or differs from adjacent-year recomputation on unique canonical rows. Absent preceding year means adjacent YOY undefined. Report both current builder lineage and canonical recomputation; do not silently fix. | INVALID; H; high for a stored value labelled YOY | Core/source |
| P01 | Period simple/log/annualised log growth, five-year CAGR or its missingness differs from endpoint formulas and duration in §2.4. Numeric simple growth ≤−1 with strictly positive endpoints violates domain. Annual log growth has no −1 lower bound. | INVALID; H; high | Period |
| P02 | Any lag differs from the specified 2018→2019 calculation or preceding period's corresponding measure; start covariate differs from its exact Core start-year observation. | INVALID; H; high | Period |
| P03 | Availability, complete-trajectory, signs, joined patterns, labels/groups or chained indices differ from §2.4, including zero-growth sign handling and missingness. | INVALID; H; high | Period |
| P04 | `SGrowth_NR`, benchmark membership, cutoff sample size, p10/p90, tail/population means or saved classification differs from actual builder logic beyond tolerance. Recompute from full canonical benchmark, never regression samples or filtered Excel rows. | INVALID; H; high | Period and existing threshold tab |
| P05 | Performance label conditions overlap, p10=p90, cutoffs are entirely above/below zero, or tail counts differ from exactly 10%. Overlap/degenerate cutoffs need review; ties and interpolation-related tail counts are descriptive context, not invalid findings by themselves. | SUSPICIOUS; C; moderate for ambiguous labels | Period |
| P06 | Annualised log growth has `abs(value)>ln(5)` (annual factor above 5 or below .2); or positive chained/full-horizon endpoints show sharp intermediate reversal under T04. Threshold annualises unequal period lengths rather than comparing P1 and P2 totals directly. | EXTREME; S; moderate | Period with Core trajectory detail |
| P07 | `Σ(d_P × growth_log_ann_P)/5` differs from full five-year annualised log growth on complete endpoint observations; real/nominal chained indices disagree with corresponding raw endpoints. | INVALID; H; high | Period |

Statistical flags alone must remain EXTREME. T03/T04/E03 are source-comparability hypotheses and therefore SUSPICIOUS; do not present them as confirmed unit mistakes or copying. Multiple screening hits increase review priority, not certainty of invalidity.

## 6. Export investigation: current evidence and unresolved questions

The source-to-ratio calculations inspected for E&Y are:

| Firm / NIP | Year | Sales | Exports | Exports / sales | Current conclusion |
| --- | ---: | ---: | ---: | ---: | --- |
| Ernst & Young Usługi Finansowe Audyt sp. z o.o. Polska sp.k. GK, Warszawa / `5260207930` | 2019 | 224,796.38242 | 459,300 | 2.043182346 = 204.3182% | Correct quotient; incompatible with a same-scope nonnegative-sales subset interpretation, but that interpretation has not been verified. |
| Same firm | 2020 | 257,582.53880 | 519,749 | 2.017795936 = 201.7796% | Same unresolved source question. |
| Iglotex Dystrybucja Polska sp. z o.o., Gdynia / `6790175807` | 2020 | 654,301 | −2,324 | −.003551882 = −.3552% | Negative source export amount; correction/sign/scope question, not a division error. |

E&Y's 2019/2020 amounts are identical in the cleaned source workbook and the existing enriched/canonical pipeline. The cleaned workbook uses `Przychody ze sprzedaży__2019`, `Przychody z eksportu__2019`, `Przychody ze sprzedaży__2020` and `Przychody z eksportu (sprzedaż po zagranicę kraju)__2020`. Its static `S/J` value is `S`. E&Y's 2021 ratio is approximately 9.9747%, then 9.11095% in 2022. The break strengthens the need for original-report verification but does not identify a correct replacement amount. E&Y's 2018 sales base is 748,290 through the fallback, another comparability question for the P1 lag.

What is established: the >100% ratios are not introduced by ratio arithmetic in Core/Period. What is **not** established: whether sales, exports, or both are wrong; whether a consolidated/separate boundary mismatch, currency/scale mismatch, source transcription, different revenue definition or gross/net treatment explains the values. No original financial-statement evidence was established here to decide between those explanations. The right initial classification is SUSPICIOUS (E01/E02), not automatically INVALID (E05).

Required researcher/source investigation for each material export case:

1. Locate the original report/table for each amount, with page/cell, entity/group name, reporting date and whether later restated.
2. Confirm currency and scale separately for exports and sales; inspect the original numeric token and parsing.
3. Confirm whether exports represent export **sales revenue**, foreign revenue, another entity/group aggregate, gross trading turnover or a different statistic.
4. Confirm consolidated/separate basis separately for each value/year; verify the S/J code dictionary and the extent of its static applicability.
5. Check net credit notes/returns and the sign convention for negative exports, including Iglotex.
6. Compare neighbouring-year statements and merger/liquidation/boundary notes before deciding whether to correct a source value, mark a specific ratio unusable, restrict a period comparison or retain the observation.

Tiny export exceedances (for example a one-unit difference between two large rounded figures) should remain visible in E01 but receive low priority pending precision evidence. No universal 1% allowance converts a true mismatch into a valid observation. Conversely, ratios above 101% deserve priority without becoming mathematically invalid just because of their size.

## 7. Required display fields for flagged observations

Every finding needs a stable `finding_id`, `run_id`, `rule_id`, rule/version/threshold, classification, severity, execution status, prerequisite evidence, review status and linked finding IDs. Use one row per observation–variable/relationship–rule, with separate related-value rows when necessary. Firm-years and Period findings have different grains; missing/duplicate source keys need a source row identifier as well as any available firm key.

Minimum identity/context fields: original and canonical firm key, company, year or period start/end/duration, dataset/file, source sheet/row/cell or report page, REGON/KRS where available, sector/PKD, static S/J, confirmed annual reporting scope or `UNKNOWN`, manual-exclusion membership/reason as metadata, and whether any descriptor was filled.

| Check family | Original and calculated values to display |
| --- | --- |
| Missing/type/key | Original token, parsed value/type, missing-marker category, original header, missing block/row distinction, conversion step, expected type, all competing duplicate records and selection outcome. |
| Ratio/log/domain | Raw numerator/denominator, their signs/missingness, currency/unit/date/scope for each, stored ratio/log, independently recomputed value, difference/tolerance, zero/negative/near-zero denominator reason. |
| Exports | Sales and exports in original and canonical units; quotient in decimal and percent; absolute/relative excess over sales; neighbouring 2018–2024 values; source labels and static/verified annual reporting status. |
| Accounting | Every participating component, confirmed aggregate definition, reconciliation residual and scaled residual, rounding steps, accounting tolerance, alternative liability fields without substituting them. |
| Temporal/unit | Current/previous/next raw values and years, year gap, nominal/real change, factor/log change, sign change, suspected unit factor, monetary fields corroborating it, employment/scope/event notes. |
| Statistical | Raw value, screen transformation, reference membership/population n, median/MAD or quartiles/IQR, score/fences, p1/p99 context, all proposed thresholds and minimum-n/fallback status. |
| Period lineage | All endpoint and start-year raw inputs, CPI values, period length, stored/recomputed growth/lag/covariate/index/class, threshold benchmark eligibility and actual p10/p90/means/tail n. |

The report must make it possible to see whether a large quotient is driven by its numerator, its denominator, mismatched scope or a simple calculation error. Display original precision; percentage formatting is presentation only. Keep negative/missing/zero distinct and never place zero in a blank to improve readability.

Researcher review fields: `decision` (retain / source correction proposed / mark variable unusable proposed / period comparability restriction proposed / firm exclusion proposed / unresolved), `reason_code`, `evidence_reference`, `reviewer`, `reviewed_at`, affected variable/year/period scope, and approval status. These record proposals/decisions; they do not execute them. Use supplementary reason codes separately from existing manual-exclusion codes.

## 8. Proposed workbook: results_data_quality_audit.xlsx

No workbook is created at this stage. Proposed tabs (each within Excel's 31-character limit):

| Tab | Contents / grain |
| --- | --- |
| `README` | Purpose, full audit population, independence from models, classification/type/severity legend, no automatic exclusions, units caveats, navigation. Put existing manual exclusions at the end as supplementary information. |
| `Rule_Register` | One row per rule ID; test/formula, inputs, prerequisites, classification/type/severity, threshold/tolerance justification, enabled/proposed status, approval requirement, existing control mapped to it. |
| `Variable_Inventory` | One row per expanded current variable, including all 59 Core and 134 Period columns distinguished by dataset; raw-source headers/years, formula, units, missing/domain rules, dependencies, provenance limitations. Threshold-sheet fields listed separately. |
| `Coverage` | Variable×year or variable×period counts: rows expected/present, nonmissing, missing, zero, negative, nonfinite, expected-unavailable and not-evaluable reasons. Separate source-selection reconciliation from value missingness. |
| `Findings_Core` | Authoritative Core/source findings table at §7 grain, with all rule classifications filterable. |
| `Findings_Period` | Period integrity/dependency findings; links to originating Core findings so propagated flags are not mistaken for independent source errors. |
| `Related_Values` | Long table keyed to finding IDs; source values, prior/next years, numerator/denominator/components, stored/recomputed values, unit and scope evidence. Supports comparisons involving more than two values. |
| `Export_Review` | Filtered review view of export cases and neighbouring-year inputs; same finding IDs as authoritative tables. Explicit unresolved E&Y and negative-export cases. |
| `Units_Scope_Review` | Variable/year source dictionary and researcher evidence for units, currency, consolidation, fiscal duration, restatement and boundary events; unknown values remain unknown. |
| `Summary` | Counts by rule/class/severity/year/sector; unique affected firms/firm-years and number of findings shown separately; coverage denominators and linked propagation counts. No regression coefficients. |
| `Researcher_Decisions` | Review log with proposed action/scope/reason/evidence and approvals; preserve across audit reruns via stable keys or a separate decision input, never overwrite previous decisions silently. |
| `Run_Metadata` | Input/source/config hashes, builder and audit versions, rule version, timestamp, runtime/library versions, schema, counts, reference statistics and deterministic reference eligibility, unavailable external sources and execution errors. |

Use filterable Excel tables, frozen key columns/header rows, readable number/percentage formats and text identifiers. No external links or formulas that modify source workbooks; store computed findings as values with formulas documented in the register. Conditional formatting distinguishes classification from review priority. Never merge separate flags into a single vaguely coloured “bad firm” category. If a table exceeds Excel row limits, export a companion machine-readable findings file and record the split; do not truncate findings.

## 9. Proposed integration and reproducibility

**Future implementation only after review:** add a standalone `code_audit_data_quality.py`, with an audit-specific approved rules configuration. Do not use model-specific samples or call regression fitters. Do not place audit flags, exclusion decisions or results inside canonical schemas.

1. Read Core and Period Parquet files without mutation; record hashes, schema and counts. Optional source-provenance reads use panel, cleaned source and original tracker/report evidence with file-specific paths and hashes. If an upstream source is unavailable, continue applicable canonical tests and explicitly mark provenance checks not evaluable.
2. Reuse/import declared builder constants and metadata only where side-effect free. Perform formula verification independently of the functions that produced the values so it can detect implementation mistakes. Expand all variable templates into exact names.
3. Check pre-cleaning input keys, parsing, pivots and dropped records through read-only source comparisons. Canonical files alone cannot recover discarded duplicate alternatives, original tokens or excluded firms. No rebuilding or replaying upstream notebook mutations as part of an audit run.
4. Evaluate schema/domain/lineage prerequisites first; then conditional accounting/boundary rules; then independent reference statistics/screens. Keep full canonical population and report existing analytical policy flags rather than applying them. Reference exclusions must be solely stated technical/domain exclusions, not model missingness or manual sample masks.
5. Propagate dependencies explicitly: a 2019 sales finding can affect 2018→2019 lag, P1, full-horizon growth/performance, start covariates and chained indices; 2020 affects P1/P2 and P2 starts; 2022 affects P2/P3 and P3 starts; 2024 affects P3/full horizon. Intermediate 2021/2023 problems can affect YOY/comparability despite not entering period endpoints.
6. Export only audit artifacts and a manifest. Atomic output writing and a post-run unchanged-hash assertion protect original inputs. Reruns have deterministic row sorting and stable finding keys; a run timestamp belongs in metadata, not the stable key. Hash any external decisions dictionary and version the thresholds.
7. Builders may later expose an **optional after-build read-only audit call**, or the documented workflow may run the audit immediately after canonical builders. Preserve their existing validation output and hard schema checks. Approved technical failures may fail an audit validation stage; suspicious/extreme findings produce review reports. No new builder stopping rule or data removal is implied without approval.
8. Proposed verification before adoption: small fixtures for missing markers, zero/negative denominators, valid negative equity, fractional FTE, nonconsecutive years, conflicting duplicates, label collisions, unavailable blocks, unit factors, tied/overlapping quantiles, same/different reporting scopes, MAD/IQR zero and insufficient references; independent formula spot checks; unchanged source hashes; exact rule/variable coverage. These are future implementation checks, not tests run during this specification task.

Avoid duplicate controls by mapping each existing validation to one rule ID and recording `existing_control`, `independent_verification`, or `new_check` in the register. Repeated export/accounting views reference master finding IDs. Existing model diagnostics continue to answer model-specific questions; the audit need not regenerate them.

## 10. Researcher decisions required before implementation

| Decision | Why it is needed | Default until approved |
| --- | --- | --- |
| Monetary currency/scale/rounding by source variable and year | Enables unit checks, absolute thresholds and credible reconciliation tolerances. | Unknown metadata; relative review screens only. |
| Sales versus `przychody` equivalence, especially 2018 | P1 lag may combine a different revenue concept or reporting basis. | Preserve current mapping; flag unresolved lineage. |
| Exports subset definition and negative-export treatment | Required to distinguish E01/E02 suspicion from E05 confirmed inconsistency. | No cap, correction or exclusion. |
| S/J code dictionary and annual applicability | Static codes/names cannot establish every observation's scope. | Boundary unknown unless verified. |
| Year-specific liability dictionary, sign and balance-sheet coverage | Three aggregate labels and missing maturity detail cannot be casually combined. | Keep all fields separate; conditional identities not evaluable without evidence. |
| Nonnegative asset/liability and cost definitions; profit-tax reconciliation | Determines which conditional violations are demonstrated constraints. | A02/A03 suspicion; A02C disabled; absent tax check not evaluable. |
| Surrogate identifiers, checksum use, duplicate source-selection policy | Avoid false firm deletion/merging and loss of legitimate group records. | Preserve current keys and all audit evidence; no fuzzy merges. |
| Missing-block expectations, upstream removals and input provenance | Existing filtering means canonical completeness is selected, not universal source coverage. | Report source selection and source-block missingness separately. |
| Review thresholds, robust reference groups, n≥30 and tolerances | Proposed screening values are transparent starting points, not scientific or accounting universals. | Treat all as proposed; no production flags/exclusions applied. |
| Calendar adjacency and pre-deduplication growth calculation | Current code can attach growth to an unintended comparison row. | Document risk; do not fix builder or rebuild. |
| Performance assignment priority when conditions overlap | Current sequential conditions can overwrite tail labels under some distributions. | Audit actual code and surface ambiguity; do not change classifications. |
| Repository instructions versus actual current Period schema | Older naming/trajectory restrictions differ from authoritative current builder/output. | Preserve actual variables; seek explicit reconciliation before any schema change. |
| Observation-, variable-, period- or firm-level action after source review | A suspect 2020 export need not invalidate all of a firm's outcomes or years. | Record review proposals only; existing six-company policy remains as configured. |

Approval should specify which rules may be implemented as reporting-only screens and which require additional source evidence before execution. **Approval of audit reporting does not authorise exclusions, imputations, clipping, source corrections or regression changes.** Those require distinct scoped decisions.

## 11. Possible regression and sensitivity implications

These are future options, not actions taken or changes requested by this specification:

- Maintain a documented baseline using current canonical inputs and existing six-company analytical exclusions. Compare approved alternatives only after source review; no new “any flag means exclude firm” rule.
- Distinguish a demonstrably wrong ratio calculation from a valid extreme financial amount and an unresolved scope mismatch. Correcting a derived formula, proposing a source amendment, making one regressor unavailable and excluding an entire firm have different implications and need separate approval.
- Missing or unusable exports primarily affect the export regressor and its size interaction, while a sales problem can affect outcomes, logs, margins, productivity, lags and complete-trajectory eligibility across several periods. Report all dependencies and reasons for resulting sample loss.
- If E&Y or another case is unresolved, approved sensitivity analyses could compare current values with a variable/year restriction or a documented comparable-scope alternative. Do not fabricate corrected values, automatically set exports equal to sales or make an unverified source inconsistency disappear through winsorisation.
- Compare raw and outcome-winsorised growth variants under the existing model policy. Outcome winsorisation does not address erroneous predictors; predictor clipping would be a new analytical specification requiring approval.
- Separate deletion from rescaling: use a comparison holding baseline means/SDs and outcome clipping limits fixed to isolate observation changes, and an additional full-pipeline rerun with recomputed sample transformations to show actual revised analyses. Report both in raw interpretable units where possible.
- Tabulate retained/lost firms by year, sector, rank, manufacturing, ownership, trajectory and missingness reason. Flagged/selected missingness can alter representativeness; it is not solved by statistical significance or larger fit statistics.
- Keep VIF/leverage/Cook's distance and quantile solver diagnostics downstream. An influential valid observation remains valid; a source error need not be influential to warrant correction. Similar numerical results after deleting a suspect record do not verify its source accuracy.

## 12. Completion boundary

This deliverable is the audit **design and rule specification only**. No audit workbook, new executable rule configuration, exclusions, source corrections, caps, dataset rebuilds or regression reruns have been produced. Implementation and any data/sample action await researcher review and explicit approval.
