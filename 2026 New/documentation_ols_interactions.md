# Stage 2 current sample policy

All interaction specifications consume the same centrally approved IDs as OLS and quantile models: ALL 2,332; manufacturing 949; RANK2019 1,748; ranking manufacturing 754. Summary rows retain model/population/sample/specification IDs, N, unique companies and fingerprints. Statistical formulas below remain unchanged; historical numerical examples are labelled. See [GRIP quantitative workflow](GRIP_quantitative_workflow.md).

# Compact supplementary OLS interactions

Updated 8 October 2026. Active output: `results_ols_interactions.xlsx`. Run `python code_ols_interactions.py` to refresh **only** this supplement; it never writes the primary OLS, diagnostics or canonical datasets.

## Workbook and model scope

Exactly four populated sheets:

1. `00_README`: purpose, common samples, separate specifications, periods, scaling, covariance and comparison interpretation. Detailed exclusions are central.
2. `01_EXPORT_SIZE`: familiar coefficient/stars/p-value tables for ALL, RANK2019, ALL_MANUFACTURING and RANK2019_MANUFACTURING; P1/P2/P3/FULL; standardised baseline before standardised winsorised models. All original controls, sector effects and fit rows retained.
3. `02_PROFIT_MANUFACTURING`: the separate profitability specification for ALL and RANK2019 only, with profit-margin main effect, Manufacturing main effect, their product, all other controls/sector indicators, and N/R²/adjusted R².
4. `03_INTERACTION_SUMMARY`: 48 rows (32 export × size, 16 profitability × manufacturing) with product coefficients/p-values, N/fit, matched reduced-model adjusted R², and the applicable conditional profitability slopes with covariance-based inference.

ALL_MANUFACTURING is the display label for the existing shared engine key `MANUFACTURING`. No shared scenario filter is renamed or changed. ALL is the eligible complete nominal/real trajectory population according to the existing growth-mode configuration, not an unconditional census. The Stage 2 common-sample policy applies, including approved logical exclusions. Exact company IDs come from code_common_samples.py rather than period-specific complete cases.

Both specifications keep the established P1=2019→2020, P2=2020→2022, P3=2022→2024, FULL=2019→2024 annualised log-growth outcomes, starting predictors at 2019/2020/2022, 2018→2019 P1 lag, existing P2/P3 lags and no FULL lag. Current mode is nominal. Only the outcome is winsorised at estimation-sample 1%/99%; no predictor clipping is added. Ownership and sector controls, the production reference, and the shared covariance setting remain unchanged. The actual current setting is **nonrobust**; no unsupported assumption that existing outputs used a robust estimator is made. The generic engine still supports configured robust covariance, verified with HC3 contrasts.

## A: export ratio × log sales — unchanged

Configuration comes from the existing active `INTERACTION_METADATA` export/size definition. Both continuous inputs subtract their scenario-period complete-case means. Their product is independently standardised using its own ddof=0 SD in standardised models, exactly as before. Ordinary main effects retain existing treatment. P1 and FULL have the same 2019 source-column names but their own centring references. No profitability product or manufacturing main effect is added to A.

All four samples remain. The active supplement displays only its 32 standardised models. All 64 A models, including raw baseline/winsor variants, are still available in the engine and verified against the preserved engine on identical current company samples. A's standardised product coefficient has product-SD units; it is not a group slope difference or a simple percentage growth effect.

## B: profit margin × manufacturing — new, separate specification

The specification is:

`standardised growth = intercept + beta_profit × z(profit_margin) + beta_manufacturing × M + beta_interaction × [z(profit_margin) × M] + existing standardised controls + ownership/sector indicators + applicable lag`.

M is the existing `manufacturing` indicator, retained as 0/1. It is an independent classification, not replaced with a sector dummy. Sector controls remain. The export ratio remains an ordinary control, but **export × size is absent from B**. A and B are never combined. B is estimated only for ALL/RANK2019; it is never fitted in a manufacturing-only sample where M is constant.

A single full-estimation-sample profit mean and population SD are used for both groups. The continuous input is centred by the existing generic procedure and the product is divided by that **same profit SD**, not its own product SD. This is the new specification's explicit `scale_with='profit_margin'` metadata rule, with independent product standardisation disabled. Manufacturing is dummy metadata and is neither centred nor standardised. The raw verification models retain the centred raw profit×M product, so their raw profit slopes remain in compatible raw-profit units as well.

This constituent scaling is necessary to make the requested direct identities true:

- Non-manufacturing slope = β_profit.
- Manufacturing slope = β_profit + β_interaction.
- Difference = β_interaction; difference p-value = product p-value.

Independently standardising the product would instead require a profit-SD/product-SD conversion before adding coefficients. That alternative is deliberately not used for B. A is not changed. No subgroup-specific scaling is used. The outcome retains the existing full-estimation-sample standardisation (`standardise_dependent=True`); slopes are therefore in outcome-SD units per full-sample profit SD. A global profit SD may be much larger than typical manufacturing variation; a large conditional manufacturing coefficient therefore need not describe a typical within-manufacturing change. Interpret as a conditional association, not a causal recovery effect.

The full design must have full column rank, and adding M and its product must increase the rank by two relative to the same controls without those two columns. Both M groups must be present. Identification checks passed alongside the retained sector controls for every applicable current model. No sector is silently removed to achieve identification.

## Matched comparisons and inference

Each summary comparator is refitted through the **same `run_model_variant` engine** on precisely the interaction model's ordered firm IDs. Its outcome array, clipping limits and full-sample transformations are identical; only the product is removed. B's comparator keeps Manufacturing as a main effect. It is therefore not assumed to equal the primary workbook's additive model, which does not add that new main effect. The primary workbook is neither read as a substitute comparator nor rewritten.

For the manufacturing slope:

`variance(beta_profit + beta_interaction) = Var(beta_profit) + Var(beta_interaction) + 2 Cov(beta_profit, beta_interaction)`.

P-values/95% intervals use the full fitted covariance and the fit's t versus normal reference (`use_t`), not independent coefficient SEs or the product p-value. Tests compare the calculations with statsmodels' own `t_test` for nonrobust and HC3 inference. Non-manufacturing inference uses the corresponding profit-main contrast. Fit differences are in-sample conditional associations, not proofs of better causal specification.

## Architecture and preserved functionality

- `code_ols_interactions.py` owns compact presentation and two clearly separated local specifications. `PROFIT_METADATA` and the Manufacturing dummy metadata are local; shared `code_config.py` remains unchanged, so quantile, severe and diagnostics do not acquire B.
- `code_ols_scenarios.py` remains the common engine. Optional per-report `interaction_metadata`/`regressor_metadata`, dummy types and opt-in constituent scaling leave legacy defaults unchanged. No modelling code is forked into a second estimator.
- `python code_ols_scenarios.py` continues the general two-report workflow, now publishing the compact supplement by default. It also regenerates primary OLS as usual when explicitly run; **it was not run for this supplementary-only update**.
- `run_ols_reports(interaction_layout='full')` and `write_scenario_workbook()` retain the previous full presentation, 64 A models, raw/standardised variants, matched Model_Comparison, audit tabs and technical calculations for explicit legacy use. No previous fitting, comparison or coefficient extraction capability is deleted.
- The old full interaction report is preserved byte-for-byte in `archive/results_ols_interactions_2026-10-08.xlsx`, including existing reader notes/historical coefficient annotations. They are not inserted as additional columns or technical tabs into the compact public supplement. Existing archival records remain intact. Future transitions from a full report are archived before replacement; same-date collisions create a numbered snapshot, never silently overwrite the earlier record.
- Old full-report tests now exercise its writer in a disposable temporary workbook. This preserves legacy validation without publishing redundant technical outputs or changing the primary report.

## Validation and results

Run:

```sh
python code_ols_interactions.py
python code_check_ols_interactions.py
python -m unittest code_check_interaction_centring.CentringTests
```

The compact acceptance suite covers all scopes/periods/variants, no combined product model, all A coefficients/SEs/p-values/CIs/N/R² against the preserved engine on matched samples, common profitability scaling, identification, exact matched comparator firms/outcomes, conditional covariance and HC3 p-values, saved table contents/summary fields, four nonempty sheets, and protected-file/shared-configuration preservation. Raw fits remain checked in memory, not shown in the supplement.

The existing full-report suite remains runnable with `python code_check_ols_reporting.py`; historical unequal-sample migration checks are optional, while matched-sample numerical equivalence is now authoritative. its temporary full report retains old sheet, comparison and annotation validation. Representative compact sheet ranges are visually reviewed; frozen headings and sample/period columns, ordering and number formats are verified in the saved file.

Acceptance on 8 October 2026: **26 tests passed** (9 compact-report, 10 legacy-report and 7 centring tests). All 64 underlying A models reproduce the archived coefficients, standard errors, p-values, confidence intervals, N and fit statistics within numerical tolerance (1e-10). All 48 displayed reduced-model comparisons use identical observations and outcomes. All 32 underlying B models, including raw verification variants, pass identification checks. The archived full report is byte-identical to the pre-change workbook. Initial/final hashes confirm that primary OLS, diagnostics, canonical datasets and shared configuration were unchanged by this task. An independently modified severe-analysis workbook was outside this change and was not included in the commit.

Current displayed N is unchanged from A's preceding report: ALL P1/P2/P3/FULL 2,385/2,426/2,438/2,423; RANK2019 1,786/1,791/1,795/1,794; ALL_MANUFACTURING 979/995/1,000/995; RANK2019_MANUFACTURING 781/782/783/784. B currently has the same respective ALL/RANK2019 Ns. Both displayed variants share these Ns, and every reduced comparison matches exactly.

The following numerical examples describe the archived 8 October pre-Stage-2 workbook, not current production estimates. B results are sample/treatment dependent. For ALL winsorised models, the profitability-slope difference is approximately −0.313 (P1, p=.467), −3.282 (P2, p=.0033), −1.665 (P3, p=.132), −2.809 (FULL, p=.00060). RANK2019 winsorised differences are approximately −0.054 (p=.366), +0.035 (p=.565), −0.055 (p=.242), +0.008 (p=.892). These are full-sample-profit-SD slope differences; they should not be pooled into a uniform effect claim or read as percentage growth changes. Their large ALL magnitudes warrant the scaling/context caution above, not a new cleaning action.
