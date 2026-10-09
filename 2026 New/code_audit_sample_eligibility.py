"""Read-only sample audit. Never fit models, clean canonical data or write production reports.

Run from the project folder. Outputs a JSON presentation payload in /tmp and the
requested separate Markdown report. Workbook authoring is in the companion mjs.
"""
from pathlib import Path
from contextlib import redirect_stdout
from itertools import combinations
import hashlib
import io
import json
import numpy as np
import pandas as pd
import code_ols_scenarios as ols
import code_ols_interactions as interactions
import code_diagnostics_trajectories as diagnostics
import code_quantile as quantile
from code_config import manual_exclusion_mask, MANUAL_EXCLUSIONS, MANUAL_EXCLUSION_REASONS

PERIODS = ['P1', 'P2', 'P3', 'FULL']
ROOT = Path(__file__).resolve().parent
PAYLOAD = Path('/tmp/grip_sample_eligibility_audit.json')
TOLERANCE = 1e-10  # Float reconstruction tolerance, never an economic threshold.


def protected_hashes():
    paths = list(ROOT.glob('data_*')) + list(ROOT.glob('results_*')) + list(ROOT.glob('code_*'))
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths
            if p.is_file() and 'sample_eligibility' not in p.name}


def ids(frame):
    return set(frame.nip.astype(str))


def sample_hash(values):
    return hashlib.sha256('\n'.join(sorted(values)).encode()).hexdigest()


def plain(value):
    if pd.isna(value): return None
    if isinstance(value, np.generic): return value.item()
    return value


def existing_exclusion_reason(company, nip, scenario, rule):
    if rule == 'MANUAL_EXISTING':
        entry = next(e for e in MANUAL_EXCLUSIONS['companies'] if e['nip'] == nip or e['company'] == company.strip())
        return f"Existing manual exclusion [{entry['reason_code']}]: {MANUAL_EXCLUSION_REASONS[entry['reason_code']]}"
    if rule == 'SCENARIO_NOT_ELIGIBLE_EXISTING':
        return f'Outside {scenario} ranking/manufacturing scope; not a data-quality failure.'
    return 'Existing complete-trajectory requirement: one or more P1/P2/P3 growth observations unavailable.'


def main():
    before = protected_hashes()
    source = pd.read_parquet(ROOT / 'data_period_2018-2024.parquet')
    core = pd.read_parquet(ROOT / 'data_core_2018-2024.parquet')
    assert not source.nip.duplicated().any()
    assert not core.duplicated(['nip', 'year']).any()
    source['nip'] = source.nip.astype(str)
    core['nip'] = core.nip.astype(str)
    annual = core.set_index(['nip', 'year'])
    manual = manual_exclusion_mask(source)
    clean = source.loc[~manual].copy()
    name = source.set_index('nip').company.to_dict()
    saved = pd.read_excel(ROOT / 'results_ols_scenarios.xlsx', sheet_name='Model_Summary_Long')
    saved_quantile = pd.read_excel(ROOT / 'results_quantile.xlsx', sheet_name='Model_Summary_Long')
    saved_interaction = pd.read_excel(ROOT / 'results_ols_interactions.xlsx', sheet_name='03_INTERACTION_SUMMARY')
    saved_diag = pd.read_excel(ROOT / 'results_diagnostics_trajectories.xlsx',
                               sheet_name=['31_SCENARIO_DIAGNOSTICS', '90_CORR_LONG_ALL', '92_FIRM_TRAJECTORIES'])
    counts, overlaps, register, alignment = [], [], [], []
    grids, frames, issues, diagnostic_sets = {}, {}, {}, {}

    configs = {
        'OLS_PRIMARY': ols.normalise_config({**ols.CONFIG, 'include_interactions': False}),
        'OLS_EXPORT_SIZE': ols.normalise_config(interactions.specification_config('export_size')),
        'OLS_PROFIT_MANUFACTURING': ols.normalise_config(interactions.specification_config('profit_manufacturing')),
        'QUANTILE': quantile.shared_model_config(quantile.CONFIG),
    }
    diagnostic_config = ols.normalise_config(diagnostics.build_diagnostic_config('data_period_2018-2024.parquet'))

    def value(nip, year, variable):
        return plain(annual.loc[(nip, year), variable]) if (nip, year) in annual.index else None

    def validity(frame, period, spec, config):
        """Only evaluate prerequisites of this exact model, on untransformed values."""
        findings = []
        start, end = {'P1': (2019, 2020), 'P2': (2020, 2022), 'P3': (2022, 2024), 'FULL': (2019, 2024)}[period]
        start_suffix = ols.get_regressor_period_for_model(period)
        ratio_sources = {'profit_margin': ('net_profit', 'sales'), 'export_ratio': ('exports', 'sales'),
                         'asset_turnover': ('sales', 'total_assets'), 'capital_ratio': ('equity', 'total_assets'),
                         'sales_per_employee': ('sales', 'employment')}
        lag_years = {'P1': (2018, 2019), 'P2': (2019, 2020), 'P3': (2020, 2022)}
        years = {start, end}
        if ols.lag_growth_col(config, period) in spec['regressors']:
            years.update(lag_years[period])
        for row in frame.itertuples():
            nip = row.nip
            def add(variable, observed, rule, reason, year=None, numerator=None, denominator=None, classification='INVALID'):
                findings.append(dict(nip=nip, company=name[nip], period=period, variable=variable,
                                     observed_value=plain(observed), source_year=year, numerator=plain(numerator),
                                     denominator=plain(denominator), rule=rule, classification=classification, reason=reason))
            required = [spec['dependent'], *spec['regressors'], *ols.get_categorical_columns(config)]
            for col in required:
                v = frame.at[row.Index, col]
                if col not in ols.get_categorical_columns(config):
                    num = pd.to_numeric(v, errors='coerce')
                    if pd.isna(num): add(col, v, 'ESSENTIAL_MISSING', 'Existing model complete-case exclusion; missing or non-numeric essential value.', classification='MISSING')
                    elif not np.isfinite(num): add(col, num, 'NONFINITE', 'Essential model input is infinite.')
                elif pd.isna(v): add(col, v, 'ESSENTIAL_MISSING', 'Existing missing categorical-control exclusion.', classification='MISSING')
            for year in sorted(years):
                sales = value(nip, year, 'sales')
                if sales is None or not np.isfinite(sales) or sales <= 0:
                    add('sales', sales, 'SALES_POSITIVE', 'Positive finite sales required for outcome/lag logarithm.', year)
            for base, (num_col, den_col) in ratio_sources.items():
                col = f'{base}_start_{start_suffix}'
                if col not in spec['regressors']: continue
                num, den = value(nip, start, num_col), value(nip, start, den_col)
                observed = frame.at[row.Index, col]
                if den is None or not np.isfinite(den) or den <= 0:
                    add(den_col, den, 'DENOMINATOR_POSITIVE', 'Sales/employment/total assets must be positive for this required ratio.', start, num, den)
                if num is None or not np.isfinite(num):
                    add(num_col, num, 'SOURCE_MISSING', 'Required ratio numerator is missing or non-finite.', start, num, den, 'MISSING')
                if num is not None and den is not None and np.isfinite(num) and np.isfinite(den) and den != 0 and pd.notna(observed):
                    if not np.isclose(float(observed), num / den, rtol=TOLERANCE, atol=TOLERANCE):
                        add(col, observed, 'FORMULA_MISMATCH', 'Stored ratio differs from exact source formula (floating tolerance 1e-10).', start, num, den)
                if base == 'export_ratio' and pd.notna(observed) and not 0 <= observed <= 1:
                    add(col, observed, 'EXPORT_0_1_PROPOSED', 'Fails proposed export/sales [0,1] eligibility; accounting/reporting boundary remains unresolved.', start, num, den, 'CONDITIONAL')
            for col in ['owner_num', 'manufacturing']:
                if col in required:
                    observed = frame.at[row.Index, col]
                    if pd.notna(observed) and observed not in [0, 1]:
                        add(col, observed, 'BINARY_0_1', 'Documented indicator must be 0 or 1.')
        return findings

    # Source-universe early exclusions: explicit distinction from proposed rules.
    for scenario, query in ols.SCENARIOS.items():
        local = {**configs['OLS_PRIMARY'], 'sample_name': ols.SHARED_SAMPLE_SCENARIOS.get(scenario, scenario), 'base_sample_filter': 'True'}
        with redirect_stdout(io.StringIO()):
            scope = ols.apply_scenario_filter(clean, scenario, query)
            filtered = ols.apply_sample_filter(scope, local)
        for row in source.itertuples():
            rule = 'MANUAL_EXISTING' if manual.at[row.Index] else 'SCENARIO_NOT_ELIGIBLE_EXISTING' if row.Index not in scope.index else 'INCOMPLETE_TRAJECTORY_EXISTING' if row.Index not in filtered.index else None
            if rule:
                register.append(dict(specification='OLS_PRIMARY', scenario=scenario, nip=row.nip, company=row.company,
                                     period='ALL_MODELS', variable='company/sample/trajectory', observed_value=None, source_year=None,
                                     numerator=None, denominator=None, rule=rule, classification='EXISTING',
                                     reason=existing_exclusion_reason(row.company, row.nip, scenario, rule),
                                     exclusion_status='EXISTING', currently_in_regression=False, enters_any_current_regression=False,
                                     in_trajectory_diagnostics=False, in_regression_diagnostics=False, in_correlations=False))

    for specification, config in configs.items():
        models = ols.build_models(config)
        scenarios = list(ols.SCENARIOS) if specification != 'OLS_PROFIT_MANUFACTURING' else ['ALL', 'RANK2019']
        for scenario in scenarios:
            local = {**config, 'sample_name': ols.SHARED_SAMPLE_SCENARIOS.get(scenario, scenario), 'base_sample_filter': 'True'}
            frame = ols.apply_sample_filter(ols.apply_scenario_filter(clean, scenario, ols.SCENARIOS[scenario]), local)
            frame = ols.add_interaction_columns(frame, local, models)
            frames[specification, scenario] = frame
            current, valid = {}, {}
            for period, spec in models.items():
                sample = ols.get_estimation_sample(frame, local, spec)
                current[period] = ids(frame.loc[sample.index])
                # Independent complete-case reconstruction validates company IDs.
                cols = [spec['dependent'], *spec['regressors'], *ols.get_categorical_columns(local)]
                working = frame[cols].copy()
                nums = [x for x in cols if x not in ols.get_categorical_columns(local)]
                working[nums] = working[nums].apply(pd.to_numeric, errors='coerce')
                assert current[period] == ids(frame.loc[working.dropna().index])
                findings = validity(frame, period, spec, local)
                issues[specification, scenario, period] = findings
                bad = {r['nip'] for r in findings}
                valid[period] = current[period] - bad
                if specification == 'OLS_PRIMARY':
                    rows = saved.loc[saved.scenario.eq(scenario) & saved.period.eq(period)]
                    assert len(rows) == 4 and set(rows.observations) == {len(current[period])}
                elif specification == 'QUANTILE':
                    rows = saved_quantile.loc[saved_quantile.scenario.eq(scenario) & saved_quantile.period.eq(period)]
                    assert len(rows) == 3 and set(rows.observations) == {len(current[period])}
                else:
                    label = 'Export × size' if specification == 'OLS_EXPORT_SIZE' else 'Profitability × manufacturing'
                    sample_label = interactions.SAMPLE_LABELS[scenario]
                    rows = saved_interaction.loc[saved_interaction.interaction_specification.eq(label) & saved_interaction['sample'].eq(sample_label) & saved_interaction.period.eq(period)]
                    assert len(rows) == 2 and set(rows.N) == {len(current[period])}
            common_current, common_valid = set.intersection(*current.values()), set.intersection(*valid.values())
            grids[specification, scenario] = dict(current=current, valid=valid, common=common_valid)
            for period in PERIODS:
                counts.append(dict(specification=specification, scenario=scenario, period=period,
                                   current_N=len(current[period]), valid_N=len(valid[period]),
                                   newly_excluded_N=len(current[period] - valid[period]), common_sample_N=len(common_valid),
                                   additional_common_loss_N=len(valid[period] - common_valid),
                                   additional_common_loss_share=len(valid[period] - common_valid) / len(valid[period]),
                                   current_four_period_intersection_N=len(common_current),
                                   interaction_extra_missing_N=len(grids['OLS_PRIMARY', scenario]['current'][period] - current[period]) if specification != 'OLS_PRIMARY' else 0,
                                   variant_company_sets_identical=True, current_ID_SHA256=sample_hash(current[period]), valid_ID_SHA256=sample_hash(valid[period])))
            for state, sets in [('CURRENT', current), ('PROPOSED_VALID', valid)]:
                universe = set.union(*sets.values())
                for size in range(1, 5):
                    for combo in combinations(PERIODS, size):
                        intersection = set.intersection(*(sets[p] for p in combo))
                        overlaps.append(dict(specification=specification, scenario=scenario, state=state,
                                             periods=' & '.join(combo), unique_firms=len(intersection),
                                             union_N=len(set.union(*(sets[p] for p in combo))),
                                             intersection_ID_SHA256=sample_hash(intersection)))
                for pattern in sorted({''.join('1' if nip in sets[p] else '0' for p in PERIODS) for nip in universe}):
                    members = {nip for nip in universe if ''.join('1' if nip in sets[p] else '0' for p in PERIODS) == pattern}
                    overlaps.append(dict(specification=specification, scenario=scenario, state=state,
                                         periods=f'EXACT membership P1/P2/P3/FULL: {pattern}', unique_firms=len(members),
                                         union_N=len(universe), intersection_ID_SHA256=sample_hash(members)))

    # Build diagnostic samples from their own current configuration, without fitting.
    for scenario in ols.SCENARIOS:
        local = {**diagnostic_config, 'sample_name': ols.SHARED_SAMPLE_SCENARIOS.get(scenario, scenario), 'base_sample_filter': 'True'}
        models = ols.build_models(local)
        frame = ols.apply_sample_filter(ols.apply_scenario_filter(clean, scenario, ols.SCENARIOS[scenario]), local)
        frame = ols.add_interaction_columns(frame, local, models)
        for period in PERIODS:
            dset = ids(frame.loc[ols.get_estimation_sample(frame, local, models[period]).index])
            diagnostic_sets[scenario, period] = dset
            primary = grids['OLS_PRIMARY', scenario]['current'][period]
            proposed = grids['OLS_PRIMARY', scenario]['valid'][period]
            common = grids['OLS_PRIMARY', scenario]['common']
            tab31 = saved_diag['31_SCENARIO_DIAGNOSTICS']
            observed31 = tab31.loc[tab31.scenario.eq(scenario) & tab31.period.eq(period)]
            assert set(observed31.estimation_sample_n) == {len(dset)}
            corr = saved_diag['90_CORR_LONG_ALL']
            observedcorr = corr.loc[corr.scenario.eq(scenario) & corr.period.eq(period)]
            assert set(observedcorr.N) == {len(dset)}
            for category, diagnostic_ids, regression_ids in [
                ('Regression descriptives / correlations vs current primary OLS', dset, primary),
                ('Trajectory summaries / profiles vs current primary OLS', ids(frame), primary),
                ('Current regression diagnostics vs proposed valid OLS', dset, proposed),
                ('Current regression diagnostics vs proposed common OLS', dset, common)]:
                diff = diagnostic_ids - regression_ids
                alignment.append(dict(row_type='SUMMARY', scenario=scenario, period=period, diagnostic_scope=category,
                                      diagnostic_N=len(diagnostic_ids), regression_N=len(regression_ids),
                                      intersection_N=len(diagnostic_ids & regression_ids), diagnostics_only_N=len(diff),
                                      regression_only_N=len(regression_ids - diagnostic_ids), nip=None, company=None, variable=None,
                                      observed_value=None, reason=None))
                for nip in sorted(diff):
                    relevant_periods = PERIODS if 'common OLS' in category else [period]
                    own = [r for p in relevant_periods for r in issues['OLS_PRIMARY', scenario, p] if r['nip'] == nip]
                    reasons = '; '.join(sorted({r['rule'] + ': ' + r['variable'] for r in own}))
                    alignment.append(dict(row_type='COMPANY', scenario=scenario, period=period, diagnostic_scope=category,
                                          diagnostic_N=None, regression_N=None, intersection_N=None, diagnostics_only_N=None,
                                          regression_only_N=None, nip=nip, company=name[nip], variable='; '.join(sorted({r['variable'] for r in own})),
                                          observed_value=None, reason=reasons or 'Period complete cases differ; inspect exclusion register.'))

    all_current = set.union(*(g['current'][p] for g in grids.values() for p in PERIODS))
    saved_trajectory_ids = set(saved_diag['92_FIRM_TRAJECTORIES'].nip.astype(str))
    assert saved_trajectory_ids == ids(frames['OLS_PRIMARY', 'ALL'])
    for (specification, scenario, period), findings in issues.items():
        grid = grids[specification, scenario]
        for finding in findings:
            nip = finding['nip']
            register.append(dict(specification=specification, scenario=scenario, **finding,
                                 exclusion_status='NEWLY_PROPOSED' if nip in grid['current'][period] else 'ALREADY_EXCLUDED',
                                 currently_in_regression=nip in grid['current'][period], enters_any_current_regression=nip in all_current,
                                 in_trajectory_diagnostics=True, in_regression_diagnostics=nip in diagnostic_sets[scenario, period],
                                 in_correlations=nip in diagnostic_sets[scenario, period]))
    # Export outliers outside the trajectory sample (e.g. Vicis) still appear.
    for row in source.itertuples():
        for period in ['P1', 'P2', 'P3']:
            v = getattr(row, f'export_ratio_start_{period}')
            if pd.notna(v) and not 0 <= v <= 1:
                year = {'P1': 2019, 'P2': 2020, 'P3': 2022}[period]
                register.append(dict(specification='SOURCE_EXPORT_SCREEN', scenario='SOURCE_UNIVERSE', nip=row.nip,
                                     company=row.company, period=period, variable=f'export_ratio_start_{period}', observed_value=plain(v),
                                     source_year=year, numerator=value(row.nip, year, 'exports'), denominator=value(row.nip, year, 'sales'),
                                     rule='EXPORT_0_1_PROPOSED', classification='CONDITIONAL',
                                     reason='Proposed bound; no cap or correction. Check company-specific model/diagnostic membership rows.',
                                     exclusion_status='SOURCE_SCREEN_ONLY', currently_in_regression=any(row.nip in g['current'][period] for g in grids.values()),
                                     enters_any_current_regression=row.nip in all_current,
                                     in_trajectory_diagnostics=any(row.nip in ids(f) for (s, _), f in frames.items() if s == 'OLS_PRIMARY'),
                                     in_regression_diagnostics=any(row.nip in v for (s, p), v in diagnostic_sets.items() if p == period),
                                     in_correlations=any(row.nip in v for (s, p), v in diagnostic_sets.items() if p == period)))
    # Correct early-row any-model status (scope exclusions can enter other scenarios).
    for r in register:
        r['enters_any_current_regression'] = r['nip'] in all_current
    tables = {'01_SAMPLE_COUNTS': counts, '02_SAMPLE_OVERLAP': overlaps,
              '03_EXCLUSION_REGISTER': register, '04_DIAGNOSTICS_ALIGNMENT': alignment}
    assert before == protected_hashes(), 'Protected source/script/output changed during audit.'
    payload = dict(tables=tables, protected_sha256=before, source_firms=len(source), core_rows=len(core),
                   manual_removed=int(manual.sum()), manual_rules=MANUAL_EXCLUSIONS,
                   rules=['Required finite inputs; source prerequisites only for variables used by exact model.',
                          'Export [0,1] is a proposed conditional comparability restriction; unresolved reporting boundaries retained.',
                          'No 0–1 constraint for capital ratio, profitability, turnover or productivity.',
                          '2018 sales only, used solely for P1 lag; no 2018 employment/export/assets requirement.',
                          'Dependent-only winsorisation at 1%/99%; no new extreme-value exclusions.',
                          'Reconstruction tolerance rtol=atol=1e-10, not an economic cleaning threshold.'])
    PAYLOAD.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False, default=str))
    summary = pd.DataFrame(counts)
    primary = summary.loc[summary.specification.eq('OLS_PRIMARY')]
    primary_display = primary[['scenario','period','current_N','valid_N','newly_excluded_N','common_sample_N','additional_common_loss_N','additional_common_loss_share']].copy()
    primary_display['additional_common_loss_share'] = primary_display.additional_common_loss_share.map(lambda x: f'{x:.2%}')
    new_by_scenario = {s: len({r['nip'] for r in register if r['specification'] == 'OLS_PRIMARY' and r['scenario'] == s and r['exclusion_status'] == 'NEWLY_PROPOSED'}) for s in ols.SCENARIOS}
    common_by_scenario = {s: len(set.union(*grids['OLS_PRIMARY', s]['valid'].values()) - grids['OLS_PRIMARY', s]['common']) for s in ols.SCENARIOS}
    union_summary = pd.DataFrame([dict(scenario=s, distinct_newly_removed=new_by_scenario[s], distinct_additional_common_loss=common_by_scenario[s]) for s in ols.SCENARIOS])
    new_firms = {r['nip'] for r in register if r['specification'] == 'OLS_PRIMARY' and r['exclusion_status'] == 'NEWLY_PROPOSED'}
    text = ['# Sample eligibility and data-quality audit', '', 'Audit date: 9 October 2026. Audit only: no production scripts, source data, regression specifications or existing Excel outputs changed.', '',
            '## Exact primary OLS counts', '', primary_display.to_markdown(index=False), '',
            f'Strict model-relevant proposed rules remove {len(new_firms)} distinct firms from at least one current primary OLS model (not the sum of scenario-period counts). All newly proposed removals are export-ratio bound failures. No other rule adds removals beyond current complete cases. These firms remain in the canonical datasets.', '',
            union_summary.to_markdown(index=False), '', 'Distinct additional common loss counts firms eligible in at least one proposed-valid period but not all four. It is not a marginal loss for any single period.', '',
            '## Rules and existing controls', '', *['- ' + x for x in payload['rules']], '',
            f'Canonical period input: {len(source):,} unique firms; annual core: {len(core):,} firm-years, 2018–2024. Keys are unique. Shared manual configuration removes {int(manual.sum())} firms present in the input; Orlen is already absent.', '',
            'Selection order: upstream source preparation → shared manual exclusions (NIP or exact stripped name) → ranking/manufacturing scenario → complete nominal P1/P2/P3 trajectory → model-specific complete cases → outcome winsorisation → scaling. FULL starts in 2019, ends in 2024 and has no lag. P1/P2/P3 lags and controls remain exactly as configured. Missing/non-numeric numeric inputs are coerced to NaN; no zero imputation. The current model helper uses dropna, without an explicit infinity screen.', '',
            'Upstream notebook manual removal list: Orlen 7740001454 and holding-duplicate IDs 5260152146B, 5262297860B, 7521457964B. The notebook also retains firms with all observed panel years and strictly positive 2019 sales; these existing source-population selections are separate from proposed rules. Notebook code inspection is not proof that every changed upstream file was regenerated in that order. These absent records are not reinstated. Current canonical/analysis scripts contain no additional sector exclusion list: sector is a categorical control; manufacturing/ranking are scenario filters. This audit cannot recover firms absent from upstream input.', '',
            'Core builder: coerces annual numeric inputs, drops missing NIP/year keys, and resolves duplicate firm-years deterministically by greatest non-missing coverage then company name. Missing source columns remain missing. Ratios require a nonzero denominator, but current safe-ratio logic permits negative nonzero denominators. Logs require positive arguments. Period builder requires positive endpoint sales for log/trajectory calculations; copies covariates from 2019/2020/2022, computes P1 lag from 2018–2019 and later lags from prior defined periods. Proposed prerequisites are evaluated only where a value is used in the exact model; a bad unused annual observation does not remove an entire firm automatically.', '',
            '## Diagnostics and model alignment', '',
            'Primary OLS is additive. Diagnostics and quantile retain the active export × size product; the interaction supplement has separate export × size and profitability × manufacturing specifications. Their exact missingness effects are displayed separately in 01_SAMPLE_COUNTS. Ordinary predictor transformations and dependent-only winsorisation do not change selected company IDs. All four OLS variants use the same selector; saved primary Ns agree for all 64 model rows. Quantile Q10/Q50/Q90 share complete cases. Severe decline uses additive P1/P2/P3 complete cases in the two ranking scenarios, plus the observed P1 group indicator; it has no FULL model. No severe refit was performed.', '',
            'Regression descriptives (31_SCENARIO_DIAGNOSTICS), winsor impact (04), and correlations (20–25, 90–91) use exact period complete cases under the diagnostics configuration. Missingness (03 and missingness columns in 31) uses pre-complete-case rows. Trajectory summaries, growth/path summaries, sector/ownership profiles, median covariate profiles, performance-band profiles (02, 10–16), and firm trajectories (92) use the broader complete-trajectory population. A variable-specific profile ignores missing values without requiring all other controls. 30_SCENARIO_SUMMARY describes this pre-model scope. These are intentionally different estimands; align only reports meant to describe regressions.', '',
            'Saved diagnostics Ns and saved primary regression Ns match independently reconstructed current company sets. The saved diagnostic workbook does not store every estimation ID; membership is reconstructed from the source and exact current helper, with SHA256 set fingerprints in 02_SAMPLE_OVERLAP. Saved 92_FIRM_TRAJECTORIES supplies the broader firm list.', '',
            'All four scenarios currently use different firms across periods. Baseline/winsorised and raw/standardised variants have identical company sets within a scenario-period. Active interaction specifications introduce **zero additional missing-case exclusions**; diagnostics complete-case firm IDs equal primary OLS IDs for all 16 scenario-period combinations. Saved quantile Ns (48 rows) and compact interaction Ns (48 rows) also agree with reconstruction.', '',
            '## Export interpretation', '',
            'Exports/sales is calculated without clipping as exports divided by nonzero sales. A proposed [0,1] eligibility rule therefore changes current practice; it is conditional on identical reporting scope and definitions. Exports above sales do not alone prove a mathematical error in the stored ratio. Negative exports may represent source adjustments; researcher review remains necessary. The exclusion register displays original numerator/denominator, value, source year and exact model/diagnostic membership. No source correction or cap is applied.', '',
            'Specific cases: Xiaomi Technology (Polska), NIP 5213838221, export ratio **5.018632** in 2019 (exports 41,491; sales 8,267.39314): included in trajectory/covariate profiles; excluded from P1 regression/correlation because the 2018–2019 lag is missing; **included in FULL regression/correlation**, which has no lag. E&Y, NIP 5260207930, **2.043182 in 2019** and **2.017796 in 2020**, enters P1/FULL and P2 regressions/correlations respectively. Vicis, NIP 5242617178, **6.565700 in 2019**, has an incomplete trajectory and enters none of these analyses. Iglotex Dystrybucja Polska, NIP 6790175807, has negative exports in 2020: the P2 export ratio is **−0.003552**, currently included in P2 regression/correlation. Some excess ratios are very close to 1 and may reflect rounding; strict proposed counts deliberately use the requested inclusive [0,1] bound without adding a grace threshold.', '',
            '## Recommendation', '',
            'A common sample is feasible with modest numerical loss: **RANK2019 retains 1,748 firms**, losing a further **27–39 (1.52–2.18%)** per period after the proposed validity screen. **RANK2019_MANUFACTURING retains 754**, losing **17–22 (2.20–2.84%)**. ALL retains 2,332 (further 1.60–3.99% loss), and ALL manufacturing retains 949 (2.16–4.33%). These are small enough to justify a common-sample sensitivity analysis. They do not establish that selection is ignorable: compare retained/excluded firms and coefficients before making the common sample primary. No arbitrary acceptance cutoff is introduced.', '',
            'Recommend introducing a common sample as a labelled sensitivity analysis after researcher approval of the conditional export rule, and using its exact company set for matching regression descriptives/correlations. Preserve broader trajectory summaries with explicit sample labels. Common growth trajectories already hold; incremental losses arise from control/lag missingness and proposed exclusions across periods. Implementation is deliberately deferred.', '',
            '## Reproduction and validation', '',
            'Run `python code_audit_sample_eligibility.py`, then the companion `code_write_sample_eligibility_audit.mjs` with `/tmp/grip_sample_eligibility_audit.json`, the absolute audit workbook destination and a temporary preview directory (bundled artifact-tool Node dependencies). Run `python code_check_sample_eligibility_audit.py` after export. The project Python environment is used because the bundled Python lacks a Parquet engine. Existing files are hashed before/after; unique keys, independent complete-case ID reconstruction, saved 64 OLS Ns, 48 quantile Ns, 48 interaction Ns, saved diagnostic/correlation Ns, severe primary Ns and actual set intersections are checked. No model is fitted. Workbook counts are static audit results regenerated by the audit, not editable cleaning decisions.', '',
            'Protected input SHA256: `' + before['data_period_2018-2024.parquet'] + '` (period) and `' + before['data_core_2018-2024.parquet'] + '` (core).', '']
    (ROOT / 'results_sample_eligibility_audit.md').write_text('\n'.join(text))
    print(primary.to_string(index=False))
    print('Unique newly excluded primary firms:', len(new_firms))
    print('Rows by sheet:', {k: len(v) for k, v in tables.items()})
    print('Protected files unchanged:', len(before))


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1:
        from code_run_quantitative_pipeline import main as unified_main
        unified_main(['--quality-only'])
    else:
        raise SystemExit('Superseded routine output. Use code_run_quantitative_pipeline.py --audit-only or --quality-only. Independent audit_frames/construct functions remain available.')
