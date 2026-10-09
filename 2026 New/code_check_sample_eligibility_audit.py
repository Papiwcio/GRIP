"""Independent audit reconciliation; read-only, never estimates regressions."""
import json
from pathlib import Path
from itertools import combinations
import numpy as np
import pandas as pd
from openpyxl import load_workbook
import code_ols_scenarios as ols
from code_config import manual_exclusion_mask
from code_audit_sample_eligibility import protected_hashes, sample_hash


def main():
    payload = json.loads(Path('/tmp/grip_sample_eligibility_audit.json').read_text())
    assert payload['protected_sha256'] == protected_hashes()
    counts = pd.DataFrame(payload['tables']['01_SAMPLE_COUNTS'])
    overlaps = pd.DataFrame(payload['tables']['02_SAMPLE_OVERLAP'])
    register = pd.DataFrame(payload['tables']['03_EXCLUSION_REGISTER'])
    source = pd.read_parquet('data_period_2018-2024.parquet')
    source['nip'] = source.nip.astype(str)
    source = source.loc[~manual_exclusion_mask(source)]
    core = pd.read_parquet('data_core_2018-2024.parquet')
    core['nip'] = core.nip.astype(str)
    core = core.set_index(['nip','year'])
    config = ols.normalise_config({**ols.CONFIG, 'include_interactions':False})
    specs = ols.build_models(config)
    reconstructed = {}
    for scenario, query in ols.SCENARIOS.items():
        scope = ols.apply_scenario_filter(source, scenario, query)
        frame = scope.loc[scope.has_complete_ntrajectory.eq(1)]
        current, valid = {}, {}
        for period, spec in specs.items():
            cols = [spec['dependent'], *spec['regressors'], 'sector_en']
            working = frame[cols].copy()
            nums = [x for x in cols if x != 'sector_en']
            working[nums] = working[nums].apply(pd.to_numeric, errors='coerce')
            complete = frame.loc[working.dropna().index]
            current[period] = set(complete.nip)
            start = {'P1':2019,'P2':2020,'P3':2022,'FULL':2019}[period]
            suffix = 'P1' if period == 'FULL' else period
            export = complete[f'export_ratio_start_{suffix}']
            keep = export.between(0,1) & np.isfinite(working.loc[complete.index, nums]).all(axis=1)
            # Check source denominators independently. No constraint on equity/sign of profits.
            for index, row in complete.iterrows():
                original = core.loc[(row.nip, start)]
                keep.at[index] &= all(pd.notna(original[x]) and np.isfinite(original[x]) and original[x] > 0
                                     for x in ['sales','employment','total_assets'])
            valid[period] = set(complete.loc[keep,'nip'])
            record = counts.loc[counts.specification.eq('OLS_PRIMARY') & counts.scenario.eq(scenario) & counts.period.eq(period)].iloc[0]
            assert record.current_N == len(current[period]) and record.valid_N == len(valid[period])
            assert record.current_ID_SHA256 == sample_hash(current[period])
            assert record.valid_ID_SHA256 == sample_hash(valid[period])
        common = set.intersection(*valid.values())
        assert set(counts.loc[counts.specification.eq('OLS_PRIMARY') & counts.scenario.eq(scenario), 'common_sample_N']) == {len(common)}
        for state, sets in [('CURRENT',current),('PROPOSED_VALID',valid)]:
            for size in range(1,5):
                for combo in combinations(specs,size):
                    members = set.intersection(*(sets[p] for p in combo))
                    record = overlaps.loc[overlaps.specification.eq('OLS_PRIMARY') & overlaps.scenario.eq(scenario) & overlaps.state.eq(state) & overlaps.periods.eq(' & '.join(combo))].iloc[0]
                    assert record.unique_firms == len(members) and record.intersection_ID_SHA256 == sample_hash(members)
        reconstructed[scenario] = current
    # Source-derived case checks prevent the P1/FULL lag distinction being hidden.
    assert '5213838221' not in reconstructed['ALL']['P1']
    assert '5213838221' in reconstructed['ALL']['FULL']
    assert '5260207930' in reconstructed['RANK2019']['P1'] & reconstructed['RANK2019']['P2']
    assert all('5242617178' not in s for sets in reconstructed.values() for s in sets.values())
    assert '6790175807' in reconstructed['ALL']['P2']
    new = register.loc[register.specification.eq('OLS_PRIMARY') & register.exclusion_status.eq('NEWLY_PROPOSED')]
    assert set(new.rule) == {'EXPORT_0_1_PROPOSED'} and new.nip.nunique() == 38
    # Native severe summary tables share the same period eligibility (read only).
    severe = load_workbook('results_severe_p1_decline_analysis.xlsx', data_only=True)
    for sheet, period in [('02_LOGIT_BOTTOMP1','P1'),('04_P2_GROUP_MODEL','P2'),('05_P3_GROUP_MODEL','P3')]:
        s = severe[sheet]
        values = list(s['A6:Q8'])
        headers = [c.value for c in values[0]]
        for row in values[1:]:
            record = dict(zip(headers,[c.value for c in row]))
            assert record['N'] == len(reconstructed[record['sample']][period])
    severe.close()
    # Read exported tables and check all record counts, typed numbers, ID text and panes.
    wb = load_workbook('results_sample_eligibility_audit.xlsx', data_only=True)
    assert wb.sheetnames == list(payload['tables'])
    for name, records in payload['tables'].items():
        sheet = wb[name]
        assert sheet.max_row == len(records) + 5
        assert len(sheet.tables) == 1 and sheet.freeze_panes
        assert sheet.max_column == len(records[0])
        assert sheet.sheet_view.showGridLines is False
    assert wb['01_SAMPLE_COUNTS']['C6'].value == 2385
    assert isinstance(wb['03_EXCLUSION_REGISTER']['C6'].value,str)
    wb.close()
    assert payload['protected_sha256'] == protected_hashes()
    print('PASS: independent primary company IDs, 120 intersections, proposed validity, P1/FULL cases, severe Ns, four exported tables and protected-file hashes.')


if __name__ == '__main__':
    from code_common_samples import current, ROOT
    from code_check_common_samples import validate_context, validate_workbooks
    context=current();validate_context(context);validate_workbooks(ROOT,context)
