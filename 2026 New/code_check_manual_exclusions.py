"""Verify exact manual exclusions across every analytical sample builder."""
from copy import deepcopy

import pandas as pd
import openpyxl

import code_config as shared
import code_ols_scenarios as ols
import code_quantile as quantile
import code_diagnostics_trajectories as diagnostics


def check_manual_exclusions():
    # Exact-name matching and NIP matching each work independently; retain
    # other Orlen companies and similarly named but different entities.
    records = []
    for entry in shared.MANUAL_EXCLUSIONS['companies']:
        records.extend([
            {'nip': entry['nip'], 'company': 'Changed company name'},
            {'nip': 'different-' + entry['nip'], 'company': entry['company']},
        ])
    records.extend([
        {'nip': '7742745992', 'company': 'Basell Orlen Polyolefins sp. z o.o., Płock'},
        {'nip': 'other', 'company': 'Another Kania company'},
    ])
    synthetic = pd.DataFrame(records)
    assert shared.manual_exclusion_mask(synthetic).tolist() == [True] * (2 * len(shared.MANUAL_EXCLUSIONS['companies'])) + [False] * 2
    saved = deepcopy(shared.MANUAL_EXCLUSIONS)
    try:
        shared.MANUAL_EXCLUSIONS['enabled'] = False
        assert not shared.manual_exclusion_mask(synthetic).any()
    finally:
        shared.MANUAL_EXCLUSIONS.update(deepcopy(saved))
    try:
        shared.MANUAL_EXCLUSIONS['companies'][0]['reason_code'] = 'UNKNOWN'
        try:
            shared.manual_exclusion_mask(synthetic)
        except ValueError:
            pass
        else:
            raise AssertionError('Unknown exclusion reasons must fail validation.')
    finally:
        shared.MANUAL_EXCLUSIONS.update(deepcopy(saved))

    config = ols.normalise_config(ols.CONFIG)
    source = pd.read_parquet(config['input_file'])
    excluded = shared.manual_exclusion_mask(source)
    expected = source.loc[~excluded]
    models = ols.build_models(config)
    prepared = ols.add_interaction_columns(source, config, models)
    for scenario, metadata in shared.get_scenario_definitions().items():
        expected_mask = shared.build_scenario_mask(prepared, scenario, ols.trajectory_col(config))
        assert not expected_mask[excluded].any(), scenario
        scenario_ols = ols.apply_scenario_filter(prepared, scenario, metadata['filter'])
        scenario_quantile = quantile.apply_scenario_filter(prepared, scenario, metadata['filter'])
        pd.testing.assert_index_equal(scenario_ols.index, scenario_quantile.index)
        assert not shared.manual_exclusion_mask(scenario_ols).any()
        for period, model in models.items():
            estimation = ols.get_estimation_sample(scenario_ols, config, model)
            assert not estimation.index.isin(source.index[excluded]).any(), (scenario, period)
    family = shared.get_trajectory_family_config(config['growth_mode'])
    diagnostic_samples = diagnostics.build_samples(prepared, family)
    for scenario, frame in diagnostic_samples.items():
        assert not shared.manual_exclusion_mask(frame).any(), scenario
        pd.testing.assert_index_equal(frame.index, prepared.loc[shared.build_scenario_mask(prepared, scenario, family['complete_flag'])].index)
    for sample in ['Rank2019', 'Rank2019_Manufacturing']:
        mask = shared.build_sample_mask(source, sample, ols.trajectory_col(config))
        assert not mask[excluded].any(), sample
    print('PASS: exact-name/NIP matching, enabled switch, other companies retained, all OLS/quantile/diagnostic/severe masks and complete-case exclusions aligned.')
    print(f'Input firms={len(source)}; manually removed={int(excluded.sum())}; eligible before scenario filtering={len(expected)}')


def check_saved_outputs():
    """Confirm current saved results, README audits and model sample counts."""
    config = ols.normalise_config(ols.CONFIG)
    models = ols.build_models(config)
    source = pd.read_parquet(config['input_file'])
    source = ols.add_interaction_columns(source, config, models)
    ols_summary = pd.read_excel('results_ols_scenarios.xlsx', sheet_name='Model_Summary_Long')
    quantile_summary = pd.read_excel('results_quantile.xlsx', sheet_name='Model_Summary_Long')
    diagnostic_summary = pd.read_excel('results_diagnostics_trajectories.xlsx', sheet_name='30_SCENARIO_SUMMARY').set_index('scenario')
    assert len(ols_summary) == 64 and len(quantile_summary) == 48
    assert quantile_summary.converged.eq('OK').all()
    for scenario in shared.get_scenario_definitions():
        frame = source.loc[shared.build_scenario_mask(source, scenario, ols.trajectory_col(config))]
        assert diagnostic_summary.loc[scenario, 'observations'] == len(frame)
        for period, model in models.items():
            n = len(ols.get_estimation_sample(frame, config, model))
            for summary in [ols_summary, quantile_summary]:
                rows = summary.loc[summary.scenario.eq(scenario) & summary.period.eq(period)]
                assert not rows.empty and rows.observations.eq(n).all(), (scenario, period)
    firm_output = pd.read_excel('results_diagnostics_trajectories.xlsx', sheet_name='92_FIRM_TRAJECTORIES', dtype={'nip': str})
    assert not shared.manual_exclusion_mask(firm_output).any()
    files = [
        ('results_ols_scenarios.xlsx', 'README', ols.OUTPUT_SHEETS),
        ('results_quantile.xlsx', 'README', quantile.OUTPUT_SHEETS),
        ('results_diagnostics_trajectories.xlsx', '00_README_STRUCTURE', diagnostics.OUTPUT_SHEETS),
        ('Results_severe_P1_decline_analysis.xlsx', '00_README', None),
    ]
    for filename, readme, expected_sheets in files:
        workbook = openpyxl.load_workbook(filename, read_only=True)
        if expected_sheets:
            assert workbook.sheetnames == expected_sheets
        descriptions = [' | '.join(str(v) for v in row if v is not None) for row in workbook[readme].values]
        for entry in shared.MANUAL_EXCLUSIONS['companies']:
            lines = [text for text in descriptions if entry['company'] in text and entry['nip'] in text]
            assert len(lines) == 1, (filename, entry)
            assert any(status in lines[0] for status in ['Removed from analysis', 'Already absent from input', 'Disabled'])
            assert entry['reason_code'] in lines[0], (filename, entry)
            assert shared.MANUAL_EXCLUSION_REASONS[entry['reason_code']] in lines[0], (filename, entry)
        for sheet in workbook:
            assert sheet.max_row > 1, (filename, sheet.title)
            assert not any(cell.data_type == 'e' for row in sheet for cell in row), (filename, sheet.title)
        workbook.close()
    print('PASS: 64 OLS and 48 converged quantile results; exact cross-workbook sample counts; no excluded firm in trajectory export; four complete README audits; expected tabs and no Excel error cells.')


if __name__ == '__main__':
    check_manual_exclusions()
    check_saved_outputs()
