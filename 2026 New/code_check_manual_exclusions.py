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
        from code_common_samples import current
        assert set(frame.nip.astype(str)) == current().memberships[scenario]
    for sample in ['Rank2019', 'Rank2019_Manufacturing']:
        mask = shared.build_sample_mask(source, sample, ols.trajectory_col(config))
        assert not mask[excluded].any(), sample
    print('PASS: exact-name/NIP matching, enabled switch, other companies retained, all OLS/quantile/diagnostic/severe masks and complete-case exclusions aligned.')
    print(f'Input firms={len(source)}; manually removed={int(excluded.sum())}; eligible before scenario filtering={len(expected)}')


def check_saved_outputs():
    from code_common_samples import current, ROOT
    from code_check_common_samples import validate_context,validate_workbooks
    context=current();validate_context(context);validate_workbooks(ROOT,context)
    companies=pd.read_excel(ROOT/'results_data_quality_and_samples.xlsx',sheet_name='03_EXCLUDED_COMPANIES',header=3,dtype={'nip':str})
    for entry in shared.MANUAL_EXCLUSIONS['companies']:
        rows=companies.loc[companies.nip.eq(entry['nip'])]
        present=context.source.nip.eq(entry['nip']).any()
        assert len(rows)==int(present)
        if present: assert entry['reason_code'] in rows.iloc[0].primary_exclusion_reason
    print('PASS: manual firm reasons occur once in the central register; absent upstream firms remain absent; exact model alignment.')


if __name__ == '__main__':
    check_manual_exclusions()
    check_saved_outputs()
