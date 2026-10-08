"""Validate source-to-estimation exclusions without changing live outputs."""
from contextlib import redirect_stdout
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from zipfile import ZipFile

import numpy as np
import pandas as pd
import code_ols_scenarios as engine
import code_refresh_ols_exclusion_audit as audit


class ExclusionAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.protected = {p: hashlib.sha256(p.read_bytes()).digest() for p in Path('.').rglob('*')
                         if p.is_file() and p.suffix in {'.xlsx', '.parquet'}}
        cls.table, cls.config, cls.source_n = audit.build_audit()
        with redirect_stdout(io.StringIO()):
            cls.report = engine.run_period_ols_scenarios(write_output=False)

    def test_vicis_is_recorded_at_correct_first_stage(self):
        rows = self.table.loc[self.table.nip.astype(str).eq('5242617178')]
        self.assertEqual(len(rows), 4)
        for scenario in ['ALL', 'RANK2019']:
            row = rows.loc[rows.scenario.eq(scenario)].iloc[0]
            self.assertEqual(row.exclusion_stage, 'incomplete_trajectory')
            self.assertEqual(row.trajectory_flag_value, 0)
            self.assertEqual(row.model, '')
            self.assertIn('ngrowth_log_ann_P2', row.missing_columns)
            self.assertIn('ngrowth_log_ann_P3', row.missing_columns)
        self.assertTrue(rows.loc[rows.scenario.str.contains('MANUFACTURING'), 'exclusion_stage'].eq('scenario_not_eligible').all())

    def test_manual_rules_and_source_scope(self):
        manual = self.table.loc[self.table.exclusion_stage.eq('manual_exclusion')]
        self.assertEqual(manual.groupby('scenario').size().tolist(), [5]*4)
        self.assertTrue(manual.reason_code.ne('').all())
        self.assertFalse(manual.nip.astype(str).eq('7740001454').any())  # Already absent upstream.
        self.assertFalse(self.table.loc[self.table.exclusion_stage.ne('model_complete_case')].duplicated(['scenario', 'nip']).any())

    def test_every_model_partitions_original_firms_exactly(self):
        source = set(pd.read_parquet(engine.CONFIG['input_file']).nip.astype(str))
        audit.validate_reconciliation(self.table, self.report['summary_long_df'], self.source_n)
        for scenario, results in self.report['scenario_results'].items():
            early = self.table.loc[self.table.scenario.eq(scenario) & self.table.exclusion_stage.ne('model_complete_case')]
            before = set(early.nip.astype(str))
            for model in results['model_results']:
                missing = set(self.table.loc[self.table.scenario.eq(scenario) & self.table.model.eq(model['model']), 'nip'].astype(str))
                estimated = set(map(str, model['estimation_sample_ids']))
                self.assertFalse(before & missing or before & estimated or missing & estimated)
                self.assertEqual(source, before | missing | estimated)
        pd.testing.assert_frame_equal(self.table.reset_index(drop=True), self.report['dropped_rows_long_df'].reset_index(drop=True))

    def test_saved_coefficients_and_sample_sizes_unchanged(self):
        saved = pd.read_excel('results_ols_scenarios.xlsx', sheet_name='Coefficients_Long')
        expected = self.report['coefficients_long_df']
        paired = saved.merge(expected, on=['scenario', 'model', 'raw_variable'], suffixes=('_old','_new'), validate='one_to_one')
        self.assertEqual(len(paired), len(expected))
        for column in ['coefficient', 'std error', 'p-value', 'CI lower', 'CI upper']:
            np.testing.assert_allclose(paired[column+'_old'], paired[column+'_new'], rtol=1e-10, atol=1e-10)
        actual = pd.read_excel('results_ols_scenarios.xlsx', sheet_name='Model_Summary_Long')
        paired = actual.merge(self.report['summary_long_df'], on=['scenario','model'], suffixes=('_old','_new'), validate='one_to_one')
        for column in ['observations','R_squared','adjusted_R_squared']:
            np.testing.assert_allclose(paired[column+'_old'], paired[column+'_new'], rtol=1e-10, atol=1e-10)
        diagnostics = pd.read_excel('results_ols_scenarios.xlsx', sheet_name='Diagnostics_Long')
        paired = diagnostics.merge(self.report['diagnostics_long_df'], on=['scenario','model'], suffixes=('_old','_new'), validate='one_to_one')
        self.assertEqual(paired.sample_sha256_old.tolist(), paired.sample_sha256_new.tolist())

    def test_audit_refresh_preserves_unrelated_workbook_parts(self):
        source = Path('results_ols_scenarios.xlsx')
        with tempfile.TemporaryDirectory(prefix='grip_exclusion_check_') as folder:
            destination = Path(folder)/source.name
            destination.write_bytes(source.read_bytes())
            with redirect_stdout(io.StringIO()):
                audit.refresh(destination)
            with ZipFile(source) as old, ZipFile(destination) as new:
                self.assertEqual(old.namelist(), new.namelist())
                # The second refresh should preserve every unrelated worksheet,
                # styles, notes, filters, panes, shared strings and workbook metadata.
                changed = [name for name in old.namelist() if old.read(name)!=new.read(name)]
                self.assertTrue(set(changed).issubset({'xl/worksheets/sheet1.xml','xl/worksheets/sheet7.xml'}))
            saved = pd.read_excel(destination, sheet_name='Dropped_Rows_Long')
            self.assertEqual(len(saved), len(self.table))
            self.assertEqual(saved.exclusion_stage.value_counts().to_dict(), self.table.exclusion_stage.value_counts().to_dict())

    def test_live_workbooks_and_data_untouched_by_validation(self):
        for path, value in self.protected.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).digest(), value, str(path))


if __name__ == '__main__':
    unittest.main()
