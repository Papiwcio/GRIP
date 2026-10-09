"""Independent policy tests; fixtures and proposed specifications stay in memory."""
from copy import deepcopy
from contextlib import redirect_stdout
import hashlib
import io
import unittest
import numpy as np
import pandas as pd
import code_common_samples as common
import code_ols_scenarios as ols


class PolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.context=common.build_context()
        cls.specs=deepcopy(cls.context.specifications)
        cls.ids=sorted(cls.context.memberships['ALL'])[:30]
        cls.source=cls.context.source.loc[cls.context.source.nip.isin(cls.ids)].copy()
        cls.core=cls.context.core.loc[cls.context.core.nip.isin(cls.ids)].copy()
        cls.nip=cls.ids[0]

    def setUp(self):
        self.old_active=common._ACTIVE
        common.activate(self.context)

    def tearDown(self): common.activate(self.old_active)

    def test_inputs_are_unchanged(self):
        p,c=self.source.copy(deep=True),self.core.copy(deep=True)
        common.construct(p,c,self.specs)
        pd.testing.assert_frame_equal(p,self.source);pd.testing.assert_frame_equal(c,self.core)

    def test_missing_or_duplicate_identifiers_stop_estimation(self):
        p=self.source.copy();p.loc[p.index[0],'nip']=None
        with self.assertRaisesRegex(ValueError,'Missing source'): common.construct(p,self.core,self.specs)
        p=pd.concat([self.source,self.source.iloc[:1]],ignore_index=True)
        with self.assertRaisesRegex(ValueError,'duplicate source'): common.construct(p,self.core,self.specs)

    def test_export_bounds_and_zero_denominator(self):
        for variable,value in [('export_ratio_start_P1',1.001),('export_ratio_start_P1',-.01)]:
            p=self.source.copy();c=self.core.copy()
            p.loc[p.nip.eq(self.nip),variable]=value
            x=common.construct(p,c,self.specs)
            self.assertNotIn(self.nip,x.memberships['ALL'])
            self.assertTrue(x.details.loc[x.details.nip.eq(self.nip)].rule.eq('EXPORT_0_1').any())
        p=self.source.copy();c=self.core.copy();c.loc[c.nip.eq(self.nip)&c.year.eq(2020),'total_assets']=0
        x=common.construct(p,c,self.specs)
        self.assertNotIn(self.nip,x.memberships['ALL'])

    def test_nonpositive_employment_is_local_year_evidence(self):
        c=self.core.copy();c.loc[c.nip.eq(self.nip)&c.year.eq(2022),'employment']=-1
        x=common.construct(self.source,c,self.specs)
        reasons=x.details.loc[x.details.nip.eq(self.nip)&x.details.variable.eq('employment')]
        self.assertTrue(reasons.year.eq(2022).all())
        self.assertNotIn(self.nip,x.memberships['ALL'])

    def test_2018_requires_only_sales(self):
        c=self.core.copy();non_sales=[v for v in c.columns if v not in ['nip','year','company','sales']]
        # Original fixtures already contain expected unavailable financial blocks in 2018.
        for v in ['employment','exports','total_assets','net_profit']: c.loc[c.year.eq(2018),v]=np.nan
        x=common.construct(self.source,c,self.specs)
        self.assertEqual(x.memberships['ALL'],set(self.ids))
        self.assertFalse(((x.details.year==2018)&x.details.variable.ne('sales')&x.details.eligibility_impact).any())

    def test_unbounded_extreme_ratios_are_retained(self):
        p=self.source.copy();c=self.core.copy()
        p.loc[p.nip.eq(self.nip),'capital_ratio_start_P1']=2
        p.loc[p.nip.eq(self.nip),'profit_margin_start_P1']=-100
        x=common.construct(p,c,self.specs)
        self.assertIn(self.nip,x.memberships['ALL'])

    def test_dynamic_new_variable_reports_sources_years_and_firm(self):
        p=self.source.copy();c=self.core.copy()
        p.loc[p.nip.eq(self.nip),'operating_margin_start_P3']=np.nan
        c.loc[c.nip.eq(self.nip)&c.year.eq(2022),'operating_result']=np.nan
        before=common.construct(p,c,self.specs)
        self.assertIn(self.nip,before.memberships['ALL'])
        specs=deepcopy(self.specs)
        target=next(s for s in specs if s['family']=='OLS_EXPORT_SIZE' and s['period']=='P3')
        target['required'].append('operating_margin_start_P3');target['regressors'].append('operating_margin_start_P3')
        after=common.construct(p,c,specs)
        self.assertNotIn(self.nip,after.memberships['ALL'])
        r=after.details.loc[after.details.nip.eq(self.nip)&after.details.variable.eq('operating_result')]
        self.assertTrue(r.year.eq(2022).all());self.assertTrue(r.specifications.str.contains('OLS_EXPORT_SIZE:P3').all())
        self.assertNotEqual(before.signature,after.signature)
        common.activate(after)
        with self.assertRaisesRegex(RuntimeError,'configuration changed'): common.current()

    def test_unapproved_runner_override_is_blocked(self):
        with self.assertRaisesRegex(RuntimeError,'Unapproved runner'):
            common.validate_request({**ols.CONFIG,'base_regressors':[*ols.CONFIG['base_regressors'],'operating_margin']})

    def test_unknown_lineage_is_not_guessed(self):
        with self.assertRaisesRegex(ValueError,'lineage'): common.lineage('invented_similar_ratio',set(self.core.columns))

    def test_common_ids_and_variants_not_just_counts(self):
        config=ols.normalise_config({**ols.CONFIG,'include_interactions':False})
        models=ols.build_models(config)
        frame=common.select(self.context.source,'RANK2019_MANUFACTURING')
        levels=ols.get_categorical_levels(frame,config);registry=ols.build_variable_registry(config,models,levels)
        selected=[]
        for period,model in models.items():
            for variant in ols.get_model_variants(config):
                result=ols.run_model_variant(period,model,variant,frame,levels,registry,config)
                selected.append(set(result['estimation_sample_ids']))
                self.assertEqual(result['sample_sha256'],common.fingerprint(selected[-1]))
        self.assertTrue(all(ids==selected[0] for ids in selected))
        altered=frame.iloc[:-1].copy()
        with self.assertRaisesRegex(RuntimeError,'Critical sample mismatch'):
            common.verify(altered,altered.nip,'bad','test','P1')

    def test_manufacturing_is_nested_and_scopes_are_not_invalid(self):
        x=self.context
        self.assertTrue(x.memberships['MANUFACTURING']<=x.memberships['ALL'])
        self.assertTrue(x.memberships['RANK2019_MANUFACTURING']<=x.memberships['RANK2019'])
        outside=set(x.source.nip)-x.scopes['RANK2019']
        self.assertTrue(outside)
        self.assertFalse(x.details.rule.str.contains('POPULATION').any())


if __name__=='__main__': unittest.main(verbosity=2)
