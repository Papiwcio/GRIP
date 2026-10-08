"""Test generic centring and export reproducible before/after OLS diagnostics."""
from copy import deepcopy
from pathlib import Path
from datetime import date
import tempfile
import unittest
import numpy as np
import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
import code_ols_scenarios as o
from code_config import build_scenario_mask, get_scenario_definitions, apply_manual_exclusions, manual_exclusion_readme_rows


class CentringTests(unittest.TestCase):
    def setUp(self):
        self.metadata = deepcopy(o.INTERACTION_METADATA)
        self.c = o.normalise_config(o.CONFIG)
        self.models = o.build_models(self.c)
        rng = np.random.default_rng(1742)
        self.data = pd.DataFrame(index=range(160))
        for spec in self.models.values():
            self.data[spec['dependent']] = rng.normal(size=160)
            for name in spec['regressors']:
                if name not in self.data:
                    self.data[name] = rng.normal(10, 2, 160)
        self.data[self.c['owner_column']] = rng.integers(0, 2, 160)
        self.data['sector_en'] = np.where(rng.random(160)>.5,'food','production')

    def tearDown(self):
        o.INTERACTION_METADATA.clear(); o.INTERACTION_METADATA.update(self.metadata)

    def prepared(self, data=None, period='P1'):
        models=o.build_models(self.c)
        data=o.add_interaction_columns(self.data if data is None else data,self.c,models)
        return o.get_estimation_sample(data,self.c,models[period]), models[period]

    def test_continuous_and_observed_binary_numeric(self):
        self.data['export_ratio_start_P1']=np.arange(160)%2
        e,s=self.prepared(); p='export_ratio_x_ln_sales_start_P1'
        np.testing.assert_allclose(e[p],(e.export_ratio_start_P1-e.export_ratio_start_P1.mean())*(e.ln_sales_start_P1-e.ln_sales_start_P1.mean()))
        self.assertNotEqual(e.attrs['interaction_centring'][p]['inputs'][0]['offset'],0)

    def test_continuous_binary_and_multiple_products(self):
        o.INTERACTION_METADATA['size_owner']={'variables':['ln_sales',self.c['owner_column']],'display_name':'size × owner','interpretation':'test','standardise':True,'include':True}
        e,s=self.prepared(); p='size_owner_start_P1'
        np.testing.assert_allclose(e[p],(e.ln_sales_start_P1-e.ln_sales_start_P1.mean())*e[self.c['owner_column']])
        self.assertEqual(len(e.attrs['interaction_centring']),2)
        self.assertEqual(e.attrs['interaction_centring'][p]['inputs'][1]['offset'],0)
        models=o.build_models(self.c); data=o.add_interaction_columns(self.data,self.c,models)
        levels=o.get_categorical_levels(data,self.c); registry=o.build_variable_registry(self.c,models,levels)
        for variant in self.c['model_variants']:
            assert_equivalent(o.run_model_variant('P1',models['P1'],variant,data,levels,registry,{**self.c,'centre_interaction_inputs':False}),
                              o.run_model_variant('P1',models['P1'],variant,data,levels,registry,self.c))

    def test_binary_binary_metadata(self):
        e,s=self.prepared(); registry=o.build_variable_registry(self.c,o.build_models(self.c))
        for name in ['export_ratio_start_P1','ln_sales_start_P1']:
            e[name]=np.arange(len(e))%2; registry[name]['variable_type']='dummy'
        centred=o.centre_interaction_columns(e,s['regressors'],registry,self.c)
        np.testing.assert_array_equal(centred['export_ratio_x_ln_sales_start_P1'],e.export_ratio_start_P1*e.ln_sales_start_P1)

    def test_missing_cases_and_reference_samples(self):
        self.data.loc[0,'profit_margin_start_P1']=np.nan
        self.data.loc[1:30,self.models['FULL']['dependent']]=np.nan
        p1,_=self.prepared(); full,_=self.prepared(period='FULL')
        self.assertNotIn(0,p1.index)
        self.assertEqual(p1.attrs['interaction_centring']['export_ratio_x_ln_sales_start_P1']['N'],159)
        self.assertNotEqual(p1.ln_sales_start_P1.mean(),full.ln_sales_start_P1.mean())
        for sample in [p1,full]:
            self.assertEqual(sample.attrs['interaction_centring']['export_ratio_x_ln_sales_start_P1']['inputs'][1]['offset'],sample.ln_sales_start_P1.mean())
        smaller=p1.iloc[:50]; registry=o.build_variable_registry(self.c,o.build_models(self.c))
        re=o.centre_interaction_columns(smaller,self.models['P1']['regressors'],registry,self.c)
        self.assertEqual(re.attrs['interaction_centring']['export_ratio_x_ln_sales_start_P1']['N'],50)

    def test_missing_main_effect_fails(self):
        e,s=self.prepared(); registry=o.build_variable_registry(self.c,o.build_models(self.c))
        with self.assertRaisesRegex(ValueError,'lacks constituent main effects'):
            o.centre_interaction_columns(e,[n for n in s['regressors'] if n!='ln_sales_start_P1'],registry,self.c)

    def test_no_active_interactions(self):
        o.INTERACTION_METADATA.clear()
        e,s=self.prepared()
        self.assertEqual(e.attrs['interaction_centring'],{})
        models=o.build_models(self.c); levels=o.get_categorical_levels(self.data,self.c)
        registry=o.build_variable_registry(self.c,models,levels)
        for variant in self.c['model_variants']:
            assert_equivalent(o.run_model_variant('P1',models['P1'],variant,self.data,levels,registry,{**self.c,'centre_interaction_inputs':False}),
                              o.run_model_variant('P1',models['P1'],variant,self.data,levels,registry,self.c))

    def test_raw_and_standardised_equivalence(self):
        models=o.build_models(self.c); data=o.add_interaction_columns(self.data,self.c,models)
        levels=o.get_categorical_levels(data,self.c); registry=o.build_variable_registry(self.c,models,levels)
        for period in self.c['periods']:
            for variant in self.c['model_variants']:
                old=o.run_model_variant(period,models[period],variant,data,levels,registry,{**self.c,'centre_interaction_inputs':False})
                new=o.run_model_variant(period,models[period],variant,data,levels,registry,self.c)
                assert_equivalent(old,new)


def assert_equivalent(old,new):
    a,b=old['result'],new['result']
    assert a.nobs==b.nobs
    pd.testing.assert_index_equal(a.fittedvalues.index,b.fittedvalues.index)
    np.testing.assert_array_equal(a.model.endog,b.model.endog)
    for i,term in enumerate(a.model.exog_names):
        if term not in new['interaction_centring']:
            np.testing.assert_array_equal(a.model.exog[:,i],b.model.exog[:,i])
    np.testing.assert_allclose(a.fittedvalues,b.fittedvalues,atol=1e-8,rtol=1e-7)
    np.testing.assert_allclose(a.resid,b.resid,atol=1e-8,rtol=1e-7)
    np.testing.assert_allclose([a.rsquared,a.rsquared_adj],[b.rsquared,b.rsquared_adj],atol=1e-10)
    for term in new['interaction_centring']:
        np.testing.assert_allclose([a.tvalues[term],a.pvalues[term]],[b.tvalues[term],b.pvalues[term]],atol=1e-7,rtol=1e-7)
        if not new['standardised_model']=='Yes':
            np.testing.assert_allclose(a.params[term],b.params[term],atol=1e-8,rtol=1e-7)


def actual_model_audit(output_path=None):
    c=o.normalise_config(o.CONFIG); data,audit=apply_manual_exclusions(pd.read_parquet(c['input_file']))
    models=o.build_models(c); data=o.add_interaction_columns(data,c,models)
    comparisons=[]; inputs=[]; products=[]; vifs=[]
    for scenario in get_scenario_definitions():
        frame=data.loc[build_scenario_mask(data,scenario,o.trajectory_col(c))]
        levels=o.get_categorical_levels(frame,c); registry=o.build_variable_registry(c,models,levels)
        for period,spec in models.items():
            for variant in c['model_variants']:
                old=o.run_model_variant(period,spec,variant,frame,levels,registry,{**c,'centre_interaction_inputs':False})
                new=o.run_model_variant(period,spec,variant,frame,levels,registry,c)
                assert_equivalent(old,new)
                a,b=old['result'],new['result']; tags={'scenario':scenario,'period':period,'variant':variant['suffix'],'N':int(b.nobs)}
                comparisons.append({**tags,'prediction_max_diff':float(np.max(np.abs(a.fittedvalues-b.fittedvalues))),
                                    'residual_max_diff':float(np.max(np.abs(a.resid-b.resid))),'R2_before':a.rsquared,'R2_after':b.rsquared,
                                    'adj_R2_before':a.rsquared_adj,'adj_R2_after':b.rsquared_adj,'PASS':True})
                ax=pd.DataFrame(a.model.exog,columns=a.model.exog_names); bx=pd.DataFrame(b.model.exog,columns=b.model.exog_names)
                av={term:variance_inflation_factor(ax.to_numpy(),i) for i,term in enumerate(ax.columns) if term!='const'}
                bv={term:variance_inflation_factor(bx.to_numpy(),i) for i,term in enumerate(bx.columns) if term!='const'}
                vifs.extend({**tags,'variable':term,'VIF_before':av[term],'VIF_after':bv[term]} for term in av)
                est=o.get_estimation_sample(frame,c,spec)
                for product,record in new['interaction_centring'].items():
                    rows=record['inputs']; raw=est[rows[0]['column']]*est[rows[1]['column']]
                    info={**tags,'interaction':product,'product_mean':record['product_mean'],'product_SD_ddof0':record['product_sd_ddof0'],
                          'coefficient_before':a.params[product],'coefficient_after':b.params[product],
                          't_before':a.tvalues[product],'t_after':b.tvalues[product],'p_before':a.pvalues[product],'p_after':b.pvalues[product],
                          'VIF_before':av[product],'VIF_after':bv[product]}
                    for i,item in enumerate(rows,1):
                        info[f'input_{i}']=item['column']; info[f'correlation_before_{i}']=raw.corr(est[item['column']]); info[f'correlation_after_{i}']=est[product].corr(est[item['column']])
                        inputs.append({**tags,'interaction':product,**item,'product_mean':record['product_mean'],'product_SD_ddof0':record['product_sd_ddof0']})
                    products.append(info)
    product_df=pd.DataFrame(products)
    tables={'README':pd.DataFrame([('Purpose','Mean-centred interaction audit: input means are actual scenario-period complete cases; binary metadata retains 0/1. All reported SDs use ddof=0.'),
                                  ('Equivalence','64 OLS fits: same observations, predictions/residuals, R², adjusted R² and interaction t/p. Raw product coefficients unchanged; standardised coefficients can change.'),
                                  ('Interpretation','Continuous inputs are centred before multiplying; ordinary regressors retain their established preprocessing. Products then follow standardisation metadata. Interpret coefficients through conditional effects.'),
                                  *manual_exclusion_readme_rows(audit)],columns=['item','description']),
            'Comparison':product_df.loc[product_df.variant.eq('winsor_std'),['scenario','period','N','coefficient_before','coefficient_after','p_before','p_after','VIF_before','VIF_after']],
            'Equivalence':pd.DataFrame(comparisons),'Input_Means_SDs':pd.DataFrame(inputs),
            'Interaction_Details':product_df,'Predictor_VIF':pd.DataFrame(vifs)}
    # Migration checks are historical references, not an active results report.
    # Preserve every existing snapshot rather than overwrite the first audit.
    destination = Path(output_path) if output_path is not None else Path('archive') / f'results_interaction_centring_{date.today().isoformat()}.xlsx'
    sequence = 2
    while destination.exists() and output_path is None:
        destination = Path('archive') / f'results_interaction_centring_snapshot_{sequence}_{date.today().isoformat()}.xlsx'
        sequence += 1
    if destination.exists():
        raise FileExistsError(f'Historical audit already exists: {destination}')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(destination,engine='xlsxwriter') as writer:
        header=writer.book.add_format({'bold':True,'bg_color':'#1F4E78','font_color':'white','text_wrap':True,'valign':'vcenter'})
        decimal=writer.book.add_format({'num_format':'0.000000','font_size':10})
        integer=writer.book.add_format({'num_format':'#,##0','font_size':10})
        scientific=writer.book.add_format({'num_format':'0.00E+00','font_size':10})
        for name,table in tables.items():
            table.to_excel(writer,sheet_name=name,index=False)
            ws=writer.sheets[name]; ws.freeze_panes(1,2); ws.autofilter(0,0,len(table),len(table.columns)-1)
            ws.hide_gridlines(2); ws.set_default_row(18); ws.set_row(0,32)
            ws.set_column(0,len(table.columns)-1,20,decimal)
            fmt=writer.book.add_format({'text_wrap':True,'valign':'top','font_size':10})
            for i,column in enumerate(table.columns):
                ws.write(0,i,column,header)
                if column=='N': ws.set_column(i,i,12,integer)
                if column.endswith('_max_diff'): ws.set_column(i,i,24,scientific)
                if table[column].dtype=='object': ws.set_column(i,i,38,fmt)
                if column=='scenario': ws.set_column(i,i,44,fmt)
                if column=='period': ws.set_column(i,i,10,fmt)
            if name=='README':
                ws.set_column(1,1,100,fmt)
                import textwrap
                for i,description in enumerate(table.description,start=1):
                    ws.set_row(i,max(32,15*max(1,len(textwrap.wrap(str(description),width=95)))+6))
    print('PASS: all 64 actual OLS model comparisons. Maximum prediction difference:',max(r['prediction_max_diff'] for r in comparisons))
    print('Historical audit saved:', destination)
    print(product_df.loc[product_df.variant.eq('winsor_std'),['scenario','period','coefficient_before','coefficient_after','VIF_before','VIF_after','p_after']].to_string(index=False))
    return tables


def severe_equivalence():
    """Check shared centring preserves supplementary fits and total AMEs."""
    import code_severe_p1_decline as severe
    from scipy.special import expit
    old_setting=o.CONFIG.get('centre_interaction_inputs',True)
    try:
        with tempfile.TemporaryDirectory(prefix='grip_centring_severe_') as folder:
            o.CONFIG['centre_interaction_inputs']=False
            before=severe.run_analysis({'output_file':str(Path(folder)/'before.xlsx')})
            o.CONFIG['centre_interaction_inputs']=True
            after=severe.run_analysis({'output_file':str(Path(folder)/'after.xlsx')})
            for key,post in after['logits'].items():
                pre=before['logits'][key]; sample=key[0]
                a=expit(before['prepared'][sample,'P1'].x.to_numpy()@pre['fit'].beta)
                b=expit(after['prepared'][sample,'P1'].x.to_numpy()@post['fit'].beta)
                np.testing.assert_allclose(a,b,atol=1e-8)
                for column in ['AME','std_error','p_value']:
                    np.testing.assert_allclose(pre['marginal_effects'].set_index('variable')[column],post['marginal_effects'].set_index('variable')[column],atol=1e-7)
            for block in ['growths','interactions']:
                for key,post in after[block].items():
                    pre=before[block][key]
                    np.testing.assert_allclose(pre['fit'].fittedvalues,post['fit'].fittedvalues,atol=1e-8)
                    if block=='interactions':
                        np.testing.assert_allclose(pre['slopes'][['estimate','std_error','p_value']],post['slopes'][['estimate','std_error','p_value']],atol=1e-7)
    finally:
        o.CONFIG['centre_interaction_inputs']=old_setting
    print('PASS: changing the unused centring setting preserves all six additive Firth fits/AMEs and twelve additive supplementary OLS fits.')


if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(CentringTests)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful(): raise SystemExit(1)
    actual_model_audit()
    severe_equivalence()
