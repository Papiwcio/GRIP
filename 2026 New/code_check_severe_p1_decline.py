"""Independent mathematical and OLS-alignment checks for the P1 supplement."""
from pathlib import Path
import hashlib

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit
from scipy.stats import norm

import code_severe_p1_decline as analysis
import code_ols_scenarios as ols


def mathematical_checks():
    indicator = analysis.bottom_indicator(pd.Series([-.25, -.20, -.15, .10, np.nan, np.inf]), 20)
    assert indicator.iloc[:4].tolist() == [1, 0, 0, 0]
    assert indicator.iloc[4:].isna().all()
    for threshold in [15, 20, 25]:
        cutoff=-threshold/100
        boundary=analysis.bottom_indicator(pd.Series([np.nextafter(cutoff,-np.inf),cutoff,np.nextafter(cutoff,np.inf),np.nan]),threshold)
        assert boundary.iloc[:3].tolist()==[1,0,0] and pd.isna(boundary.iloc[3])
    # Saturated two-group logit: Firth has the exact add-half odds solution,
    # including complete separation, so this checks against a closed form.
    x = np.column_stack([np.ones(20), np.r_[np.zeros(10), np.ones(10)]])
    y = np.r_[np.zeros(10), np.ones(10)]
    fit = analysis.fit_firth(x, y)
    beta0 = np.log(.5 / 10.5)
    expected = np.array([beta0, np.log(10.5 / .5) - beta0])
    np.testing.assert_allclose(fit.beta, expected, atol=1e-7)
    # Independent general-purpose optimiser against the penalised likelihood.
    optimizer = minimize(lambda b: -analysis.firth_state(x, y, b)[0], np.zeros(2), method='BFGS', options={'gtol': 1e-7})
    np.testing.assert_allclose(fit.beta, optimizer.x, atol=2e-5)
    for position in range(2):
        change = np.eye(2)[position] * 1e-5
        numerical = (analysis.firth_state(x, y, fit.beta + change)[0] - analysis.firth_state(x, y, fit.beta - change)[0]) / 2e-5
        np.testing.assert_allclose(numerical, analysis.firth_state(x, y, fit.beta)[1][position], atol=1e-7)
    restricted = analysis.fit_firth(x, y, active=[0])
    assert restricted.beta[1] == 0 and fit.penalized_loglik > restricted.penalized_loglik
    # AME and its delta-method gradient against finite differences.
    rng = np.random.default_rng(431)
    raw_a, raw_b = rng.normal(size=(2, 120))
    sa, sb, sp = raw_a.std(), raw_b.std(), (raw_a * raw_b).std()
    design = np.column_stack([np.ones(120), (raw_a - raw_a.mean())/sa, (raw_b - raw_b.mean())/sb,
                              (raw_a*raw_b - (raw_a*raw_b).mean())/sp])
    derivative = np.zeros_like(design)
    derivative[:,1] = 1
    derivative[:,3] = raw_b * sa / sp
    beta = np.array([-.4, .2, -.1, .3])
    statistics, gradient = analysis.continuous_ame(design, beta, np.eye(4), derivative)
    epsilon = 1e-5
    # Vary the original predictor; regenerate its standardised product using
    # the original estimation-sample scaling, rather than holding it fixed.
    plus = design.copy(); minus = design.copy()
    plus[:,1] += epsilon; minus[:,1] -= epsilon
    plus[:,3] += raw_b * sa / sp * epsilon
    minus[:,3] -= raw_b * sa / sp * epsilon
    finite_effect = np.mean((expit(plus @ beta) - expit(minus @ beta)) / (2*epsilon))
    np.testing.assert_allclose(statistics['estimate'], finite_effect, atol=1e-10)
    for position in range(4):
        change = np.eye(4)[position] * epsilon
        finite = (analysis.continuous_ame(design,beta+change,np.eye(4),derivative)[0]['estimate'] - analysis.continuous_ame(design,beta-change,np.eye(4),derivative)[0]['estimate']) / (2*epsilon)
        np.testing.assert_allclose(gradient[position], finite, atol=1e-9)
    print('PASS: strict threshold/missingness boundaries, exact separated-logit solution, independent optimiser, adjusted score, and total-AME gradients.')


def check_results(result):
    source = pd.read_parquet(analysis.CONFIG['input_file'])
    shared = result['shared']
    models = ols.build_models(shared)
    assert set(result['frames'])=={'Rank2019','Rank2019_Manufacturing'}
    source = ols.add_interaction_columns(source, shared, models)
    for sample, frame in result['frames'].items():
        for threshold in [15,20,25]:
            expected = frame['ngrowth_P1' if shared['growth_mode']=='nominal' else 'rgrowth_P1'].lt(-threshold/100).astype('Int64')
            pd.testing.assert_series_equal(frame[f'BottomP1_{threshold}'], expected, check_names=False)
        assert frame.BottomP1_25.sum() <= frame.BottomP1_20.sum() <= frame.BottomP1_15.sum()
        for period in ['P1','P2','P3']:
            prepared = result['prepared'][sample,period]
            estimated = ols.get_estimation_sample(frame,shared,models[period])
            pd.testing.assert_index_equal(prepared.x.index,estimated.index)
            levels = ols.get_categorical_levels(frame,shared)
            registry = ols.build_variable_registry(shared,models,levels)
            matrix,_,_,_ = ols.build_design_matrix(estimated,models[period]['regressors'],levels,registry,shared,True)
            pd.testing.assert_frame_equal(prepared.x,matrix)
            if period in ['P2','P3']:
                variant = next(v for v in shared['model_variants'] if v['suffix']=='winsor_std')
                main = ols.run_model_variant(period,models[period],variant,frame,levels,registry,shared)
                np.testing.assert_allclose(prepared.y, main['result'].model.endog, atol=1e-12)
                assert prepared.lower==main['winsorisation_lower_bound'] and prepared.upper==main['winsorisation_upper_bound']
                for threshold in [15,20,25]:
                    augmented = result['interactions'][sample,threshold,period]
                    name=f'BottomP1_{threshold}'
                    assert sum(v.startswith('BottomP1_') for v in augmented['fit'].params.index)==1
                    covariate=f'profit_margin_start_{period}'
                    product=f'profitability_z_x_{name}'
                    actual=augmented['fit'].model.exog[:,augmented['fit'].model.exog_names.index(product)]
                    expected=prepared.x[covariate]*frame.loc[prepared.x.index,name].astype(float)
                    np.testing.assert_array_equal(actual,expected)
                    contrast=np.zeros(len(augmented['fit'].params))
                    for term in [covariate,product]:
                        contrast[augmented['fit'].params.index.get_loc(term)]=1
                    independently=augmented['fit'].t_test(contrast)
                    reported=augmented['slopes'].set_index('group').loc[name]
                    np.testing.assert_allclose(reported['estimate'],independently.effect.item(),atol=1e-12)
                    np.testing.assert_allclose(reported['std_error'],independently.sd.item(),atol=1e-12)
                    np.testing.assert_allclose(reported['p_value'],independently.pvalue.item(),atol=1e-12)
    if result['config']['logit_method']=='firth':
        assert len(result['logits'])==6
        assert all(r['fit'].score_max<result['config']['tolerance'] for r in result['logits'].values())
    assert len(result['growths'])==12 and len(result['interactions'])==12
    for sheet,sections in result['sections'].items():
        assert any(not table.empty for _,table in sections),sheet
    sensitivity=result['audit']['P2_lag_sensitivity']
    for sample, rows in sensitivity.groupby('sample'):
        assert len(rows)==2 and rows.N.nunique()==1
        assert rows.N.iloc[0]==len(result['prepared'][sample,'P2'].x)
    separation=result['audit']['separation']
    assert separation.loc[separation['sample'].eq('Rank2019'),'separation'].eq('None').all()
    assert separation.loc[separation['sample'].eq('Rank2019_Manufacturing'),'separation'].eq('Quasi-complete').all()
    assert result['audit']['interaction_units'].identical_units.eq('Yes').all()
    assert result['audit']['interaction_units'].combined_effect_valid.eq('Yes').all()
    print('PASS: shared OLS samples/designs/outcome preparation; fixed nested groups; exactly one dummy per model; original-scale interaction; slope covariance/inference; all required workbook sections.')


def independent_data_checks(result):
    """Reproduce OLS algebra and actual-data AMEs without reporting helpers."""
    for fitted_block in ['growths', 'interactions']:
        for model in result[fitted_block].values():
            fit = model['fit']
            x, y = fit.model.exog, fit.model.endog
            q, r = np.linalg.qr(x, mode='reduced')
            beta = np.linalg.solve(r, q.T @ y)
            residual = y - x @ beta
            inverse_r = np.linalg.solve(r, np.eye(r.shape[0]))
            covariance = inverse_r @ inverse_r.T * (residual @ residual) / fit.df_resid
            np.testing.assert_allclose(beta, fit.params, atol=1e-9)
            np.testing.assert_allclose(covariance, fit.cov_params(), atol=1e-9)
    checked = 0
    for sample in result['frames']:
        prepared = result['prepared'][sample, 'P1']
        model = result['logits'][sample, 20]
        fit = model['fit']
        columns = prepared.x.columns.tolist()
        x = prepared.x.to_numpy()
        for record in model['marginal_effects'].itertuples():
            variable = record.variable
            plus, minus = x.copy(), x.copy()
            if record.effect_type.startswith('Total derivative'):
                epsilon = 1e-4
                scale = prepared.scales[variable]
                raw_plus, raw_minus = prepared.estimation.copy(), prepared.estimation.copy()
                shift = epsilon * (scale['sd'] if scale['standardised'] else 1.)
                raw_plus[variable] += shift
                raw_minus[variable] -= shift
                for raw, design in [(raw_plus, plus), (raw_minus, minus)]:
                    for interaction in ols.get_active_interactions():
                        names = [ols.resolve_interaction_variable(result['shared'], v, 'P1') for v in interaction['variables']]
                        product = ols.build_interaction_column_name(interaction['name'], 'P1')
                        raw[product] = raw[names[0]] * raw[names[1]]
                    for name, scale in prepared.scales.items():
                        design[:, columns.index(name)] = (raw[name] - scale['mean']) / scale['sd'] if scale['standardised'] else raw[name]
                divisor = 2 * epsilon
            else:
                divisor = 1.
                if variable == 'owner_num':
                    plus[:, columns.index(variable)] = 1
                    minus[:, columns.index(variable)] = 0
                else:
                    for name in columns:
                        if name.startswith('sector_en_'):
                            plus[:, columns.index(name)] = 0
                            minus[:, columns.index(name)] = 0
                    plus[:, columns.index(variable)] = 1
            def probability_difference(beta):
                return np.mean(expit(plus @ beta) - expit(minus @ beta)) / divisor
            value = probability_difference(fit.beta)
            steps = np.eye(len(fit.beta)) * 1e-4
            gradient = np.array([(probability_difference(fit.beta + step) - probability_difference(fit.beta - step)) / 2e-4 for step in steps])
            se = np.sqrt(gradient @ fit.covariance @ gradient)
            p_value = 2 * norm.sf(abs(value / se))
            np.testing.assert_allclose([value, se, p_value], [record.AME, record.std_error, record.p_value], atol=1e-6, rtol=1e-5)
            checked += 1
    print(f'PASS: independent QR coefficients/covariances for 24 OLS fits and numerical probability/gradient checks for all {checked} principal AMEs.')


if __name__=='__main__':
    mathematical_checks()
    files=[Path('data_core_2018-2024.parquet'),Path('data_period_2018-2024.parquet'),
           Path('results_ols_scenarios.xlsx'),Path('results_quantile.xlsx'),Path('results_diagnostics_trajectories.xlsx')]
    hashes={path:hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    results=analysis.run_analysis()
    check_results(results)
    independent_data_checks(results)
    for path,expected in hashes.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest()==expected,path
    print('PASS: canonical data and all main analysis workbooks unchanged.')
