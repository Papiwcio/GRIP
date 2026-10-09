"""Validate, stage, verify and atomically publish the unified quantitative workflow."""
from __future__ import annotations
import argparse
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo
import code_common_samples as common
from code_audit_data_quality import json_value

ROOT = Path(__file__).resolve().parent
OUTPUTS = ['results_ols_scenarios.xlsx','results_ols_interactions.xlsx','results_quantile.xlsx',
           'results_diagnostics_trajectories.xlsx','results_severe_p1_decline_analysis.xlsx','results_data_quality_and_samples.xlsx']


@contextmanager
def working_directory(path):
    previous = Path.cwd(); os.chdir(path)
    try: yield
    finally: os.chdir(previous)


def archive(path):
    date = datetime.now(ZoneInfo('Europe/Warsaw')).date().isoformat()
    target = ROOT / 'archive' / f'{path.stem}_{date}{path.suffix}'
    sequence = 2
    while target.exists():
        target = ROOT / 'archive' / f'{path.stem}_{date}_{sequence}{path.suffix}';sequence += 1
    target.parent.mkdir(exist_ok=True)
    shutil.copy2(path,target)
    return target


def publish(staging, files):
    # All checks precede this transaction. Roll back every replacement on failure.
    old = {}
    for name in files:
        p = ROOT/name
        if p.exists(): old[name]=archive(p)
    replaced = []
    try:
        for name in files:
            shutil.copy2(staging/name,ROOT/(name+'.publishing'))
            (ROOT/(name+'.publishing')).replace(ROOT/name);replaced.append(name)
    except Exception:
        for name in replaced:
            if name in old: shutil.copy2(old[name],ROOT/name)
            else: (ROOT/name).unlink(missing_ok=True)
        raise


def main(argv=None):
    args=argparse.ArgumentParser()
    args.add_argument('--adopt-specification',action='store_true',help='Explicitly adopt the reported current specifications and eligibility revision.')
    args.add_argument('--audit-only',action='store_true',help='Report candidate membership without estimation or production publication.')
    args.add_argument('--quality-only',action='store_true',help='Refresh consolidated audit evidence for an unchanged approved sample; preserve regression workbooks.')
    opts=args.parse_args(argv)
    previous=json.loads(common.STATE.read_text()) if common.STATE.exists() else None
    context=common.build_context()
    candidate=context.manifest()
    changed=not previous or previous['specification_signature']!=candidate['specification_signature'] or previous['source_hashes']!=candidate['source_hashes'] or previous['populations']!=candidate['populations']
    sample_changes=[]
    for population,ids in context.memberships.items():
        old=set(previous.get('populations',{}).get(population,{}).get('company_ids',[])) if previous else set()
        evidence=context.details.loc[context.details.nip.isin(old-ids)]
        sample_changes.append(dict(population=population,previous_N=len(old) if previous else None,new_N=len(ids),
                                   added_N=len(ids-old),removed_N=len(old-ids),added_company_ids=sorted(ids-old),removed_company_ids=sorted(old-ids),
                                   responsible_variables=sorted(set(evidence.variable)),observation_years=sorted(str(y) for y in evidence.year.dropna().unique()),
                                   responsible_specifications=sorted(set(evidence.specifications))))
    proposal={'sample_changes':sample_changes,'previous':previous,'proposed':candidate,'requirements':context.requirements.to_dict('records'),'exclusions':context.details.to_dict('records')}
    if changed:
        (ROOT/'results_sample_change_proposal.json').write_text(json.dumps(json_value(proposal),ensure_ascii=False,indent=2))
        print('PROPOSED COMMON SAMPLES:',{p:len(s) for p,s in context.memberships.items()},flush=True)
        if not opts.adopt_specification or opts.audit_only:
            print('Proposal written. No production files changed. Explicit adoption is required.',flush=True)
            return context
    elif opts.audit_only:
        print('Approved sample unchanged; no production files changed.',flush=True);return context
    common.activate(context)
    source_hashes=candidate['source_hashes']
    staging=Path(tempfile.mkdtemp(prefix='grip_stage2_'))
    print('Staging outputs:',staging,flush=True)
    if opts.quality_only:
        if changed: raise RuntimeError('Quality-only cannot adopt new membership; run the complete pipeline after approval.')
        from code_check_common_samples import validate_context, validate_workbooks
        validate_context(context);validate_workbooks(ROOT,context)
        context.alignment=pd.read_excel(ROOT/'results_data_quality_and_samples.xlsx',sheet_name='08_MODEL_ALIGNMENT',header=3).to_dict('records')
        from code_report_quality_samples import write
        write(context,staging/'results_data_quality_and_samples.xlsx',previous)
        metadata=json.loads((ROOT/'results_data_quality_and_samples_metadata.json').read_text())
        metadata['output_hashes']['results_data_quality_and_samples.xlsx']=hashlib.sha256((staging/'results_data_quality_and_samples.xlsx').read_bytes()).hexdigest()
        metadata['requirements']=context.requirements.to_dict('records')
        metadata['variable_inventory']=context.quality_audit.inventory
        metadata['recomputed_performance_thresholds']=context.quality_audit.thresholds
        metadata['rule_execution_contexts']=context.quality_audit.contexts
        metadata['source_hashes']=source_hashes
        metadata['code_hashes']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in ROOT.glob('code_*.py')}
        metadata['sample_changes']=sample_changes
        metadata['quality_refresh']='Independent evidence refreshed; analytical outputs and approved membership unchanged.'
        (staging/'results_data_quality_and_samples_metadata.json').write_text(json.dumps(json_value(metadata),ensure_ascii=False,indent=2))
        if any(hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=value for name,value in source_hashes.items()):
            raise RuntimeError('Source changed during quality refresh; production files withheld.')
        publish(staging,['results_data_quality_and_samples.xlsx','results_data_quality_and_samples_metadata.json'])
        print('Consolidated quality audit refreshed; regression workbooks unchanged.',flush=True)
        return context
    # Preserve researcher annotations from current workbooks while rebuilding results.
    for name in OUTPUTS:
        if (ROOT/name).exists(): shutil.copy2(ROOT/name,staging/name)
    import code_ols_scenarios as ols
    import code_quantile as quantile
    import code_diagnostics_trajectories as diagnostics
    import code_severe_p1_decline as severe
    period_path=str(ROOT/'data_period_2018-2024.parquet')
    with working_directory(staging):
        ols_report=ols.run_ols_reports({**ols.CONFIG,'input_file':period_path})
        if ols_report['primary']['models_skipped'] or ols_report['interactions']['models_skipped']:
            raise RuntimeError('OLS numerical/model failure; staged files not published.')
        quantile_report=quantile.run_quantile_regression({**quantile.CONFIG,'input_file':period_path})
        diagnostic_report=diagnostics.run_trajectory_analysis({**diagnostics.CONFIG,'input_file':period_path})
        severe_report=severe.run_analysis({'input_file':period_path})
        from code_report_quality_samples import write
        central_tables=write(context,staging/'results_data_quality_and_samples.xlsx',previous)
    # Verify actual model identities, not equal counts. Every family records IDs at estimation.
    check=pd.DataFrame(context.alignment)
    for population,selected in context.memberships.items():
        rows=check.loc[check.population_id.eq(population)]
        if rows.empty or set(rows.sample_sha256)!={common.fingerprint(selected)} or set(rows.unique_companies)!={len(selected)}:
            raise RuntimeError(f'Cross-family alignment failure: {population}')
    if len(check.loc[check.specification_id.eq('quantile')]) != 48:
        raise RuntimeError('Quantile numerical failure: missing fits; production outputs withheld.')
    from code_check_common_samples import validate_context, validate_workbooks
    validate_context(context)
    validate_workbooks(staging,context)
    for name,value in source_hashes.items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=value:
            raise RuntimeError('Source changed during analysis; production files withheld.')
    # Changes in results must be attributed to sample membership; formulas/estimators remain unchanged.
    baseline=ROOT/'archive'/'results_ols_scenarios_2026-10-09.xlsx'
    old_summary=pd.read_excel(baseline if baseline.exists() else ROOT/'results_ols_scenarios.xlsx',sheet_name='Model_Summary_Long')
    new_summary=ols_report['primary']['summary_long_df']
    comparison=old_summary.merge(new_summary,on=['scenario','period','model'],suffixes=('_old','_new'))
    old_coefficients=pd.read_excel(baseline if baseline.exists() else ROOT/'results_ols_scenarios.xlsx',sheet_name='Coefficients_Long')
    new_coefficients=ols_report['primary']['coefficients_long_df']
    coefficients=old_coefficients.merge(new_coefficients,on=['scenario','period','model','raw_variable'],suffixes=('_old','_new'))
    selected=coefficients.loc[coefficients.scenario.eq('RANK2019') & coefficients.model.str.endswith('winsor_std') & coefficients.raw_variable.str.startswith(('export_ratio_start_','ln_sales_start_'))]
    report=['# Stage 2 implementation results','',f"Final common company samples: { {p:len(s) for p,s in context.memberships.items()} }",'',
            f"Unique consolidated excluded companies: {len(central_tables['03_EXCLUDED_COMPANIES'])}. Annual and derived review findings remain separate from automatic eligibility exclusions.",'',
            comparison[['scenario','period','model','observations_old','observations_new','R_squared_old','R_squared_new']].to_markdown(index=False),'',
            '## RANK2019 winsor-standardised coefficient changes','', selected[['period','raw_variable','coefficient_old','coefficient_new','p-value_old','p-value_new']].to_markdown(index=False),'',
            'Every recorded estimator and diagnostic uses the same company-ID set per population. Source datasets are unchanged. Coefficients and fit statistics change because company membership, sample means/SDs, interaction centring and within-sample winsor cutoffs change together under the existing formulas. Statistical specifications and estimation settings are unchanged. Independent matched-sample checks are documented in the validation tests.','']
    (staging/'documentation_stage2_implementation_results.md').write_text('\n'.join(report))
    (staging/common.STATE.name).write_text(json.dumps(json_value(candidate),ensure_ascii=False,indent=2))
    metadata=dict(code_hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in ROOT.glob('code_*.py')},sample_changes=sample_changes,source_hashes=source_hashes,specification_signature=context.signature,alignment_rows=len(check),
                  independent_audit_rules=context.quality_audit.rules,coverage=context.quality_audit.coverage,
                  variable_inventory=context.quality_audit.inventory,recomputed_performance_thresholds=context.quality_audit.thresholds,rule_execution_contexts=context.quality_audit.contexts,
                  statistical_references=context.quality_audit.references,requirements=context.requirements.to_dict('records'),
                  output_hashes={name:hashlib.sha256((staging/name).read_bytes()).hexdigest() for name in OUTPUTS})
    (staging/'results_data_quality_and_samples_metadata.json').write_text(json.dumps(json_value(metadata),ensure_ascii=False,indent=2))
    publish(staging,[*OUTPUTS,common.STATE.name,'results_data_quality_and_samples_metadata.json','documentation_stage2_implementation_results.md'])
    # Only after validated publication retire the superseded audit reports.
    for name in ['results_data_quality_audit.xlsx','results_sample_eligibility_audit.xlsx','results_data_quality_audit_metadata.json','results_sample_eligibility_audit.md']:
        path=ROOT/name
        if path.exists(): archive(path);path.unlink()
    (ROOT/'results_sample_change_proposal.json').unlink(missing_ok=True)
    print('Published validated unified outputs; source hashes unchanged.',flush=True)
    return context


if __name__=='__main__': main()
