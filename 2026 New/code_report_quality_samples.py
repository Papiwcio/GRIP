"""Consolidated reporting from the shared process; no sample decisions here."""
from collections import defaultdict
import json
import textwrap
from pathlib import Path
import numpy as np
import pandas as pd
import xlsxwriter
from code_common_samples import fingerprint
from code_audit_data_quality import json_value, merge_decisions

SHEETS = ['00_WORKFLOW_OVERVIEW','01_SAMPLE_FLOW','02_SAMPLE_OVERLAP','03_EXCLUDED_COMPANIES',
          '04_EXCLUSION_DETAILS','05_SAMPLE_MEMBERSHIP','06_VALIDITY_RULES','07_DATA_QUALITY_SUMMARY',
          '08_MODEL_ALIGNMENT','09_SAMPLE_CHANGE_LOG']


def tables(context, previous=None):
    detail = context.details.copy()
    quality = context.quality
    # Reuse independent checks and human-review records, without adopting speculative exclusions.
    decisions = Path(__file__).resolve().parent / 'data_quality_audit_decisions.csv'
    if decisions.exists(): quality = merge_decisions(quality,decisions,persist=False)
    from code_config import MANUAL_EXCLUSIONS, MANUAL_EXCLUSION_REASONS
    for index,row in detail.loc[detail.rule.eq('MANUAL_FIRM_EXCLUSION')].iterrows():
        rule=next(r for r in MANUAL_EXCLUSIONS['companies'] if r['nip']==row.nip or r['company']==str(row.company).strip())
        detail.loc[index,'reason']=f"{rule['reason_code']}: {MANUAL_EXCLUSION_REASONS[rule['reason_code']]}"
    records = detail.to_dict('records')
    for r in quality.to_dict('records'):
        # Export source checks now have one exact approved policy rule; retain review context in summary.
        if r['rule_id'] in {'E01','E02'} and r['variable'] in {'exports','export_ratio'}: continue
        records.append(dict(nip=str(r['nip']),company=r['company'],year=json_value(r['year']),variable=r['variable'],
                            observed_value=json_value(r['original_value']),rule=r['rule_id'],violation_type=r['classification'],
                            specifications='',populations='',eligibility_impact=False,review_status=r.get('review_status'),
                            reason=r['reason'],numerator=json_value(r['numerator']),denominator=json_value(r['denominator'])))
    detail = pd.DataFrame(records).drop_duplicates(['nip','year','variable','rule'],keep='first')
    # Add original source amounts to exact export findings.
    annual = context.core.set_index(['nip','year'])
    for index,r in detail.loc[detail.rule.eq('EXPORT_0_1')].iterrows():
        if (r.nip,r.year) in annual.index:
            detail.loc[index,'numerator'] = annual.loc[(r.nip,r.year),'exports']
            detail.loc[index,'denominator'] = annual.loc[(r.nip,r.year),'sales']
    membership, excluded, flows, overlap = [], [], [], []
    blocking = context.details.loc[context.details.eligibility_impact]
    for row in context.source.itertuples():
        reasons = blocking.loc[blocking.nip.eq(row.nip)]
        statuses = {}
        for population in context.memberships:
            statuses[population] = 'OUTSIDE_POPULATION' if row.nip not in context.scopes[population] else 'INCLUDED' if row.nip in context.memberships[population] else 'EXCLUDED'
        record = dict(nip=row.nip,company=row.company,**statuses)
        membership.append(record)
        if 'EXCLUDED' in statuses.values():
            reason_codes = set(reasons.rule)
            primary = 'MANUAL_FIRM_EXCLUSION' if row.nip in context.manual else 'INVALID_REQUIRED_OBSERVATION' if reasons.violation_type.eq('INVALID_ANNUAL').any() else 'MISSING_REQUIRED_DATA'
            manual_reason=''
            if row.nip in context.manual:
                entry=next(r for r in MANUAL_EXCLUSIONS['companies'] if r['nip']==row.nip or r['company']==str(row.company).strip())
                manual_reason=f"{entry['reason_code']}: {MANUAL_EXCLUSION_REASONS[entry['reason_code']]}"
            excluded.append(dict(**record,data_quality_status='FIRM_WIDE_EXCLUSION' if row.nip in context.manual else 'INVALID_REQUIRED' if reasons.violation_type.eq('INVALID_ANNUAL').any() else 'MISSING_REQUIRED',
                                 primary_exclusion_reason=manual_reason or primary,additional_reasons='; '.join(sorted(reason_codes)),
                                 variables='; '.join(sorted(set(reasons.variable))),years='; '.join(sorted(str(int(y)) for y in reasons.year.dropna().unique())),
                                 specifications='; '.join(sorted({tag.split(':')[0] for value in reasons.specifications for tag in str(value).split('; ') if tag})),final_analytical_status='EXCLUDED_FROM_ONE_OR_MORE_ELIGIBLE_POPULATIONS'))
    for population, final in context.memberships.items():
        scope = context.scopes[population]
        invalid = set(blocking.loc[blocking.populations.str.split('; ').apply(lambda x: population in x) & blocking.violation_type.eq('INVALID_ANNUAL'),'nip'])
        remaining = scope - context.manual
        manual_N = len(scope - remaining)
        invalid_N = len(remaining & invalid); remaining -= invalid
        flag='has_complete_rtrajectory' if context.specifications[0]['settings']['growth_mode']=='real' else 'has_complete_ntrajectory'
        flags = context.source.set_index('nip')[flag]
        incomplete = {n for n in remaining if flags.at[n] != 1}
        trajectory_N = len(incomplete); remaining -= incomplete
        population_sets = {tag:s for (p,tag),s in context.eligible.items() if p == population}
        union = set.union(*population_sets.values()) & remaining
        missing_all_N = len(remaining - union)
        common_loss_N = len(union - final)
        assert len(scope) == manual_N + invalid_N + trajectory_N + missing_all_N + common_loss_N + len(final)
        flows.append(dict(population=population,source_companies=len(context.source),outside_population=len(context.source)-len(scope),population_companies=len(scope),
                          firm_wide_exclusions=manual_N,invalid_required_observation_companies=invalid_N,
                          incomplete_trajectory_companies=trajectory_N,missing_all_model_eligibility_companies=missing_all_N,
                          common_alignment_losses=common_loss_N,final_common_N=len(final),sample_id=f'COMMON_{population}',sample_sha256=fingerprint(final)))
        for tag, selected in population_sets.items():
            overlap.append(dict(population=population,specification=tag,eligible_N=len(selected),common_N=len(final),intersection_N=len(selected & final),additional_common_loss_N=len(selected - final),
                                eligible_sha256=fingerprint(selected),common_sha256=fingerprint(final)))
    rules = pd.DataFrame(context.quality_audit.rules).copy()
    rules['automatic_exclusion'] = False
    automatic = [dict(rule_id=rule,description=description,classification=classification,automatic_exclusion=True,threshold=threshold)
                 for rule,description,classification,threshold in [
                     ('MANUAL_FIRM_EXCLUSION','Documented verified NIP or exact name; whole record non-comparable.','FIRM_WIDE','Shared manual configuration'),
                     ('REQUIRED_MISSING','Missing/non-numeric essential model input or underlying source.','ELIGIBILITY','No imputation'),
                     ('EXPORT_0_1','Approved comparable-export-intensity eligibility; boundaries unresolved scientifically but policy is now adopted.','INVALID_ANNUAL','0 <= exports/sales <= 1'),
                     ('REQUIRED_POSITIVE','Positive sales/employment/assets where needed for analysis.','INVALID_ANNUAL','>0'),
                     ('REQUIRED_NONZERO','Denominator must be finite and nonzero; negative equity retained.','INVALID_ANNUAL','!=0'),
                     ('REQUIRED_BINARY','Documented indicator bounds.','INVALID_ANNUAL','0 or 1'),
                     ('INDEPENDENT_INTEGRITY','Required source/derived variable fails an independent mathematical or documented constraint.','INVALID_ANNUAL','Existing auditor tolerances'),
                     ('INCOMPLETE_TRAJECTORY','Existing complete trajectory requirement retained.','ELIGIBILITY','Approved growth family P1/P2/P3 all available')]]
    rules = pd.concat([pd.DataFrame(automatic),rules],ignore_index=True)
    summary = []
    for field in ['classification','rule_id','year','variable','dataset']:
        for value,g in quality.groupby(field,dropna=False):
            summary.append(dict(section='Review-only audit',category=f'{field}: {value}',violations=len(g),unique_companies=g.nip.nunique()))
    for r in context.quality_audit.coverage:
        summary.append(dict(section='Coverage',category=f"{r['dataset']}: {r['variable']}",year=r.get('year'),rows=r['rows'],missing=r['missing'],nonmissing=r['nonmissing'],status=r.get('status')))
    for kind,g in detail.groupby('violation_type'):
        summary.append(dict(section='Consolidated violations',category=kind,violations=len(g),unique_companies=g.nip.nunique()))
    changes = []
    for population, final in context.memberships.items():
        old = set(previous.get('populations',{}).get(population,{}).get('company_ids',[])) if previous else set()
        for action, members in [('ADDED',final-old),('REMOVED',old-final)]:
            for nip in sorted(members):
                evidence = blocking.loc[blocking.nip.eq(nip)]
                changes.append(dict(population=population,change=action,nip=nip,previous_N=len(old) if previous else None,new_N=len(final),
                                    previous_sample='Previous approved common' if previous else 'INITIAL_ADOPTION: no previous approved common sample',
                                    variables='; '.join(sorted(set(evidence.variable))),years='; '.join(str(int(y)) for y in evidence.year.dropna().unique()),
                                    responsible_specifications='; '.join(sorted(set(evidence.specifications))),specification_signature=context.signature))
        if not (final ^ old): changes.append(dict(population=population,change='UNCHANGED',previous_N=len(old),new_N=len(final),specification_signature=context.signature))
    # First adoption additionally reports exact changes from previous period-specific fits.
    if not previous:
        for population in context.memberships:
            for period in ['P1','P2','P3','FULL']:
                frame = context.source.loc[context.source.nip.isin(context.scopes[population] - context.manual) & context.source.has_complete_ntrajectory.eq(1)]
                import code_ols_scenarios as e
                c = e.normalise_config({**e.CONFIG,'include_interactions':False})
                model = e.build_models(c)[period]
                cols = [model['dependent'],*model['regressors'],*e.get_categorical_columns(c)]
                work = frame[cols].copy(); nums=[v for v in cols if v not in e.get_categorical_columns(c)]
                work[nums] = work[nums].apply(pd.to_numeric,errors='coerce')
                old = set(frame.loc[work.dropna().index,'nip'])
                for nip in sorted(old-context.memberships[population]):
                    evidence = blocking.loc[blocking.nip.eq(nip)]
                    changes.append(dict(population=population,change='REMOVED_VS_PREVIOUS_PERIOD_MODEL',nip=nip,period=period,previous_N=len(old),new_N=len(context.memberships[population]),previous_sample='Previous primary period-specific OLS',variables='; '.join(sorted(set(evidence.variable))),years='; '.join(str(int(y)) for y in evidence.year.dropna().unique()),responsible_specifications='; '.join(sorted(set(evidence.specifications))),specification_signature=context.signature))
    existing = Path(__file__).resolve().parent/'results_data_quality_and_samples.xlsx'
    if existing.exists():
        history=pd.read_excel(existing,sheet_name='09_SAMPLE_CHANGE_LOG',header=3,dtype={'nip':str})
        current=pd.DataFrame(changes)
        changes=pd.concat([history,current],ignore_index=True).drop_duplicates().to_dict('records')
    output = {'01_SAMPLE_FLOW':pd.DataFrame(flows),'02_SAMPLE_OVERLAP':pd.DataFrame(overlap),'03_EXCLUDED_COMPANIES':pd.DataFrame(excluded),
              '04_EXCLUSION_DETAILS':detail,'05_SAMPLE_MEMBERSHIP':pd.DataFrame(membership),'06_VALIDITY_RULES':rules,
              '07_DATA_QUALITY_SUMMARY':pd.DataFrame(summary),'08_MODEL_ALIGNMENT':pd.DataFrame(context.alignment),'09_SAMPLE_CHANGE_LOG':pd.DataFrame(changes)}
    assert not output['03_EXCLUDED_COMPANIES'].nip.duplicated().any()
    assert not detail.duplicated(['nip','year','variable','rule']).any()
    return output


def write(context, destination, previous=None):
    output = tables(context,previous)
    with xlsxwriter.Workbook(destination) as book:
        base = {'font_name':'Arial','font_size':10,'valign':'vcenter'}
        body = book.add_format(base); text=book.add_format({**base,'text_wrap':True})
        number=book.add_format({**base,'num_format':'#,##0.00000000'}); integer=book.add_format({**base,'num_format':'#,##0'})
        head=book.add_format({**base,'bold':True,'text_wrap':True,'bg_color':'#1F4E78','font_color':'white','align':'center'})
        title=book.add_format({**base,'font_size':14,'bold':True,'font_color':'#1F4E78'})
        box=book.add_format({**base,'align':'center','text_wrap':True,'bg_color':'#D9EAF7','border':1,'border_color':'#A6B5C5'})
        workflow=book.add_worksheet(SHEETS[0]);workflow.hide_gridlines(2);workflow.set_column('A:H',21)
        workflow.write('A2','GRIP quantitative workflow',title)
        for bounds,label in [('A4:D5','Canonical Core and Period datasets\nSource observations remain unchanged'),('E4:H5','All approved model specifications\nControls, interactions, lags and outcomes'),('A7:H8','Shared logical validation\nManual exclusions and traceable annual violations'),('A10:H11','Population restrictions and dynamic model-driven eligibility'),('A13:H14','ONE COMMON COMPANY SAMPLE PER POPULATION'),('A16:B17','OLS scenarios'),('C16:D17','OLS interactions'),('E16:F17','Quantile regression'),('G16:H17','Trajectories and severe decline'),('A19:H20','Verify identical company IDs and fingerprints before publication'),('A22:H23','Central exclusions, sample membership and change log\nThis workbook owns exclusion reporting')]:
            workflow.merge_range(bounds,label,box)
        for r in [6,9,12,15,18,21]: workflow.write(r-1,3,'↓',book.add_format({**base,'font_size':14,'align':'center'}))
        for r in range(3,23): workflow.set_row(r,23)
        workflow.merge_range('A26:H26','Source lineage, rule details and changing specifications: GRIP_quantitative_workflow.md',text)
        workflow.merge_range('A28:H29','2018 supplies sales only for P1 lag. Extreme and suspicious findings are reviewed, not automatically excluded. Winsorisation happens after eligibility.',text)
        for name in SHEETS[1:]:
            sheet=book.add_worksheet(name);sheet.hide_gridlines(2);sheet.freeze_panes(4,2)
            frame=output[name]; records=json_value(frame.to_dict('records')); cols=list(frame.columns)
            sheet.write(1,0,name[3:].replace('_',' ').title(),title)
            sheet.set_row(3,45)
            widths={}
            for col, field in enumerate(cols):
                width=58 if field=='company' else 70 if field in {'reason','description','formula_threshold_justification','limitation','primary_exclusion_reason'} else 45 if field in {'variables','additional_reasons','specifications','responsible_specifications','source_variables'} else 68 if 'sha256' in field else 24
                widths[field]=width
                fmt=number if field in {'observed_value','numerator','denominator'} else integer if field.endswith('_N') or field in {'violations','unique_companies','source_companies','population_companies','outside_population','rows','missing','nonmissing'} else text
                sheet.set_column(col,col,width,fmt)
                for offset,record in enumerate(records,4):
                    value=record.get(field)
                    if value is None: continue
                    if isinstance(value,str): sheet.write_string(offset,col,value,fmt)
                    elif isinstance(value,bool): sheet.write_boolean(offset,col,value,fmt)
                    else: sheet.write_number(offset,col,float(value),fmt)
            for row,record in enumerate(records,4):
                lines=max([1]+[len(textwrap.wrap(str(value),width=max(12,int(widths[field]*1.15)))) for field,value in record.items() if field in widths and isinstance(value,str) and 'sha256' not in field])
                sheet.set_row(row,max(22,15*lines+4))
            sheet.add_table(3,0,max(4,len(records)+3),len(cols)-1,{'name':'Common'+name[:2], 'columns':[{'header':c,'header_format':head} for c in cols],'style':'Table Style Medium 2'})
            if name=='06_VALIDITY_RULES':
                start=len(records)+8;sheet.write(start,0,'Model requirements and original observation years',title)
                for row,record in enumerate(context.requirements.to_dict('records'),start+2):
                    for col,value in enumerate(record.values()): sheet.write_string(row,col,str(value),text)
                for col,label in enumerate(context.requirements.columns): sheet.write(start+1,col,label,head)
    return output
