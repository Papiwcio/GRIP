"""One authoritative validity/eligibility process, independent of estimators.

No source writes. Approved membership is immutable until the orchestrator adopts
a reported specification/data revision. All production runners use select().
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
import hashlib
import json
import re
import numpy as np
import pandas as pd
from code_config import get_scenario_definitions, manual_exclusion_mask, MANUAL_EXCLUSIONS
from code_audit_data_quality import RATIOS, json_value

ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'data_common_sample_registry.json'
_ACTIVE = None
ALIASES = {'All':'ALL','Rank2019':'RANK2019','Rank2019_Manufacturing':'RANK2019_MANUFACTURING','ALL_MANUFACTURING':'MANUFACTURING'}


def fingerprint(company_ids):
    return hashlib.sha256('\n'.join(sorted(map(str, company_ids))).encode()).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(json_value(value), sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def policy_signature(specifications):
    return digest({'models':specifications,'manual_exclusions':MANUAL_EXCLUSIONS,
                   'populations':get_scenario_definitions(),'policy_version':2,
                   'export_bounds':[0,1],'positive_required_denominators':['sales','employment','total_assets'],
                   'other_denominators':'finite_nonzero','expected_2018':'sales_only'})


def approved_specifications():
    # Runtime imports avoid a module cycle with the shared regression engine.
    import code_ols_scenarios as e
    import code_ols_interactions as i
    import code_quantile as q
    import code_diagnostics_trajectories as d
    import code_severe_p1_decline as severe
    configs = {'OLS_PRIMARY': {**e.CONFIG,'include_interactions':False},
               'OLS_EXPORT_SIZE':i.specification_config('export_size'),
               'OLS_PROFIT_MANUFACTURING':i.specification_config('profit_manufacturing'),
               'QUANTILE':q.CONFIG, 'DIAGNOSTICS':d.build_diagnostic_config(str(ROOT / e.CONFIG['input_file'])),
               'SEVERE':{**e.CONFIG,'include_interactions':False}}
    result = []
    for family, c in configs.items():
        c = e.normalise_config(c)
        populations = ['ALL','RANK2019'] if family == 'OLS_PROFIT_MANUFACTURING' else ['RANK2019','RANK2019_MANUFACTURING'] if family == 'SEVERE' else list(get_scenario_definitions())
        for period, model in e.build_models(c).items():
            if family == 'SEVERE' and period == 'FULL': continue
            products = e.get_interaction_column_names(c)
            # Products depend on their constituents, not on a separately cleaned column.
            required = [model['dependent'], *[v for v in model['regressors'] if v not in products], *e.get_categorical_columns(c)]
            if family == 'SEVERE': required.append(f"{e.get_growth_prefix(c)}growth_P1")
            for interaction in e.get_active_interactions(c):
                required += [e.resolve_interaction_variable(c, v, e.get_regressor_period_for_model(period)) for v in interaction['variables']]
            result.append({'family':family,'period':period,'populations':populations,
                           'dependent':model['dependent'],'regressors':model['regressors'],
                           'required':sorted(set(required)),
                           'settings':{k:c.get(k) for k in ['growth_mode','centre_interaction_inputs','base_regressors','categorical_controls','include_owner','owner_variable','include_lag_growth','lag_growth_periods','interaction_metadata','winsor_lower','winsor_upper','standardise_dependent','covariance_type','model_variants']},
                           'quantiles':q.CONFIG['quantiles'] if family == 'QUANTILE' else [],
                           'estimator_settings':q.CONFIG['quantreg_settings'] if family == 'QUANTILE' else {k:severe.CONFIG[k] for k in ['thresholds','primary_threshold','logit_method','max_iter','tolerance']} if family == 'SEVERE' else {}})
    return result


def lineage(variable, core_columns):
    """Return exact annual prerequisites, or a stable descriptor prerequisite.

    Unknown transformations fail explicitly rather than guessing an alternative.
    New core/start covariates are resolved from the builder's annual schema.
    """
    growth = re.fullmatch(r'(lag_)?([nr])growth(?:_log)?(?:_ann)?_(P[123]|2019_2024)', variable)
    if growth:
        lag, mode, period = growth.groups()
        years = {'P1':[2019,2020],'P2':[2020,2022],'P3':[2022,2024],'2019_2024':[2019,2024]}[period]
        if lag: years = {'P1':[2018,2019],'P2':[2019,2020],'P3':[2020,2022]}[period]
        return [(y,'sales','positive') for y in years] + ([(y,'price_index','positive') for y in years] if mode == 'r' else [])
    start = re.fullmatch(r'(.+)_start_(P[123])', variable)
    if start:
        base, period = start.groups(); year = {'P1':2019,'P2':2020,'P3':2022}[period]
        if base in RATIOS:
            numerator, denominator = RATIOS[base]
            return [(year,numerator,'finite'), (year,denominator,'positive' if denominator in {'sales','employment','total_assets'} else 'nonzero'),(year,base,'bounded_export' if base == 'export_ratio' else 'finite')]
        if base.startswith('ln_'):
            raw = base[3:]
            if raw not in core_columns: raise ValueError(f'Unknown log lineage: {variable}')
            return [(year,raw,'positive'),(year,base,'finite')]
        if base in core_columns: return [(year,base,'finite')]
        raise ValueError(f'Missing source definition for required start covariate: {variable}')
    if variable in core_columns:
        return [(None,variable,'binary' if variable in {'owner_num','manufacturing','in_rank_2019'} else 'categorical' if variable in {'sector_en','owner','company'} else 'finite')]
    raise ValueError(f'Required variable lacks documented annual/descriptor lineage: {variable}')


@dataclass
class CommonSamples:
    source: pd.DataFrame
    core: pd.DataFrame
    specifications: list
    signature: str
    memberships: dict
    eligible: dict
    details: pd.DataFrame
    requirements: pd.DataFrame
    source_hashes: dict
    alignment: list = field(default_factory=list)

    def manifest(self):
        return dict(version=2, specification_signature=self.signature, source_hashes=self.source_hashes,
                    populations={p:{'company_ids':sorted(s),'N':len(s),'sample_sha256':fingerprint(s)} for p,s in self.memberships.items()},
                    specifications=self.specifications)


def construct(source, core, specifications=None, integrity_findings=None):
    source, core = source.copy(), core.copy()
    if source.nip.isna().any() or core[['nip','year']].isna().any().any():
        raise ValueError('Missing source company/year keys: stop before estimation; no surrogate invented.')
    source['nip'], core['nip'] = source.nip.astype(str), core.nip.astype(str)
    if source.nip.duplicated().any() or core.duplicated(['nip','year']).any() or source.nip.str.strip().eq('').any() or core.nip.str.strip().eq('').any() or source.nip.isin(['nan','<NA>']).any():
        raise ValueError('Invalid/duplicate source company keys: stop before estimation.')
    specs = specifications if specifications is not None else approved_specifications()
    annual = {int(y):f.set_index('nip').reindex(source.nip) for y,f in core.groupby('year')}
    index = source.nip.tolist()
    values = source.set_index('nip')
    details, requirements, eligible, memberships = {}, [], {}, {}
    manual = set(source.loc[manual_exclusion_mask(source),'nip'])
    scopes = {}
    for population, metadata in get_scenario_definitions().items():
        mask = pd.Series(True,index=source.index)
        if population in {'RANK2019','RANK2019_MANUFACTURING'}: mask &= source.in_rank_2019.eq(1)
        if population in {'MANUFACTURING','RANK2019_MANUFACTURING'}: mask &= source.manufacturing.eq(1)
        if metadata.get('filter') and population not in {'MANUFACTURING','RANK2019_MANUFACTURING'}:
            mask &= source.index.isin(source.query(metadata['filter'],engine='python').index)
        scopes[population] = set(source.loc[mask,'nip'])

    def emit(nip, year, variable, rule, value, kind, tag=None, population=None):
        start = re.fullmatch(r'(.+)_start_(P[123])',variable)
        if year is None and start:
            base,period=start.groups();year={'P1':2019,'P2':2020,'P3':2022}[period]
            original=annual[year].at[nip,base] if base in annual[year] else None
            # Identical annual copies share one violation. A discrepant period
            # copy keeps its own variable identity and still carries the year.
            if (pd.isna(original) and pd.isna(value)) or (pd.notna(original) and pd.notna(value) and original==value): variable=base
        key = (nip,year,variable,rule)
        if key not in details:
            details[key] = dict(nip=nip,company=values.at[nip,'company'],year=year,variable=variable,
                                observed_value=json_value(value),rule=rule,violation_type=kind,
                                specifications=set(),populations=set(),eligibility_impact=False)
        if tag: details[key]['specifications'].add(tag)
        if population: details[key]['populations'].add(population);details[key]['eligibility_impact']=True

    # Universal domain evidence is retained for all years, even if unused by a model.
    for year, frame in annual.items():
        for variable, rule in [('export_ratio','EXPORT_0_1'),('employment','EMPLOYMENT_NONNEGATIVE')]:
            if year == 2018: continue
            s = pd.to_numeric(frame[variable],errors='coerce')
            invalid = s.notna() & (~s.between(0,1) if variable == 'export_ratio' else s.lt(0))
            for nip in s.index[invalid]: emit(nip,year,variable,rule,s.at[nip],'INVALID_ANNUAL')
    for nip in manual: emit(nip,None,'company','MANUAL_FIRM_EXCLUSION',None,'FIRM_WIDE')
    cache = {}
    integrity = defaultdict(set)
    if integrity_findings is not None:
        for r in integrity_findings.loc[integrity_findings.classification.eq('INVALID')].to_dict('records'):
            year = int(r['year']) if pd.notna(r['year']) else None
            integrity[year,r['variable']].add(str(r['nip']))
    for spec in specs:
        tag = f"{spec['family']}:{spec['period']}"
        model_bad = set()
        for variable in spec['required']:
            if variable not in source: raise ValueError(f'Approved {tag} requires absent {variable}. No substitute.')
            deps = lineage(variable, set(core.columns))
            requirements.append(dict(specification=tag,variable=variable,years='; '.join(str(y) for y,_,_ in deps if y is not None),source_variables='; '.join(sorted({v for _,v,_ in deps})),populations='; '.join(spec['populations'])))
            direct_constraint='categorical' if variable == 'sector_en' else 'bounded_export' if variable.startswith('export_ratio_start_') else 'finite'
            for year, raw, constraint in deps + [(None,variable,direct_constraint)]:
                cache_key = (year,raw,constraint)
                if cache_key not in cache:
                    s = values[raw] if year is None else annual.get(year,pd.DataFrame(index=index)).get(raw,pd.Series(np.nan,index=index))
                    num = pd.to_numeric(s,errors='coerce')
                    missing = s.isna() if constraint == 'categorical' else num.isna()
                    invalid = pd.Series(False,index=s.index)
                    if constraint != 'categorical': invalid |= num.notna() & ~np.isfinite(num)
                    if constraint == 'positive': invalid |= num.le(0)
                    if constraint == 'nonzero': invalid |= num.eq(0)
                    if constraint == 'bounded_export': invalid |= num.notna() & ~num.between(0,1)
                    if constraint == 'binary': invalid |= num.notna() & ~num.isin([0,1])
                    cache[cache_key] = (s, missing, invalid)
                s, missing, invalid = cache[cache_key]
                integrity_bad = set(s.index) & integrity[year,raw]
                for nip in integrity_bad:
                    model_bad.add(nip)
                    for population in spec['populations']:
                        if nip in scopes[population]: emit(nip,year,raw,'INDEPENDENT_INTEGRITY',s.at[nip],'INVALID_ANNUAL',tag,population)
                for nip in s.index[missing | invalid]:
                    model_bad.add(nip)
                    rule = 'REQUIRED_MISSING' if missing.at[nip] else 'EXPORT_0_1' if constraint == 'bounded_export' else f'REQUIRED_{constraint.upper()}'
                    for population in spec['populations']:
                        if nip in scopes[population]: emit(nip,year,raw,rule,s.at[nip],'REQUIRED_MISSING' if missing.at[nip] else 'INVALID_ANNUAL',tag,population)
        for population in spec['populations']:
            # Retain the existing complete-trajectory requirement; it is shared with FULL.
            flag = 'has_complete_rtrajectory' if spec['settings']['growth_mode'] == 'real' else 'has_complete_ntrajectory'
            incomplete = set(source.loc[~source[flag].eq(1),'nip'])
            for nip in incomplete & scopes[population]: emit(nip,None,flag,'INCOMPLETE_TRAJECTORY',values.at[nip,flag],'REQUIRED_MISSING',tag,population)
            eligible[population,tag] = scopes[population] - manual - incomplete - model_bad
    for population in scopes:
        sets = [s for (p,_),s in eligible.items() if p == population]
        memberships[population] = set.intersection(*sets)
    memberships['MANUFACTURING'] &= memberships['ALL']
    memberships['RANK2019_MANUFACTURING'] &= memberships['RANK2019'] & memberships['MANUFACTURING']
    for population, selected in memberships.items():
        if not selected: raise ValueError(f'Common sample empty: {population}')
        for (p,_),s in eligible.items():
            if p == population: assert selected <= s
    for item in details.values():
        item['specifications'] = '; '.join(sorted(item['specifications']))
        item['populations'] = '; '.join(sorted(item['populations']))
    result = CommonSamples(source,core,specs,policy_signature(specs),memberships,eligible,pd.DataFrame(details.values()),pd.DataFrame(requirements),{})
    result.scopes, result.manual = scopes, manual
    return result


def build_context():
    from code_ols_scenarios import CONFIG
    period_path = ROOT / CONFIG['input_file']
    core_path = ROOT / 'data_core_2018-2024.parquet'
    hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [period_path,core_path]}
    source, core = pd.read_parquet(period_path),pd.read_parquet(core_path)
    from code_audit_data_quality import audit_frames
    quality, quality_audit = audit_frames(core,source,statistical=True)
    context = construct(source,core,integrity_findings=quality)
    context.quality, context.quality_audit = quality, quality_audit
    context.source_hashes = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [period_path,core_path]}
    if hashes!=context.source_hashes: raise RuntimeError('Source changed while building eligibility; no output may be published.')
    return context


def activate(context):
    global _ACTIVE
    _ACTIVE = context


def current():
    global _ACTIVE
    if _ACTIVE is not None and policy_signature(approved_specifications()) != _ACTIVE.signature:
        raise RuntimeError('Model configuration changed during the run: eligibility reassessment and explicit adoption required.')
    if _ACTIVE is None:
        candidate = build_context()
        if not STATE.exists(): raise RuntimeError('No approved common sample. Run the unified pipeline with explicit adoption.')
        previous = json.loads(STATE.read_text())
        if previous['specification_signature'] != candidate.signature or previous['source_hashes'] != candidate.source_hashes or previous['populations'] != candidate.manifest()['populations']:
            proposal = {'previous':previous,'proposed':candidate.manifest(),'requirements':candidate.requirements.to_dict('records'),'exclusions':candidate.details.to_dict('records')}
            (ROOT / 'results_sample_change_proposal.json').write_text(json.dumps(json_value(proposal),ensure_ascii=False,indent=2))
            raise RuntimeError('Specification/data changed. Proposal saved; approval/adoption required before production results.')
        if previous['populations'] != candidate.manifest()['populations']: raise RuntimeError('Approved sample reconstruction disagrees.')
        _ACTIVE = candidate
    return _ACTIVE


def select(frame, population):
    population = ALIASES.get(population,population)
    context = current()
    requested = context.memberships[population]
    observed = set(frame.nip.astype(str))
    if not requested <= observed: raise RuntimeError(f'Missing centrally selected firms in {population} input.')
    result = frame.loc[frame.nip.astype(str).isin(requested)].copy()
    result.attrs['common_population'] = population
    result.attrs['common_sample_sha256'] = fingerprint(requested)
    return result


def validate_request(config):
    """A runner override cannot bypass the approved specification inventory."""
    import code_ols_scenarios as e
    normalized=config if config['base_regressors'] and isinstance(config['base_regressors'][0],dict) else e.normalise_config(config)
    context=current()
    for period,model in e.build_models(normalized).items():
        matches=[s for s in context.specifications if s['period']==period and s['dependent']==model['dependent'] and s['regressors']==model['regressors']]
        if not matches: raise RuntimeError(f'Unapproved runner specification in {period}; audit and adopt it in authoritative configuration first.')
        if not any(all(normalized.get(k)==s['settings'].get(k) for k in ['covariance_type','winsor_lower','winsor_upper','standardise_dependent','centre_interaction_inputs']) for s in matches):
            raise RuntimeError('Unapproved statistical settings override; configuration adoption required.')
    if 'quantiles' in config:
        approved=next(s for s in context.specifications if s['family']=='QUANTILE')
        if config['quantiles']!=approved['quantiles'] or config.get('quantreg_settings')!=approved['estimator_settings']:
            raise RuntimeError('Unapproved quantile settings override; audit and adoption required.')
    path=Path(config['input_file'])
    expected=context.source_hashes.get('data_period_2018-2024.parquet')
    if expected and hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
        raise RuntimeError('Runner input differs from the approved source version; reassessment required.')


def verify(frame, company_ids, model_id, specification_id, period):
    population = frame.attrs.get('common_population')
    if population is None: return {}  # Synthetic/audit-only engine inputs have no production marker.
    context = current()
    actual = list(map(str,company_ids))
    expected = context.memberships[population]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise RuntimeError(f'Critical sample mismatch {specification_id}/{population}/{model_id}; stop output generation.')
    metadata = dict(population_id=population,sample_id=f'COMMON_{population}',specification_id=specification_id,
                    model_id=model_id,unique_companies=len(actual),sample_sha256=fingerprint(actual))
    context.alignment.append(dict(**metadata,period=period,observations=len(actual),alignment_status='PASS'))
    return metadata


def metadata_note():
    return 'One centrally approved company sample per population, shared across periods, OLS, interactions, quantiles and diagnostics. Eligibility uses all approved specifications. Exclusions and source evidence: results_data_quality_and_samples.xlsx. Source data unchanged.'
