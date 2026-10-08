"""Independent, reporting-only GRIP audit. Never builds data or fits models."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# Import definitions only. Neither builder's __main__ block is executed.
import code_build_core as core_def
import code_build_period as period_def
from code_config import MANUAL_EXCLUSIONS, MANUAL_EXCLUSION_REASONS

VERSION = "1.0.0"
ATOL, RTOL = 1e-10, 1e-8
MIN_REFERENCE_N, MAD_LIMIT = 30, 5.0
ROOT = Path(__file__).resolve().parent
REVIEW_OUTCOMES = {
    "Retain", "Correction proposed", "Variable unusable", "Period comparability issue",
    "Firm exclusion proposed", "Unresolved",
}
RATIOS = {
    "profit_margin": ("net_profit", "sales"),
    "operating_margin": ("operating_result", "sales"),
    "export_ratio": ("exports", "sales"),
    "asset_turnover": ("sales", "total_assets"),
    "capital_ratio": ("equity", "total_assets"),
    "equity_multiplier": ("total_assets", "equity"),
    "roa": ("net_profit", "total_assets"),
    "roe": ("net_profit", "equity"),
    "depreciation_ratio": ("depreciation", "total_assets"),
    "wage_intensity": ("wages_total", "sales"),
    "sales_per_employee": ("sales", "employment"),
    "assets_per_employee": ("total_assets", "employment"),
}
SCREEN_LIMITS = {
    "profit_margin": 1, "operating_margin": 1, "roa": 1, "roe": 5,
    "wage_intensity": 1, "depreciation_ratio": 1, "asset_turnover": 20,
    "equity_multiplier": 100,
}
SOURCE_VALUES = ["sales", "exports", "net_profit", "operating_result", "employment",
                 "total_assets", "equity", "fixed_assets", "current_assets", "wages_total"]
FINDING_COLUMNS = [
    "company", "nip", "year", "period", "classification", "severity", "rule_id", "variable",
    "original_value", "calculated_value", "numerator", "denominator", "ratio_percent", "source_year",
    *SOURCE_VALUES, "reason", "affected_variables", "lineage", "review_status",
    "existing_manual_exclusion", "manual_reason", "sector", "dataset", "finding_id",
    "row_reference", "related_values",
]
DECISION_COLUMNS = ["finding_id", "dataset", "nip", "company", "year", "period", "variable",
                    "rule_id", "decision", "reason_code", "evidence_reference", "reviewer",
                    "reviewed_at", "action_scope", "approval_status"]
DEFERRED = {
    "D03": "Original numeric tokens and parsing ambiguities require a verified tracker mapping; canonical type/coercion checks are D01/D04.",
    "D12": "Existing Excel files contain researcher edits; this run audits authoritative Parquet and preserves all workbooks.",
    "A02C": "Confirmed nonnegative balance-sheet field definitions/sign conventions are not supplied.",
    "A09": "Complete equity-balancing liabilities definition and year-specific dictionary are unverified.",
    "A10": "Long-term and short-term liability source components are unavailable.",
    "A11": "Income tax source component is unavailable; no implied tax substituted.",
    "E04": "Per-value currency, scale, fiscal duration and reporting boundary are not verified.",
    "E05": "A documented same-scope nonnegative export-sales subset relationship is not established.",
    "E07": "Static S/J and GK names cannot establish annual/variable reporting boundaries or restructuring events.",
}


def json_value(value):
    """JSON-safe values; preserve unbounded representations as explicit text."""
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [json_value(v) for v in value]
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return None
        return float(value) if np.isfinite(value) else str(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    return value


def numeric(frame, column):
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").astype(float)


def finite(series):
    return series.notna() & np.isfinite(series)


def divide(a, b):
    valid = finite(a) & finite(b) & b.ne(0)
    with np.errstate(all="ignore"):
        return (a / b).where(valid)


def log_positive(a):
    with np.errstate(all="ignore"):
        return np.log(a.where(finite(a) & a.gt(0)))


def growth(a, b, duration=1):
    valid = finite(a) & finite(b) & a.gt(0) & b.gt(0)
    with np.errstate(all="ignore"):
        simple = (b / a - 1).where(valid)
        logs = (np.log(b.where(valid)) - np.log(a.where(valid))).where(valid)
    return simple, logs, logs / duration


def mismatch(stored, expected):
    if pd.api.types.is_numeric_dtype(expected):
        s = pd.to_numeric(stored, errors="coerce").astype(float)
        e = pd.to_numeric(expected, errors="coerce").astype(float)
        bad = s.isna().ne(e.isna())
        both = s.notna() & e.notna()
        bad |= both & (~np.isfinite(s) | ~np.isfinite(e) | (s - e).abs().gt(ATOL + RTOL * e.abs()))
        return bad.fillna(False)
    s, e = stored.astype("string"), expected.astype("string")
    return (s.isna().ne(e.isna()) | (s.notna() & e.notna() & s.ne(e))).fillna(False)


def expected_unavailable(column, years):
    mask = years.eq(2018) & (column != "sales")
    if column in {"income_tax", "zobowiazania_dlugoterminowe", "zobowiazania_krotkoterminow"}:
        mask |= True
    if column == "operating_result":
        mask |= years.eq(2023)
    if column == "total_liabilities":
        mask |= ~years.eq(2019)
    if column == "liabilities_provisions":
        mask |= ~years.between(2020, 2023)
    if column == "zobowiazania_i_rezerwy_na_zobowiazania":
        mask |= ~years.eq(2024)
    return mask.fillna(False)


def period_columns():
    columns = [*period_def.BLOCK_1_COLUMNS, *period_def.BLOCK_2_COLUMNS]
    for prefix in ["REAL", "NOMINAL"]:
        for suffix in ["GROWTH_COLUMNS", "GROWTH_LOG_COLUMNS", "GROWTH_LOG_ANN_COLUMNS", "LAG_COLUMNS"]:
            columns += getattr(period_def, f"{prefix}_{suffix}")
    columns += [f"{base}_start_{p}" for p in period_def.PERIODS for base in period_def.START_COVARIATE_BASE_COLUMNS]
    columns += [*period_def.REAL_AVAILABILITY_COLUMNS, *period_def.NOMINAL_AVAILABILITY_COLUMNS,
                *period_def.REAL_TRAJECTORY_COLUMNS, *period_def.NOMINAL_TRAJECTORY_COLUMNS,
                *period_def.SGROWTH_COLUMNS, *period_def.PERFORMANCE_COLUMNS]
    return columns


def rule_register():
    """Read the preserved, versioned design register; annotate execution separately."""
    rows = []
    for line in (ROOT / "documentation_data_quality_audit_specification.md").read_text().splitlines():
        if not re.match(r"^\| (D|A|E|T|P)\d\dC? \|", line):
            continue
        _, rid, formula, meaning, location, _ = line.split("|")
        parts = meaning.strip().split(";")
        rows.append({"rule_id": rid.strip(), "formula_threshold_justification": formula.strip(),
                     "classification": parts[0].strip(), "rule_type": parts[1].strip(),
                     "severity_definition": ";".join(parts[2:]).strip(), "location": location.strip(),
                     "implementation": "DEFERRED" if rid.strip() in DEFERRED else "IMPLEMENTED",
                     "limitation": DEFERRED.get(rid.strip(), ""), "findings": 0,
                     "evaluated_contexts": 0, "not_evaluable_contexts": 0})
    if len(rows) != 49 or len({r["rule_id"] for r in rows}) != 49:
        raise ValueError("Design rule register must have 49 unique IDs; review design/version change.")
    return rows


def affected_variables(variable, year):
    """Potential dependencies, not claims that downstream values are invalid."""
    if year is None:
        return []
    result = []
    bases = [name for name, pair in RATIOS.items() if variable in pair]
    if variable in {"sales", "total_assets", "employment"}:
        bases += [{"sales": "ln_sales", "total_assets": "ln_total_assets", "employment": "ln_employment"}[variable]]
    if variable in RATIOS or variable in period_def.START_COVARIATE_BASE_COLUMNS:
        bases += [variable]
    for p, d in period_def.PERIODS.items():
        if year == d["start"]:
            result += [f"{b}_start_{p}" for b in bases]
            if p == "P1" and any(b in {"export_ratio", "ln_sales", "profit_margin", "asset_turnover", "capital_ratio", "sales_per_employee"} for b in bases):
                result += ["FULL starting covariates (same 2019 inputs)"]
        if variable in {"sales", "sales_real", "price_index"} and year in {d["start"], d["end"]}:
            result += [f"{f}growth{form}_{p}" for f in ["r", "n"] for form in ["", "_log", "_log_ann"]]
            result += [f"{f}trajectory / indices" for f in ["r", "n"]]
            if p in {"P1", "P2"}:
                lag_p = "P2" if p == "P1" else "P3"
                result += [f"lag_{f}growth{form}_{lag_p}" for f in ["r", "n"] for form in ["", "_log", "_log_ann"]]
    if variable in {"sales", "sales_real", "price_index"}:
        if year in {2018, 2019}:
            result += [f"lag_{f}growth{form}_P1" for f in ["r", "n"] for form in ["", "_log", "_log_ann"]]
        if year in {2019, 2024}:
            result += ["FULL real/nominal CAGR and annualised log growth", "SGrowth_NR", "RPerf_Q_*", "NPerf_Q_*"]
        result += ["Core adjacent-year growth at this and following year"]
    return sorted(set(result))


class Audit:
    def __init__(self):
        self.rows = []
        self.coverage = []
        self.references = []
        self.contexts = []
        self.inventory = []
        self.thresholds = []
        self.core_context = {}
        self.invalid_screen_values = set()
        self.rules = rule_register()
        self.rule_by_id = {r["rule_id"]: r for r in self.rules}

    def note(self, rule, status, dataset, context, reason="", n=None):
        self.contexts.append({"rule_id": rule, "status": status, "dataset": dataset,
                              "context": context, "reason": reason, "eligible_n": n})

    def emit(self, rule, frame, mask, variable, expected=None, classification=None,
             severity="moderate", reason="", dataset="Core", period="", related=None):
        mask = pd.Series(mask, index=frame.index).fillna(False).astype(bool)
        self.note(rule, "FLAG" if mask.any() else "PASS", dataset, f"{variable}:{period}", n=int(mask.sum()))
        cls = classification or self.rule_by_id[rule]["classification"]
        for idx in frame.index[mask]:
            row = frame.loc[idx]
            nip = str(row.get("nip", "")) if pd.notna(row.get("nip")) else ""
            company = str(row.get("company", "")) if pd.notna(row.get("company")) else ""
            year_value = row.get("year")
            year = int(year_value) if year_value is not None and pd.notna(year_value) and str(year_value).replace(".0", "").isdigit() else None
            values = {}
            for key, value in (related or {}).items():
                values[key] = json_value(value.loc[idx] if isinstance(value, pd.Series) else value)
            original = row.get(variable)
            calc = expected.loc[idx] if isinstance(expected, pd.Series) else expected
            identity = [dataset, nip, year_value, period, variable, rule]
            # Row ordinal is necessary only for structural duplicate/missing-key findings.
            row_ref = str(idx) if rule == "D02" or not nip else ""
            fid = hashlib.sha256(json.dumps(json_value([identity, row_ref]), sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]
            manual = [e for e in MANUAL_EXCLUSIONS["companies"] if nip == e["nip"] or company.strip() == e["company"]]
            source_year = year if dataset != "Period" else period_def.PERIODS.get(period, {"start": 2019})["start"]
            original_context = self.core_context.get((nip, source_year), {}) if dataset == "Period" else row
            item = {"company": company, "nip": nip, "year": year, "period": period,
                    "classification": cls, "severity": severity, "rule_id": rule,
                    "variable": variable, "original_value": json_value(original),
                    "calculated_value": json_value(calc), "reason": reason,
                    "affected_variables": "; ".join(affected_variables(variable, year)),
                    "lineage": "Canonical Core firm-year" if dataset == "Core" else "Canonical Period; Core endpoint/start-year lineage" if dataset == "Period" else "Optional panel input; no upstream mutation",
                    "review_status": "Unresolved", "sector": json_value(row.get("sector_en", row.get("sector"))),
                    "dataset": dataset, "finding_id": fid, "row_reference": row_ref,
                    "source_year": source_year,
                    "existing_manual_exclusion": bool(manual),
                    "manual_reason": "; ".join(e["reason_code"] for e in manual),
                    "related_values": json.dumps(values, ensure_ascii=False, sort_keys=True),
                    "numerator": values.get("numerator"), "denominator": values.get("denominator"),
                    "ratio_percent": values.get("ratio_percent")}
            item.update({key: json_value(original_context.get(key)) for key in SOURCE_VALUES})
            self.rows.append(item)

    def verify(self, frame, column, expected, rule="A05", dataset="Core", period="", related=None):
        if column not in frame:
            self.note(rule, "NOT_EVALUABLE", dataset, column, "Required column absent; D01 reports schema failure.")
            return
        self.emit(rule, frame, mismatch(frame[column], expected), column, expected,
                  classification="INVALID", severity="high", dataset=dataset, period=period,
                  reason=f"Stored value/missingness differs from independent formula; atol={ATOL}, rtol={RTOL}.", related=related)


def validate_structure(audit, frame, dataset, expected_columns, keys):
    for column in set(expected_columns).difference(frame.columns):
        dummy = pd.DataFrame([{"nip": "", "company": "Dataset schema"}])
        audit.emit("D01", dummy, [True], column, severity="critical", dataset=dataset,
                   reason="Required canonical column absent.")
    if frame.columns.duplicated().any():
        raise ValueError(f"{dataset}: duplicate column labels prevent unambiguous audit; no input changed.")
    extra = set(frame.columns).difference(expected_columns)
    if extra:
        dummy = pd.DataFrame([{"nip": "", "company": "Dataset schema"}])
        audit.emit("D01", dummy, [True], ";".join(sorted(extra)), severity="critical", dataset=dataset,
                   reason="Unexpected canonical columns.")
    for key in keys:
        if key in frame:
            bad = frame[key].isna() | frame[key].astype("string").str.strip().isin(["", "nan", "None", "<NA>"])
            audit.emit("D02", frame, bad, key, severity="critical", dataset=dataset, reason="Missing or unusable canonical key.")
    if all(k in frame for k in keys):
        audit.emit("D02", frame, frame.duplicated(keys, keep=False), "+".join(keys), severity="critical",
                   dataset=dataset, reason="Duplicate canonical observation grain; all competing rows reported.")
    if "year" in frame:
        years = numeric(frame, "year")
        audit.emit("D01", frame, ~years.isin(range(2018, 2025)) | years.mod(1).ne(0), "year",
                   severity="critical", dataset=dataset, reason="Year missing, nonintegral or outside 2018–2024.")
        missing_years = set(range(2018, 2025)).difference(years.dropna())
        if missing_years:
            audit.note("D01", "NOT_EVALUABLE", dataset, "year coverage", f"Global years absent: {sorted(missing_years)}; fixtures need not cover all years.")
    # Numerical strings are representations requiring review; true unparseable values violate the numeric schema.
    categorical = set(core_def.STRING_COLUMNS) | {"owner", "company", "nip", "sector", "sector_en", "legal_form", "city", "pkd_description", "SGrowth_NR"} | set(period_def.PERFORMANCE_COLUMNS)
    categorical |= {x for x in expected_columns if "trajectory" in x and not x.startswith("has_complete") or x.endswith("_sign")}
    for column in set(expected_columns).intersection(frame.columns).difference(categorical):
        values = numeric(frame, column)
        bad = frame[column].notna() & values.isna()
        audit.emit("D01", frame, bad, column, severity="high", dataset=dataset, reason="Nonmissing value cannot be represented as a canonical number.")
        audit.emit("D04", frame, values.notna() & ~np.isfinite(values), column, severity="high", dataset=dataset, reason="Infinite numerical value is not a usable finite observation.")
        if not pd.api.types.is_numeric_dtype(frame[column]):
            audit.emit("D01", frame, frame[column].notna() & values.notna(), column, severity="high", dataset=dataset,
                       reason="Canonical numerical column stored with nonnumeric dtype; original token preserved.")


def audit_core(audit, core):
    years = numeric(core, "year")
    financial = years.between(2019, 2024)
    for column in core_def.ANNUAL_COLUMNS:
        vals = numeric(core, column)
        unavailable = expected_unavailable(column, years)
        for year, indexes in core.groupby("year", dropna=False).groups.items():
            part = vals.loc[indexes]
            audit.coverage.append({"dataset": "Core", "variable": column, "year": json_value(year),
                                   "rows": len(part), "nonmissing": int(part.notna().sum()),
                                   "missing": int(part.isna().sum()), "zero": int(part.eq(0).sum()),
                                   "negative": int(part.lt(0).sum()), "nonfinite": int((part.notna() & ~np.isfinite(part)).sum()),
                                   "status": "EXPECTED_UNAVAILABLE" if unavailable.loc[indexes].all() and part.isna().all() else "OBSERVED_BLOCK"})
        audit.emit("D05", core, vals.isna() & ~unavailable, column, reason="Missing source observation in an otherwise expected annual field; never replaced with zero.")
        if column in {"employment"}:
            rule, cls = "A01", "INVALID"
        elif column in {"sales", "exports", "wages_total", "depreciation"}:
            rule, cls = ("E02" if column == "exports" else "A03"), "SUSPICIOUS"
        elif column in {"total_assets", "fixed_assets", "current_assets", "total_liabilities", "liabilities_provisions", "zobowiazania_i_rezerwy_na_zobowiazania"}:
            rule, cls = "A02", "SUSPICIOUS"
        else:
            continue
        context = {"numerator": vals, "denominator": numeric(core, "sales"), "ratio_percent": 100 * divide(vals, numeric(core, "sales"))} if column == "exports" else None
        domain_years = financial | years.eq(2018) if column == "sales" else financial
        audit.emit(rule, core, domain_years & vals.lt(0), column, classification=cls, severity="high" if column in {"sales", "exports", "employment", "total_assets"} else "moderate", reason="Negative annual value; check documented domain or unresolved sign/scope convention.", related=context)
    sales, exports = numeric(core, "sales"), numeric(core, "exports")
    audit.emit("A04", core, sales.eq(0), "sales", severity="high", reason="Zero sales is distinct from missing sales; logarithms and positive-endpoint growth are unavailable.", related={"positive_exports": exports.gt(0)})
    for column in ["total_assets", "employment"]:
        values = numeric(core, column)
        activity = sales.ne(0) & sales.notna() | numeric(core, "net_profit").ne(0) & numeric(core, "net_profit").notna()
        audit.emit("A04", core, financial & values.eq(0) & activity, column, reason="Zero denominator despite observed economic activity; raw zero may be valid.")
    expected = years.map(core_def.PRICE_INDEX_BY_YEAR).astype(float)
    audit.verify(core, "price_index", expected, "T06")
    audit.verify(core, "sales_real", divide(sales, expected), "T06", related={"numerator": sales, "denominator": expected})
    for raw in ["sales", "total_assets", "employment"]:
        audit.verify(core, f"ln_{raw}", log_positive(numeric(core, raw)), "A06", related={"input": numeric(core, raw)})
        audit.verify(core, {"sales": "has_sales", "total_assets": "has_assets", "employment": "has_employment"}[raw], numeric(core, raw).gt(0).astype(float), "D09")
    for ratio, (num, den) in RATIOS.items():
        numerator, denominator = numeric(core, num), numeric(core, den)
        audit.verify(core, ratio, divide(numerator, denominator), related={"numerator": numerator, "denominator": denominator})
    ratio = divide(exports, sales)
    audit.emit("E02", core, financial & exports.ge(0) & ratio.lt(0), "export_ratio", ratio, severity="high",
               reason="Negative export intensity caused by a negative sales denominator, despite nonnegative exports; investigate revenue/sign/scope.",
               related={"numerator": exports, "denominator": sales, "ratio_percent": 100 * ratio})
    audit.emit("E01", core, financial & sales.gt(0) & ratio.gt(1.01), "export_ratio", ratio,
               severity="high", reason="Export/sales exceeds 101%; same-scope subset interpretation is unverified, so retain for source review.",
               related={"numerator": exports, "denominator": sales, "ratio_percent": 100 * ratio, "excess_exports": exports - sales})
    audit.emit("E01", core, financial & sales.gt(0) & ratio.gt(1) & ratio.le(1.01), "export_ratio", ratio,
               severity="low", reason="Export/sales exceeds 100% by at most one percentage point; source rounding/scope unresolved.",
               related={"numerator": exports, "denominator": sales, "ratio_percent": 100 * ratio, "excess_exports": exports - sales})
    employment, wages = numeric(core, "employment"), numeric(core, "wages_total")
    audit.emit("A13", core, financial & ((employment.eq(0) & wages.gt(0)) | (employment.gt(0) & wages.eq(0))), "employment", reason="Employment/wages activity disagreement may reflect contractors, timing or scope.")
    assets, equity = numeric(core, "total_assets"), numeric(core, "equity")
    fixed, current = numeric(core, "fixed_assets"), numeric(core, "current_assets")
    scale = pd.concat([assets.abs(), fixed.abs(), current.abs()], axis=1).max(axis=1)
    residual = fixed + current - assets
    tolerance = 1e-6 * scale
    testable = finite(assets) & finite(fixed) & finite(current) & financial
    for relation, mask in [("asset_partition", residual.abs().gt(tolerance)), ("asset_components", fixed.gt(assets + tolerance) | current.gt(assets + tolerance))]:
        difference = residual.abs() if relation == "asset_partition" else pd.concat([fixed - assets, current - assets], axis=1).max(axis=1)
        material = difference.gt(.01 * scale)
        for severity, priority in [("high", material), ("moderate", ~material)]:
            audit.emit("A08", core, testable & mask & priority, relation, residual, severity=severity, reason="Asset comparison exceeds review-only relative tolerance; units, precision and exhaustive partition are unverified. Residual above 1% of scale gets high priority.", related={"residual": residual, "scale": scale, "tolerance": tolerance})
    audit.note("A08", "NOT_EVALUABLE", "Core", "asset comparisons", "Missing components; no zero substituted.", int((financial & ~testable).sum()))
    for column, limit in SCREEN_LIMITS.items():
        values = numeric(core, column)
        signed = column in {"profit_margin", "operating_margin", "roa", "roe", "equity_multiplier"}
        mask = values.abs().gt(limit) if signed else values.gt(limit)
        audit.emit("A14", core, financial & mask, column, classification="EXTREME", reason=f"Exploratory magnitude screen exceeds {limit}; not a feasibility constraint.", related={"threshold": limit})
        if column in RATIOS:
            den = numeric(core, RATIOS[column][1])
            for year in range(2019, 2025):
                eligible = den.where(financial & years.eq(year) & finite(den) & den.gt(0)).dropna()
                if len(eligible) >= MIN_REFERENCE_N:
                    p1 = eligible.quantile(.01)
                    audit.emit("A07", core, years.eq(year) & den.gt(0) & den.lt(p1) & mask, column, reason="Extreme ratio combined with positive denominator below its pooled-year p1; investigate small-denominator mechanism.", related={"denominator": den, "denominator_p1": p1, "reference_n": len(eligible)})
    cap = divide(equity, assets)
    audit.emit("A15", core, financial & cap.gt(1), "capital_ratio", cap, reason="Equity/assets above one needs conditional liabilities/sign/scope review; negative equity is not a hard failure.")
    near_equity = assets.gt(0) & divide(equity.abs(), assets).lt(.01)
    audit.emit("A07", core, financial & near_equity & (numeric(core, "roe").abs().gt(1) | numeric(core, "equity_multiplier").abs().gt(100)), "equity", reason="Small absolute equity relative to assets amplifies an unusually large equity-denominated ratio.")
    identities = {
        "roa": numeric(core, "profit_margin") * numeric(core, "asset_turnover"),
        "roe": numeric(core, "roa") * numeric(core, "equity_multiplier"),
        "capital_ratio": divide(pd.Series(1., index=core.index), numeric(core, "equity_multiplier")),
        "asset_turnover": divide(numeric(core, "sales_per_employee"), numeric(core, "assets_per_employee")),
    }
    for col, value in identities.items():
        eligible = finite(value) & finite(numeric(core, col))
        audit.emit("A12", core, eligible & mismatch(core.get(col, pd.Series(np.nan, index=core.index)), value), col, value, classification="INVALID", severity="high", reason="Defined algebraic ratio identity does not reconcile.")
    # Descriptor mapping is verified without imposing extra 2018 source requirements.
    ownership = numeric(core, "owner_type")
    owner = np.where(ownership.where(finite(ownership)).round().astype("Int64").astype("string").str.startswith("5", na=False), "Foreign", "Domestic")
    audit.verify(core, "owner", pd.Series(owner, index=core.index, dtype="string"), "D09")
    audit.verify(core, "owner_num", pd.Series(owner == "Foreign", index=core.index).astype(float), "D09")
    audit.verify(core, "in_rank_2019", numeric(core, "rank_2019").notna().astype(float), "D09")
    pkd = numeric(core, "pkd")
    prefix = pkd.where(finite(pkd)).round().astype("Int64").astype("string").str.zfill(4).str[:2]
    manu = pd.to_numeric(prefix, errors="coerce").between(10, 33).astype(float)
    manu = manu.where(prefix.notna())
    audit.verify(core, "manufacturing", manu, "D09")
    if "sector" in core:
        mapped = core.sector.astype("string").str.strip().str.lower().map(core_def.SECTOR_EN_MAP).astype("string")
        audit.verify(core, "sector_en", mapped, "D09")
    audit.emit("D08", core, financial & numeric(core, "owner_type").isna(), "owner_type", reason="Missing source ownership is mapped to Domestic by the existing builder; no 2018 ownership requirement.")
    audit.emit("D07", core, financial & ~core.nip.astype("string").str.fullmatch(r"\d{10}", na=False), "nip", severity="high", reason="Firm key is not a ten-digit NIP; may be an approved surrogate. No padding, merging or removal.")
    for column in ["company", "regon", "krs", "legal_form", "owner_type", "pkd", "sj"]:
        if column in core:
            counts = core.loc[financial].groupby("nip")[column].nunique(dropna=True)
            conflicting = set(counts[counts.gt(1)].index)
            audit.emit("D08", core, financial & core.nip.isin(conflicting), column, reason="Descriptor has multiple observed annual values; first-nonmissing selection does not establish historical stability.")
    for column in ["regon", "krs"]:
        if column in core:
            valid = core.loc[financial & numeric(core, column).gt(0)]
            counts = valid.groupby(column).nip.nunique()
            audit.emit("D07", core, financial & core[column].isin(counts[counts.gt(1)].index), column, severity="high", reason="One registration identifier is shared by several canonical firm keys; boundaries/identity require review.")


def audit_time_series(audit, core):
    ordered = core.sort_values(["nip", "year"], kind="mergesort").copy()
    years = numeric(ordered, "year")
    prev_year = years.groupby(ordered.nip).shift()
    consecutive = years.sub(prev_year).eq(1)
    unique = ~ordered.duplicated(["nip", "year"], keep=False)
    consecutive &= unique & unique.groupby(ordered.nip).shift().eq(True)
    for col, raw, logs in [("sales_growth_yoy", "sales", False), ("sales_real_growth_yoy", "sales_real", False), ("sales_log_growth_yoy", "sales", True)]:
        now = numeric(ordered, raw)
        prev = now.groupby(ordered.nip).shift()
        calculated = (log_positive(now) - log_positive(prev)) if logs else divide(now, prev).sub(1).where(prev.gt(0))
        calculated = calculated.where(consecutive)
        audit.verify(ordered, col, calculated, "T07", related={"previous_year": prev_year, "previous_value": prev, "current_value": now, "calendar_gap": years - prev_year})
    for col in ["sales", "sales_real", "total_assets", "exports", "employment", "wages_total", "profit_before_tax", "net_profit", "equity"]:
        now = numeric(ordered, col)
        prev = now.groupby(ordered.nip).shift()
        nxt = now.groupby(ordered.nip).shift(-1)
        factors = divide(now, prev)
        related = {"previous_year": prev_year, "previous_value": prev, "current_value": now, "next_value": nxt, "change_factor": factors}
        positive = consecutive & now.gt(0) & prev.gt(0) & finite(now) & finite(prev)
        if col not in {"profit_before_tax", "net_profit", "equity"}:
            audit.emit("T01", ordered, positive & (factors.gt(5) | factors.lt(.2)), col, classification="EXTREME", reason="Consecutive positive levels change by more than fivefold; valid expansion/distress is possible.", related=related)
        else:
            scale = pd.concat([now.abs(), prev.abs()], axis=1).max(axis=1)
            movement = divide((now - prev).abs(), scale)
            audit.emit("T02", ordered, consecutive & movement.gt(1.5), col, classification="EXTREME", reason="Large movement/sign reversal in a signed quantity; percentage growth near zero is not used.", related={**related, "scaled_signed_movement": movement})
        unit = pd.Series(False, index=ordered.index)
        for factor in [100., 1000., 1e6, .01, .001, 1e-6]:
            unit |= factors.between(.95 * factor, 1.05 * factor)
        audit.emit("T03", ordered, positive & unit, col, severity="high", reason="Change factor is within 5% of a power-of-ten unit scale; hypothesis only, no rescaling.", related=related)
        next_year = years.groupby(ordered.nip).shift(-1)
        both = positive & next_year.sub(years).eq(1) & nxt.gt(0)
        spike = divide(now, prev).gt(10) & divide(now, nxt).gt(10)
        trough = divide(prev, now).gt(10) & divide(nxt, now).gt(10)
        audit.emit("T04", ordered, both & (spike | trough), col, reason="Temporary greater-than-tenfold spike/trough; source/comparability question.", related=related)
    export_ratio = divide(numeric(ordered, "exports"), numeric(ordered, "sales"))
    prev_ratio = export_ratio.groupby(ordered.nip).shift()
    export_factor = divide(numeric(ordered, "exports"), numeric(ordered, "exports").groupby(ordered.nip).shift())
    sales_factor = divide(numeric(ordered, "sales"), numeric(ordered, "sales").groupby(ordered.nip).shift())
    discontinuity = (export_ratio - prev_ratio).abs().gt(.5) | ((export_factor.gt(10) | export_factor.lt(.1)) & export_factor.gt(0) & sales_factor.between(.5, 2))
    audit.emit("E03", ordered, consecutive & years.ge(2019) & discontinuity, "export_ratio", export_ratio, severity="high", reason="Export intensity moves by more than 50 percentage points or exports change tenfold without a comparable sales change.", related={"previous_ratio": prev_ratio, "ratio_percent": 100 * export_ratio, "export_factor": export_factor, "sales_factor": sales_factor})


def audit_period(audit, core, period):
    if core.duplicated(["nip", "year"]).any() or period.duplicated("nip").any():
        audit.note("P01", "NOT_EVALUABLE", "Period", "endpoint joins", "Duplicate canonical keys make lineage ambiguous; D02 records every candidate.")
        return
    # Use a numeric comparison key inside the audit only; original representations
    # are still reported by D01 and never written back to the caller or files.
    c = core.assign(_audit_year=numeric(core, "year")).set_index(["nip", "_audit_year"])
    if c.index.duplicated().any():
        audit.note("P01", "NOT_EVALUABLE", "Period", "numeric year keys", "Canonical year representations collapse to competing numeric keys; no arbitrary candidate selected.")
        return
    def source(column, year):
        if column not in core or year not in set(numeric(core, "year").dropna()):
            return pd.Series(np.nan, index=period.index)
        values = c.xs(year, level="_audit_year")[column]
        return period.nip.map(values)
    expected = {}
    for col in period_def.STABLE_DESCRIPTOR_CANDIDATES:
        if col not in core:
            continue
        stable = core.loc[numeric(core, "year").ge(2019)].sort_values(["nip", "year"], kind="mergesort").groupby("nip")[col].first()
        value = period.nip.map(stable)
        audit.verify(period, col, value, "D09", "Period")
    core_firms, period_firms = set(core.nip.dropna()), set(period.nip.dropna())
    audit.emit("D07", period, ~period.nip.isin(core_firms), "nip", severity="high", dataset="Period", reason="Period firm has no matching canonical Core firm.")
    lost = core.loc[~core.nip.isin(period_firms)].drop_duplicates("nip")
    audit.emit("D07", lost, pd.Series(True, index=lost.index), "nip", severity="high", reason="Core firm has no matching Period observation.")
    for family, raw in [("r", "sales_real"), ("n", "sales")]:
        for p, spec in period_def.PERIODS.items():
            start = pd.to_numeric(source(raw, spec["start"]), errors="coerce")
            end = pd.to_numeric(source(raw, spec["end"]), errors="coerce")
            forms = growth(start, end, spec["years"])
            details = {"start_year": spec["start"], "end_year": spec["end"], "duration": spec["years"], "start_sales": start, "end_sales": end, "sales_basis": raw}
            for suffix, values in zip(["", "_log", "_log_ann"], forms):
                column = f"{family}growth{suffix}_{p}"
                expected[column] = values
                audit.verify(period, column, values, "P01", "Period", p, details)
            available = finite(start) & finite(end) & start.gt(0) & end.gt(0)
            audit.verify(period, f"has_{family}{p}_data", available.astype(float), "P03", "Period", p, details)
            # Real and nominal blocks share missing raw sales; one source-lineage finding per nominal period.
            if family == "n":
                missing = start.isna() | end.isna()
                audit.emit("D06", period, missing, f"ngrowth_log_ann_{p}", severity="moderate", dataset="Period", period=p,
                           reason="Endpoint source sales is missing; calculation remains unavailable.", related=details)
            audit.emit("P06", period, forms[2].abs().gt(np.log(5)), f"{family}growth_log_ann_{p}", classification="EXTREME", dataset="Period", period=p,
                       reason="Annualised log growth implies a greater-than-fivefold annual factor or its reciprocal.", related=details)
        for p, lag_years in [("P1", (2018, 2019, 1)), ("P2", (2019, 2020, 1)), ("P3", (2020, 2022, 2))]:
            start = pd.to_numeric(source(raw, lag_years[0]), errors="coerce")
            end = pd.to_numeric(source(raw, lag_years[1]), errors="coerce")
            for suffix, values in zip(["", "_log", "_log_ann"], growth(start, end, lag_years[2])):
                audit.verify(period, f"lag_{family}growth{suffix}_{p}", values, "P02", "Period", p,
                             {"lag_start_year": lag_years[0], "lag_end_year": lag_years[1], "lag_duration": lag_years[2], "start_sales": start, "end_sales": end})
        complete = pd.concat([expected[f"{family}growth_{p}"] for p in period_def.PERIODS], axis=1).notna().all(axis=1)
        audit.verify(period, f"has_complete_{family}trajectory", complete.astype(float), "P03", "Period")
        signs = []
        for p in period_def.PERIODS:
            vals = expected[f"{family}growth_{p}"]
            sign = pd.Series(np.where(vals.lt(0), "D", "G"), index=period.index, dtype="string").where(vals.notna())
            signs.append(sign)
            audit.verify(period, f"{family}{p}_sign", sign, "P03", "Period", p)
        pattern = (signs[0] + "-" + signs[1] + "-" + signs[2]).where(complete)
        audit.verify(period, f"{family}trajectory_3step", pattern, "P03", "Period")
        audit.verify(period, f"{family}trajectory_label", pattern.map(period_def.TRAJECTORY_LABEL_MAP).astype("string"), "P03", "Period")
        audit.verify(period, f"{family}trajectory_group", pattern.map(period_def.TRAJECTORY_GROUP_MAP).astype("string"), "P03", "Period")
        base = pd.to_numeric(source(raw, 2019), errors="coerce")
        for year in [2019, 2020, 2022, 2024]:
            value = (100 * divide(pd.to_numeric(source(raw, year), errors="coerce"), base)).where(complete)
            audit.verify(period, f"{family}index_{year}", value, "P03", "Period")
        end = pd.to_numeric(source(raw, 2024), errors="coerce")
        _, _, full_log = growth(base, end, 5)
        with np.errstate(all="ignore"):
            cagr = np.expm1(full_log)
        expected[f"{family}_cagr"], expected[f"{family}_full_log"] = cagr, full_log
        audit.verify(period, f"{family}growth_ann_2019_2024", cagr, "P01", "Period", "FULL", {"start_sales": base, "end_sales": end, "duration": 5})
        audit.verify(period, f"{family}growth_log_ann_2019_2024", full_log, "P01", "Period", "FULL", {"start_sales": base, "end_sales": end, "duration": 5})
        weighted = sum(period_def.PERIODS[p]["years"] * numeric(period, f"{family}growth_log_ann_{p}") for p in period_def.PERIODS) / 5
        comparable = complete & finite(full_log) & finite(weighted)
        audit.emit("P07", period, comparable & mismatch(weighted, full_log), f"{family}growth_log_ann_2019_2024", weighted,
                   classification="INVALID", severity="high", dataset="Period", period="FULL", reason="Length-weighted period log growth does not match full-horizon endpoint growth.")
    for p, spec in period_def.PERIODS.items():
        for base in period_def.START_COVARIATE_BASE_COLUMNS:
            audit.verify(period, f"{base}_start_{p}", pd.to_numeric(source(base, spec["start"]), errors="coerce"), "P02", "Period", p,
                         {"source_year": spec["start"], "source_column": base})
    real, nominal = expected["r_cagr"], expected["n_cagr"]
    valid = real.notna() & nominal.notna()
    category = pd.Series(pd.NA, index=period.index, dtype="string")
    for label, mask in [("R2", real.ge(0) & nominal.gt(.2)), ("R1", real.ge(0) & nominal.le(.2)), ("N1", real.lt(0) & nominal.ge(0)), ("N2", nominal.lt(0))]:
        category.loc[valid & mask] = label
    audit.verify(period, "SGrowth_NR", category, "P04", "Period", "FULL")
    for family, letter in [("r", "R"), ("n", "N")]:
        g = expected[f"{family}_cagr"]
        rank = pd.to_numeric(source("in_rank_2019", 2019), errors="coerce").eq(1)
        manu = pd.to_numeric(source("manufacturing", 2019), errors="coerce").eq(1)
        for group, eligible in [("all", valid), ("rank2019", valid & rank), ("manu2019", valid & rank & manu)]:
            values = g.loc[eligible]
            column = f"{letter}Perf_Q_{group}"
            if values.empty:
                audit.note("P04", "NOT_EVALUABLE", "Period", column, "Empty canonical benchmark.")
                continue
            p10, p90 = values.quantile(.1), values.quantile(.9)
            masks = [valid & g.le(p10), valid & g.ge(p90), valid & g.gt(p10) & g.lt(0), valid & g.ge(0) & g.lt(p90)]
            classification = pd.Series(pd.NA, index=period.index, dtype="string")
            for mask, label in zip(masks, ["Bottom 10%", "Top 10%", "Moderate decline", "Moderate growth"]):
                classification.loc[mask] = label
            audit.verify(period, column, classification, "P04", "Period", "FULL", {"p10": p10, "p90": p90, "benchmark_n": len(values), "benchmark": group})
            overlaps = sum(mask.astype(int) for mask in masks).gt(1)
            audit.emit("P05", period, overlaps, column, dataset="Period", period="FULL", reason="Performance assignment conditions overlap; actual sequential builder assignment retained.", related={"p10": p10, "p90": p90})
            audit.thresholds.append({"growth_type": "real" if family == "r" else "nominal", "benchmark_group": group,
                                     "performance_column": column, "p10": p10, "p90": p90, "n_used_for_threshold": len(values),
                                     "mean_growth_bottom10": values[values.le(p10)].mean(), "mean_growth_top10": values[values.ge(p90)].mean(),
                                     "mean_growth_population": values.mean(), "bottom_n": int(values.le(p10).sum()), "top_n": int(values.ge(p90).sum())})
    for p, d in period_def.PERIODS.items():
        n = numeric(period, f"ngrowth_log_ann_{p}")
        r = numeric(period, f"rgrowth_log_ann_{p}")
        inflation = np.log(core_def.PRICE_INDEX_BY_YEAR[d["end"]] / core_def.PRICE_INDEX_BY_YEAR[d["start"]]) / d["years"]
        audit.emit("T06", period, finite(n) & finite(r) & (n - r - inflation).abs().gt(ATOL + RTOL * abs(inflation)), f"rgrowth_log_ann_{p}", n - inflation,
                   classification="INVALID", severity="high", dataset="Period", period=p, reason="Real/nominal growth difference is inconsistent with the fixed CPI and duration.")


def screen(audit, frame, columns, dataset):
    for column in columns:
        values = numeric(frame, column)
        eligible = finite(values)
        if audit.invalid_screen_values:
            eligible &= pd.Series([(dataset, str(row.get("nip", "")), json_value(row.get("year")), column) not in audit.invalid_screen_values for row in frame[[x for x in ["nip", "year"] if x in frame]].to_dict("records")], index=frame.index)
        if dataset == "Core":
            eligible &= numeric(frame, "year").ge(2019) | (column in {"sales", "sales_real", "ln_sales"})
        positive_level = column in core_def.ANNUAL_COLUMNS and column not in {"net_profit", "operating_result", "profit_before_tax", "income_tax", "equity"} or "_per_employee" in column
        transformed = log_positive(values) if positive_level else values
        eligible &= finite(transformed)
        time_groups = frame.groupby("year", dropna=False).groups if "year" in frame else {"period_variable": frame.index}
        for time, indexes in time_groups.items():
            pool = pd.Index(indexes)[eligible.loc[indexes]]
            if len(pool) < MIN_REFERENCE_N:
                audit.note("T05", "NOT_EVALUABLE", dataset, f"{column}:{time}", "Insufficient finite/domain-valid pooled reference.", len(pool))
                continue
            sectors = frame.loc[pool, "sector_en"].fillna("Unknown") if "sector_en" in frame else pd.Series("Unknown", index=pool)
            for sector, members in sectors.groupby(sectors).groups.items():
                reference = pd.Index(members) if len(members) >= MIN_REFERENCE_N else pool
                ref = transformed.loc[reference]
                med, mad = ref.median(), (ref - ref.median()).abs().median()
                q1, q3 = ref.quantile(.25), ref.quantile(.75)
                iqr = q3 - q1
                if mad > 0:
                    method, lower, upper = "MAD", med - MAD_LIMIT * 1.4826 * mad, med + MAD_LIMIT * 1.4826 * mad
                    scores = (transformed - med).abs() / (1.4826 * mad)
                    bad = scores.gt(MAD_LIMIT)
                elif iqr > 0:
                    method, lower, upper = "IQR fallback", q1 - 3 * iqr, q3 + 3 * iqr
                    scores = pd.Series(np.nan, index=frame.index)
                    bad = transformed.lt(lower) | transformed.gt(upper)
                else:
                    audit.note("T05", "NOT_EVALUABLE", dataset, f"{column}:{time}:{sector}", "Both MAD and IQR are zero.", len(reference))
                    continue
                selected = pd.Series(frame.index.isin(members), index=frame.index)
                details = {"method": method, "transformation": "natural log of positive level" if positive_level else "raw signed value",
                           "reference_scope": f"{time}:sector={sector}" if len(members) >= MIN_REFERENCE_N else f"{time}:pooled",
                           "reference_n": len(reference), "median": med, "MAD": mad, "Q1": q1, "Q3": q3,
                           "lower_fence": lower, "upper_fence": upper, "robust_score": scores,
                           "p1_raw": values.loc[reference].quantile(.01), "p99_raw": values.loc[reference].quantile(.99)}
                audit.references.append({"dataset": dataset, "variable": column, "time": json_value(time), "assigned_sector": str(sector), **{k: json_value(v) for k, v in details.items() if not isinstance(v, pd.Series)}})
                period = column.rsplit("_", 1)[-1] if dataset == "Period" else ""
                if "2019_2024" in column:
                    period = "FULL"
                audit.emit("T05", frame, selected & bad & eligible, column, classification="EXTREME", dataset=dataset, period=period,
                           reason=f"Exploratory {method} screen on eligible {details['transformation']}; retained for review.", related=details)


def audit_source(audit, core, source):
    if source is None:
        audit.note("D10", "NOT_EVALUABLE", "Source", "input panel", "Optional panel unavailable; discarded duplicates cannot be recovered from canonical files.")
        audit.note("D11", "NOT_EVALUABLE", "Source", "input panel", "Optional panel unavailable.")
        return
    if not {"nip", "year"}.issubset(source):
        audit.note("D11", "NOT_EVALUABLE", "Source", "panel keys", "Source keys unavailable.")
        return
    duplicates = source.duplicated(["nip", "year"], keep=False)
    audit.emit("D10", source, duplicates, "nip+year", severity="high", dataset="Source", reason="Input has competing firm-year rows before canonical deduplication; source provenance requires review.")
    if duplicates.any() or core.duplicated(["nip", "year"]).any():
        audit.note("D11", "NOT_EVALUABLE", "Source", "raw values", "Duplicate candidates prevent an unambiguous selected-source comparison.")
        return
    indexed = source.set_index(["nip", "year"])
    keys = pd.MultiIndex.from_frame(core[["nip", "year"]])
    for col in core_def.ANNUAL_COLUMNS:
        if col not in source:
            continue
        original = indexed[col].reindex(keys)
        if col == "sales" and "przychody" in source:
            original = original.fillna(indexed.przychody.reindex(keys))
        original = pd.Series(pd.to_numeric(original, errors="coerce").to_numpy(), index=core.index)
        audit.verify(core, col, original, "D11", related={"source_column": col, "sales_fallback": "przychody only where sales missing" if col == "sales" else "none"})
    # A duplicate source can contaminate retained YOY values. Canonical recomputation in T07
    # tests the effect; this context establishes whether duplicate dependence was possible.
    audit.note("T07", "PASS", "Source", "pre-deduplication keys", "Input panel firm-year keys unique; no discarded-duplicate dependency possible in this panel snapshot.", len(source))


def audit_frames(core, period, source=None, statistical=True):
    """Pure in-memory entry point: caller inputs remain unchanged."""
    audit = Audit()
    core, period = core.copy(deep=True).reset_index(drop=True), period.copy(deep=True).reset_index(drop=True)
    if {"nip", "year"}.issubset(core) and not core.duplicated(["nip", "year"]).any():
        audit.core_context = {(str(row["nip"]), row["year"]): row for row in core.to_dict("records")}
    for rule, reason in DEFERRED.items():
        audit.note(rule, "NOT_EVALUABLE", "Core/source", "documentary prerequisites", reason)
    validate_structure(audit, core, "Core", core_def.CORE_COLUMNS, ["nip", "year"])
    validate_structure(audit, period, "Period", period_columns(), ["nip"])
    required_core = {"nip", "year", "company", "sales"}
    if required_core.issubset(core) and "nip" in period:
        audit_core(audit, core)
        audit_time_series(audit, core)
        audit_period(audit, core, period)
        audit_source(audit, core, source)
        if statistical:
            audit.invalid_screen_values = {(r["dataset"], r["nip"], r["year"], r["variable"]) for r in audit.rows if r["classification"] == "INVALID"}
            screen(audit, core, [*core_def.ANNUAL_COLUMNS, *RATIOS, "ln_sales", "ln_total_assets", "ln_employment", "sales_real", "sales_growth_yoy", "sales_real_growth_yoy", "sales_log_growth_yoy"], "Core")
            numeric_period = [x for x in period_columns() if "growth" in x or "_start_" in x or x.startswith(("rindex_", "nindex_")) and not x.endswith("2019")]
            screen(audit, period, numeric_period, "Period")
    else:
        audit.note("P01", "NOT_EVALUABLE", "Period", "missing fundamental keys", "Schema findings reported; dependent numerical evaluation skipped.")
    for dataset, frame in [("Core", core), ("Period", period)]:
        for col in frame:
            audit.inventory.append({"dataset": dataset, "variable": col, "dtype": str(frame[col].dtype),
                                    "nonmissing": int(frame[col].notna().sum()), "missing": int(frame[col].isna().sum()),
                                    "definition_reference": "Preserved specification §2; current builder constants", "source_file": f"data_{dataset.lower()}_2018-2024.parquet"})
            if dataset == "Period":
                vals = pd.to_numeric(frame[col], errors="coerce")
                audit.coverage.append({"dataset": dataset, "variable": col, "year": None, "rows": len(frame), "nonmissing": int(frame[col].notna().sum()), "missing": int(frame[col].isna().sum()), "zero": int(vals.eq(0).sum()), "negative": int(vals.lt(0).sum()), "status": "DERIVED_AVAILABILITY"})
    findings = pd.DataFrame(audit.rows, columns=FINDING_COLUMNS)
    if findings.finding_id.duplicated().any():
        # Two manifestations of the same observation/rule share one stable finding.
        findings = findings.drop_duplicates("finding_id", keep="first")
    findings = findings.sort_values(["dataset", "nip", "year", "period", "variable", "rule_id"], kind="mergesort", na_position="last").reset_index(drop=True)
    for rule in audit.rules:
        rule["findings"] = int(findings.rule_id.eq(rule["rule_id"]).sum())
        contexts = [c for c in audit.contexts if c["rule_id"] == rule["rule_id"]]
        rule["evaluated_contexts"] = sum(c["status"] in {"PASS", "FLAG"} for c in contexts)
        rule["not_evaluable_contexts"] = sum(c["status"] == "NOT_EVALUABLE" for c in contexts)
        if rule["rule_id"] == "T04":
            rule["limitation"] = "Spikes/troughs implemented; exact multi-field repeated-vector copying screen deferred (needs field/rounding definition)."
        if rule["rule_id"] == "D07":
            rule["limitation"] = "Pattern, cross-dataset membership and shared registration identifiers; checksum/surrogate resolution deferred."
        if rule["rule_id"] == "P04":
            rule["limitation"] = "Canonical classifications independently checked; threshold workbook values not compared because source Excel edits are preserved. Recomputed thresholds in metadata."
        if rule["rule_id"] == "E06":
            rule["implementation"] = "COVERED_BY A05/P02"
            rule["limitation"] = "Export quotient and copied starting values share independent A05/P02 tests; no duplicated calculation findings. Workbook display formats are not source-data proof."
    return findings, audit


def merge_decisions(findings, path, persist=True):
    """Append new IDs only; existing log bytes and prior decisions are preserved."""
    existing = pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else pd.DataFrame(columns=DECISION_COLUMNS)
    if set(DECISION_COLUMNS).difference(existing.columns):
        raise ValueError("Decision log missing required columns; preserved without rewriting.")
    if not set(existing.decision).issubset(REVIEW_OUTCOMES):
        raise ValueError("Unsupported decision in persistent log; preserved for researcher correction.")
    latest = existing.drop_duplicates("finding_id", keep="last").set_index("finding_id")
    output = findings.copy()
    output["review_status"] = output.finding_id.map(latest.decision).fillna("Unresolved")
    new = findings.loc[~findings.finding_id.isin(existing.finding_id)]
    if persist and (len(new) or not path.exists()):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DECISION_COLUMNS)
            if not len(existing) and handle.tell() == 0:
                writer.writeheader()
            for row in new.to_dict("records"):
                decision = {key: json_value(row.get(key)) or "" for key in DECISION_COLUMNS}
                decision["decision"] = "Unresolved"
                decision["approval_status"] = "No data/sample action authorised"
                writer.writerow(decision)
    return output


def company_review(findings):
    rows = []
    weights = {"critical": 4, "high": 3, "moderate": 2, "low": 1}
    for nip, group in findings.loc[findings.nip.ne("")].groupby("nip", sort=True):
        reasons = sorted({f"{r.rule_id} {r.variable}: {r.reason}" for r in group.itertuples()})
        examples = group.loc[group.sales.notna() | group.exports.notna(), ["year", "sales", "exports", "net_profit", "employment", "total_assets"]].drop_duplicates().sort_values("year").to_dict("records")
        max_severity = max(group.severity, key=lambda x: weights[x])
        relevant = group.affected_variables.ne("") | group.variable.str.contains("growth|_start_|export_ratio", regex=True)
        rows.append({"company": group.company.iloc[0], "nip": nip, "highest_severity": max_severity,
                     "distinct_rules": group.rule_id.nunique(), "analytically_relevant_findings": int(relevant.sum()),
                     "findings": len(group), "invalid_findings": int(group.classification.eq("INVALID").sum()),
                     "unresolved_high_priority": int((group.severity.isin(["critical", "high"]) & group.review_status.eq("Unresolved")).sum()),
                     "years": ", ".join(str(int(y)) for y in sorted(group.year.dropna().unique())),
                     "rules": ", ".join(sorted(group.rule_id.unique())), "reasons": "\n".join(reasons),
                     "source_amounts": json.dumps(json_value(examples), ensure_ascii=False),
                     "existing_manual_exclusion": bool(group.existing_manual_exclusion.any()),
                     "manual_reason": "; ".join(sorted(set(group.manual_reason) - {""})), "severity_rank": weights[max_severity]})
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result = result.sort_values(["severity_rank", "distinct_rules", "analytically_relevant_findings", "findings", "nip"], ascending=[False, False, False, False, True], kind="mergesort").reset_index(drop=True)
    result.insert(0, "review_rank", np.arange(1, len(result) + 1))
    return result.drop(columns="severity_rank")


def summary_rows(findings, firms, companies):
    rows = []
    affected = findings.loc[findings.nip.ne(""), "nip"].nunique()
    unique_firm_years = findings.loc[findings.dataset.eq("Core") & findings.year.notna() & findings.nip.ne("")].drop_duplicates(["nip", "year"])
    headlines = {"Total findings": len(findings), "Canonical population firms": firms,
                 "Unique affected companies": affected, "Affected firm share": affected / firms if firms else None,
                 "Affected Core firm-years": len(unique_firm_years),
                 "Unresolved high-priority findings": int((findings.severity.isin(["critical", "high"]) & findings.review_status.eq("Unresolved")).sum()),
                 "INVALID findings": int(findings.classification.eq("INVALID").sum())}
    for metric, count in headlines.items():
        rows.append({"section": "Headlines", "category": metric, "findings": count})
    for column in ["classification", "severity", "rule_id", "year", "sector", "variable", "dataset"]:
        for value, group in findings.groupby(column, dropna=False):
            rows.append({"section": column, "category": json_value(value) if pd.notna(value) else "Unavailable/Period", "findings": len(group), "affected_companies": group.loc[group.nip.ne(""), "nip"].nunique(), "affected_firm_years": len(group.loc[group.year.notna()].drop_duplicates(["nip", "year"]))})
    return rows, headlines


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def protected_files(root):
    files = {p for p in root.rglob("*") if p.is_file() and (p.suffix in {".xlsx", ".parquet"} or p.name in {"code_config.py", "code_build_core.py", "code_build_period.py", "code_ols_scenarios.py", "code_quantile.py", "code_severe_p1_decline.py", "code_diagnostics_trajectories.py", "documentation_data_quality_audit_specification.md"})}
    return sorted(p for p in files if p.name != "results_data_quality_audit.xlsx")


def export_workbook(payload, destination, previews=None):
    # Artifact-tool exceeded the full report's memory budget (1M+ populated/styled
    # cells). Use the tested native-table fallback; artifact-tool inspects bounded
    # visible ranges with identical content/styles rather than dropping findings.
    from code_write_data_quality_audit import write_audit_workbook
    write_audit_workbook(json_value(payload), destination)
    if not previews:
        return
    dependencies = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies"
    node = Path(os.environ.get("GRIP_AUDIT_NODE", str(dependencies / "node/bin/node")))
    modules = Path(os.environ.get("GRIP_AUDIT_NODE_MODULES", str(dependencies / "node/node_modules")))
    if not node.exists() or not (modules / "@oai/artifact-tool").exists():
        raise RuntimeError("Bundled artifact-tool runtime unavailable. Set GRIP_AUDIT_NODE and GRIP_AUDIT_NODE_MODULES to an installed bundled runtime.")
    with tempfile.TemporaryDirectory(prefix="grip_audit_workbook_") as tmp:
        temp = Path(tmp)
        (temp / "node_modules").symlink_to(modules, target_is_directory=True)
        (temp / "code_write_data_quality_audit.mjs").write_bytes((ROOT / "code_write_data_quality_audit.mjs").read_bytes())
        data = temp / "audit.json"
        sampled = dict(payload)
        for name in ["README", "Summary", "Core_Findings", "Period_Findings", "Company_Review", "Rule_Register"]:
            sampled[name] = payload[name][:12]
        data.write_text(json.dumps(json_value(sampled), ensure_ascii=False, allow_nan=False))
        subprocess.run([str(node), str(temp / "code_write_data_quality_audit.mjs"), str(data), "", str(previews)], check=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT)
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument("--previews", type=Path)
    parser.add_argument("--no-statistical", action="store_true", help="Disable exploratory screens; integrity/accounting checks still run.")
    args = parser.parse_args(argv)
    root = args.directory.resolve()
    out = (args.output_directory or root).resolve()
    out.mkdir(parents=True, exist_ok=True)
    inputs = {"core": root / "data_core_2018-2024.parquet", "period": root / "data_period_2018-2024.parquet", "panel": root / "data_panel_2018-2024.parquet"}
    protected = protected_files(root)
    hashes_before = {str(p.relative_to(root)): file_hash(p) for p in protected}
    before_time = datetime.now(timezone.utc).isoformat()
    core, period = pd.read_parquet(inputs["core"]), pd.read_parquet(inputs["period"])
    source = pd.read_parquet(inputs["panel"]) if inputs["panel"].exists() else None
    findings, audit = audit_frames(core, period, source, statistical=not args.no_statistical)
    decision_path = out / "data_quality_audit_decisions.csv"
    decisions_before = file_hash(decision_path) if decision_path.exists() else None
    findings = merge_decisions(findings, decision_path)
    companies = company_review(findings)
    summaries, headlines = summary_rows(findings, core.nip.nunique(), companies)
    readme = [
        {"item": "GRIP data-quality audit", "description": "Reporting-only audit of every canonical firm; no data or regression sample changed."},
        {"item": "Population", "description": f"Core: {len(core):,} firm-years; Period: {len(period):,} firms. Audit version {VERSION}."},
        {"item": "2018", "description": "Approved sales-only year, solely for P1 lag. Other 2018 financial fields are EXPECTED_UNAVAILABLE, never errors."},
        {"item": "Missing blocks", "description": "2023 operating results, tax and maturity-specific liabilities are source unavailable. Detailed coverage/statuses are in supporting metadata."},
        {"item": "Classification", "description": "INVALID: demonstrated violation. SUSPICIOUS: unresolved source/accounting question. EXTREME: potentially valid exploratory screen."},
        {"item": "Severity", "description": "Critical structural failure; high source/calculation priority; moderate review; low minor exceedance. Severity is not invalidity."},
        {"item": "Navigation", "description": "Summary: counts and top 20. Core_Findings / Period_Findings: full evidence. Company_Review: all affected firms. Rule_Register: implemented/deferred rules."},
        {"item": "Ranking", "description": "Lexicographic descending: maximum severity (critical > high > moderate > low), distinct rule count, analytically relevant finding count, total finding count; NIP ascending breaks ties. Ranking is for review, not exclusion."},
        {"item": "Numerical tolerance", "description": f"Independent formula checks: atol={ATOL}, rtol={RTOL}. Accounting residuals use review-only relative tolerance 1e-6, not proof of invalidity."},
        {"item": "Statistical screening", "description": f"Eligible finite values, sector-year/period reference n≥{MIN_REFERENCE_N}, pooled fallback; MAD score >{MAD_LIMIT}, outer 3×IQR fallback; zero dispersion not evaluable. Positive levels use logs; signed ratios use raw values."},
        {"item": "Units and scope", "description": "Monetary values retain original scale; currency/scale and annual consolidation boundaries unresolved. Ratios are fractions; ratio_percent is a typed percentage-point number."},
        {"item": "Source evidence", "description": "Canonical Parquet values; optional local panel for selected-source/duplicate lineage. Related_values records endpoints, numerators, denominators and screen references. No original-report scope inference."},
        {"item": "Researcher review", "description": "Persistent data_quality_audit_decisions.csv keyed by stable finding_id. Retain; Correction proposed; Variable unusable; Period comparability issue; Firm exclusion proposed; Unresolved. Decisions do not execute changes."},
        {"item": "Supporting metadata", "description": "results_data_quality_audit_metadata.json contains coverage, variable inventory, thresholds, screen references, check statuses, hashes and top 20."},
        {"item": "Review evidence", "description": "Stable IDs omit measured values so decisions survive data-value revisions. Review must be reconsidered when source hashes/values change; existing decisions are preserved, never automatic authority to clean data."},
        {"item": "Existing manual exclusions (supplementary)", "description": "Reported below only; not applied to the audit population."},
    ]
    for entry in MANUAL_EXCLUSIONS["companies"]:
        matched = core.nip.astype(str).eq(entry["nip"]) | core.company.astype(str).str.strip().eq(entry["company"])
        readme.append({"item": entry["company"], "description": f"NIP {entry['nip']} | {entry['reason_code']} | present={bool(matched.any())}. {MANUAL_EXCLUSION_REASONS[entry['reason_code']]}"})
    payload = {"README": readme, "Summary": summaries, "Top20": companies.head(20).to_dict("records"),
               "Core_Findings": findings.loc[findings.dataset.ne("Period")].to_dict("records"),
               "Period_Findings": findings.loc[findings.dataset.eq("Period")].to_dict("records"),
               "Company_Review": companies.to_dict("records"), "Rule_Register": audit.rules,
               "finding_columns": FINDING_COLUMNS}
    destination = out / "results_data_quality_audit.xlsx"
    export_workbook(payload, destination, args.previews)
    hashes_after = {str(p.relative_to(root)): file_hash(p) for p in protected}
    if hashes_before != hashes_after:
        raise RuntimeError("Protected input changed during audit; investigate external edit/concurrent process. Audit never writes these inputs.")
    metadata = {"version": VERSION, "started_utc": before_time, "completed_utc": datetime.now(timezone.utc).isoformat(),
                "python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__, "xlsxwriter": __import__("xlsxwriter").__version__,
                "input_hashes_before": hashes_before, "input_hashes_after": hashes_after, "protected_files_unchanged": True,
                "code_hashes": {name: file_hash(ROOT / name) for name in ["code_audit_data_quality.py", "code_write_data_quality_audit.py", "code_write_data_quality_audit.mjs"]},
                "workbook_exporter": "XlsxWriter native-table fallback after artifact-tool full-size memory failure; bounded artifact-tool previews",
                "decision_log_hash_before": decisions_before, "decision_log_hash_after": file_hash(decision_path),
                "workbook_sha256": file_hash(destination), "summary": headlines, "top20": payload["Top20"],
                "manual_exclusion_policy_applied": False, "statistical_screening": not args.no_statistical,
                "core_rows": len(core), "period_rows": len(period), "canonical_firms": core.nip.nunique(),
                "coverage": audit.coverage, "variable_inventory": audit.inventory, "recomputed_performance_thresholds": audit.thresholds,
                "screen_references": audit.references, "execution_contexts": audit.contexts, "rule_register": audit.rules}
    (out / "results_data_quality_audit_metadata.json").write_text(json.dumps(json_value(metadata), ensure_ascii=False, indent=2, allow_nan=False))
    print(json.dumps(json_value(headlines), ensure_ascii=False, indent=2))
    if not companies.empty:
        print(companies.head(20)[["review_rank", "company", "nip", "highest_severity", "distinct_rules", "findings"]].to_string(index=False))
    print("Protected input hashes unchanged; no regressions, data corrections or sample exclusions executed.")
    return findings, audit


if __name__ == "__main__":
    main()
