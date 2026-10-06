"""
Shared analytical definitions for the GRIP 2019-2024 resilience analysis.

The source period dataset covers 2018-2024, with 2018 used only for P1 lag
growth. Main outcomes, trajectories, and FULL growth remain 2019-2024.
This file contains stable project-wide definitions only.
Shared period-model settings live here so regression and diagnostic workbooks
use identical variables, periods, samples, interactions, and winsorisation
rules. Output paths and other script-specific settings remain in each runner.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pandas as pd


# Manual analytical exclusions. Keep canonical data intact; apply before
# scenario selection, complete-case filtering, winsorisation and scaling.
MANUAL_EXCLUSION_REASONS = {
    "M_AND_A": "Non-comparable financial statements due to M&A activities.",
    "LIQUIDATION": "Non-comparable financial statements due to liquidation.",
    "RANK2019_MISCLASSIFIED": "Non-comparable financial statements: energy trading company incorrectly classified as eligible in the 2019 ranking.",
    "EXTREME_PROFITABILITY_RATIO": "Extreme net-profit/sales ratio from a very small sales denominator; dominates profitability variation in regression analysis.",
}

MANUAL_EXCLUSIONS = {
    "enabled": True,
    "companies": [
        {"company": "Orlen SA GK, Płock", "nip": "7740001454", "reason_code": "M_AND_A"},
        {"company": "Ignitis Polska sp. z o.o., Warszawa", "nip": "5252714003", "reason_code": "RANK2019_MISCLASSIFIED"},
        {"company": "Elektrobudowa SA w upadłości likwidacyjnej GK, Katowice", "nip": "6340135506", "reason_code": "LIQUIDATION"},
        {"company": "Zakłady Mięsne Henryk Kania SA w upadłości", "nip": "7440003325", "reason_code": "LIQUIDATION"},
        {"company": "Globus sp. z o.o., Warszawa", "nip": "7773261746", "reason_code": "EXTREME_PROFITABILITY_RATIO"},
    ],
}


def manual_exclusion_mask(df: pd.DataFrame) -> pd.Series:
    """Match verified NIPs or exact company names, ignoring outer whitespace."""
    mask = pd.Series(False, index=df.index, dtype=bool)
    if not MANUAL_EXCLUSIONS["enabled"]:
        return mask
    if not {"company", "nip"}.issubset(df.columns):
        raise ValueError("Manual exclusions require company and nip columns.")
    names = df.company.astype("string").str.strip()
    nips = df.nip.astype("string").str.strip()
    for entry in MANUAL_EXCLUSIONS["companies"]:
        if entry.get("reason_code") not in MANUAL_EXCLUSION_REASONS:
            raise ValueError(f"Manual exclusion has a missing/unknown reason code: {entry}")
        mask |= (names.eq(entry["company"]) | nips.eq(entry["nip"])).fillna(False)
    return mask


def apply_manual_exclusions(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Exclude whole firms and record present/absent status for every rule."""
    mask = manual_exclusion_mask(df)
    rows = []
    names = df.company.astype("string").str.strip()
    nips = df.nip.astype("string").str.strip()
    for entry in MANUAL_EXCLUSIONS["companies"]:
        matched = (names.eq(entry["company"]) | nips.eq(entry["nip"])).fillna(False)
        if not MANUAL_EXCLUSIONS["enabled"]:
            matched &= False
        observed = df.loc[matched]
        rows.append({**entry, "reason_description": MANUAL_EXCLUSION_REASONS[entry["reason_code"]],
                     "matched_firms": int(observed.nip.nunique()),
                     "matched_rows": len(observed),
                     "observed_names": "; ".join(sorted(observed.company.dropna().astype(str).unique())),
                     "status": "Removed from analysis" if matched.any() else
                               "Already absent from input" if MANUAL_EXCLUSIONS["enabled"] else "Disabled"})
    filtered = df.loc[~mask].copy()
    assert not manual_exclusion_mask(filtered).any()
    print(f"Manual exclusions enabled={MANUAL_EXCLUSIONS['enabled']}: input rows={len(df)}, output rows={len(filtered)}, removed firms={df.loc[mask, 'nip'].nunique()}")
    print(pd.DataFrame(rows).to_string(index=False))
    return filtered, rows


def manual_exclusion_readme_rows(audit: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """One common README block for regression and diagnostic workbooks."""
    rows = [("Manual exclusions", f"Enabled={MANUAL_EXCLUSIONS['enabled']}. Shared code_config.MANUAL_EXCLUSIONS; match verified NIP or exact company name (outer whitespace ignored). Applied before analytical filtering and all transformations. Canonical data retained.")]
    rows.extend((f"Manual exclusion {i}", f"{entry['company']} | NIP {entry['nip']} | {entry['status']} | firms={entry['matched_firms']}, rows={entry['matched_rows']} | Reason [{entry['reason_code']}]: {entry['reason_description']}") for i, entry in enumerate(audit, 1))
    return rows


PERIODS = {
    "P1": {"start": 2019, "end": 2020, "years": 1},
    "P2": {"start": 2020, "end": 2022, "years": 2},
    "P3": {"start": 2022, "end": 2024, "years": 2},
}

def period_dependent_metadata(growth_mode: str, period: str) -> dict[str, str]:
    """Describe the outcome's price basis, source column, interval and formula."""
    if growth_mode not in {"nominal", "real"}:
        raise ValueError("growth_mode must be 'nominal' or 'real'.")
    details = {"start": 2019, "end": 2024, "years": 5} if period == "FULL" else PERIODS[period]
    start, end, years = details["start"], details["end"], details["years"]
    prefix = "n" if growth_mode == "nominal" else "r"
    sales = "sales" if growth_mode == "nominal" else "sales_real"
    column = f"{prefix}growth_log_ann_2019_2024" if period == "FULL" else f"{prefix}growth_log_ann_{period}"
    return {
        "column": column,
        "label": f"{growth_mode.title()} annualised log sales growth ({start}-{end})",
        "formula": f"(ln({sales}_{end}) - ln({sales}_{start})) / {years}",
        "sales_basis": "Nominal sales at current prices (sales)." if growth_mode == "nominal" else "Inflation-adjusted real sales (sales_real).",
    }


PERIOD_MODEL_SETTINGS = {
    "growth_mode": "nominal",
    "periods": [*PERIODS, "FULL"],
    "base_regressors": [
        "ln_sales",
        "profit_margin",
        "export_ratio",
        "asset_turnover",
        "capital_ratio",
        "sales_per_employee",
    ],
    "include_owner": True,
    "owner_column": "owner_num",
    "include_lag_growth": True,
    "lag_growth_periods": ["P1", "P2", "P3"],
    "categorical_controls": ["sector_en"],
    "winsorise_dependent": True,
    "winsor_lower": 0.01,
    "winsor_upper": 0.99,
    "standardised_models": True,
    "standardise_dependent": True,
    "covariance_type": "nonrobust",
    "model_variants": [
        {
            "suffix": "baseline",
            "model_family": "raw",
            "winsorised": False,
            "standardised_model": False,
        },
        {
            "suffix": "baseline_std",
            "model_family": "std",
            "winsorised": False,
            "standardised_model": True,
        },
        {
            "suffix": "winsor",
            "model_family": "raw_winsor",
            "winsorised": True,
            "standardised_model": False,
        },
        {
            "suffix": "winsor_std",
            "model_family": "std_winsor",
            "winsorised": True,
            "standardised_model": True,
        },
    ],
}

DEFAULT_REGRESSOR_RULES = {
    "column_pattern": "{base_name}_start_{period}",
    "standardise": True,
}

REGRESSOR_METADATA = {
    "ln_sales": {"interpretation": "firm size"},
    "profit_margin": {"interpretation": "profitability"},
    "export_ratio": {"interpretation": "internationalisation intensity"},
    "asset_turnover": {"interpretation": "asset efficiency"},
    "capital_ratio": {"interpretation": "equity financing strength"},
    "sales_per_employee": {"interpretation": "labour productivity"},
}

CATEGORICAL_METADATA = {
    "sector_en": {
        "reference": "production",
        "display_prefix": "sector: ",
        "interpretation_template": "sector dummy relative to production reference category",
    }
}

OWNER_METADATA = {
    "owner_num": {
        "display_name": "Foreign",
        "interpretation": "foreign ownership dummy; Domestic = 0 reference group",
        "standardise": False,
    }
}

LAG_GROWTH_METADATA = {
    "display_name": "lag_growth_log_ann",
    "interpretation": "prior-period growth persistence",
    "standardise": True,
}

INTERACTION_METADATA = {
    "export_ratio_x_ln_sales": {
        "variables": ["export_ratio", "ln_sales"],
        "display_name": "export_ratio × ln_sales",
        "interpretation": "interaction between export intensity and company size",
        "standardise": True,
        "include": True,
    }
}


def get_period_model_settings() -> dict[str, Any]:
    """Return one independent copy of the shared period-model specification."""
    return deepcopy(PERIOD_MODEL_SETTINGS)


SAMPLE_ORDER = ["All", "Rank2019", "Rank2019_Manufacturing"]

SAMPLE_LABELS = {
    "All": "All",
    "Rank2019": "Rank2019",
    "Rank2019_Manufacturing": "Rank2019_Manufacturing",
}


def build_sample_mask(df: pd.DataFrame, sample_name: str, complete_flag_column: str | None = None) -> pd.Series:
    if sample_name not in SAMPLE_ORDER:
        raise ValueError(f"Unknown sample_name {sample_name!r}. Expected one of: {SAMPLE_ORDER}.")

    mask = ~manual_exclusion_mask(df)
    if complete_flag_column is not None:
        if complete_flag_column not in df.columns:
            raise ValueError(f"Complete-flag column not found: {complete_flag_column}")
        mask &= pd.to_numeric(df[complete_flag_column], errors="coerce").eq(1)

    if sample_name in {"Rank2019", "Rank2019_Manufacturing"}:
        if "in_rank_2019" not in df.columns:
            raise ValueError("Sample requires missing column: in_rank_2019")
        mask &= pd.to_numeric(df["in_rank_2019"], errors="coerce").eq(1)

    if sample_name == "Rank2019_Manufacturing":
        if "manufacturing" not in df.columns:
            raise ValueError("Sample requires missing column: manufacturing")
        mask &= pd.to_numeric(df["manufacturing"], errors="coerce").eq(1)

    return mask


TRAJECTORY_FAMILY_MAP = {
    "real": {
        "growth_columns": ["rgrowth_P1", "rgrowth_P2", "rgrowth_P3"],
        "log_growth_columns": ["rgrowth_log_P1", "rgrowth_log_P2", "rgrowth_log_P3"],
        "log_ann_growth_columns": ["rgrowth_log_ann_P1", "rgrowth_log_ann_P2", "rgrowth_log_ann_P3"],
        "complete_flag": "has_complete_rtrajectory",
        "sign_columns": ["rP1_sign", "rP2_sign", "rP3_sign"],
        "trajectory_3step": "rtrajectory_3step",
        "trajectory_label": "rtrajectory_label",
        "trajectory_group": "rtrajectory_group",
        "index_columns": ["rindex_2019", "rindex_2020", "rindex_2022", "rindex_2024"],
        "index_2024": "rindex_2024",
        "full_period_growth": "rgrowth_ann_2019_2024",
    },
    "nominal": {
        "growth_columns": ["ngrowth_P1", "ngrowth_P2", "ngrowth_P3"],
        "log_growth_columns": ["ngrowth_log_P1", "ngrowth_log_P2", "ngrowth_log_P3"],
        "log_ann_growth_columns": ["ngrowth_log_ann_P1", "ngrowth_log_ann_P2", "ngrowth_log_ann_P3"],
        "complete_flag": "has_complete_ntrajectory",
        "sign_columns": ["nP1_sign", "nP2_sign", "nP3_sign"],
        "trajectory_3step": "ntrajectory_3step",
        "trajectory_label": "ntrajectory_label",
        "trajectory_group": "ntrajectory_group",
        "index_columns": ["nindex_2019", "nindex_2020", "nindex_2022", "nindex_2024"],
        "index_2024": "nindex_2024",
        "full_period_growth": "ngrowth_ann_2019_2024",
    },
}


def get_trajectory_family_config(family: str) -> dict[str, Any]:
    if family not in TRAJECTORY_FAMILY_MAP:
        raise ValueError("trajectory family must be one of: real, nominal")
    return {"trajectory_family": family, **TRAJECTORY_FAMILY_MAP[family]}


TRAJECTORY_LABEL_MAP = {
    "D-D-D": "Persistent decline",
    "D-G-G": "Recovery",
    "D-D-G": "Late recovery",
    "G-G-G": "Consistent growth",
    "G-D-D": "Deterioration",
    "D-G-D": "Instability",
    "G-D-G": "Volatile growth",
    "G-G-D": "Late deterioration",
}

TRAJECTORY_GROUP_MAP = {
    "D-D-D": "Persistent decline",
    "D-G-G": "Interrupted trajectory",
    "D-D-G": "Interrupted trajectory",
    "G-G-G": "Consistent growth",
    "G-D-D": "Interrupted trajectory",
    "D-G-D": "Interrupted trajectory",
    "G-D-G": "Interrupted trajectory",
    "G-G-D": "Interrupted trajectory",
}

TRAJECTORY_LEVEL_ORDER = {
    "total": 0,
    "group": 1,
    "3step": 2,
}

STATISTIC_ORDER = {
    "median": 0,
    "p10": 1,
    "p90": 2,
}

PERFORMANCE_BAND_ORDER = {
    "Bottom 10%": 0,
    "Moderate decline": 1,
    "Moderate growth": 2,
    "Top 10%": 3,
}

PERFORMANCE_BAND_COLUMNS = {
    "real": {
        "All": "RPerf_Q_all",
        "Rank2019": "RPerf_Q_rank2019",
        "Rank2019_Manufacturing": "RPerf_Q_manu2019",
    },
    "nominal": {
        "All": "NPerf_Q_all",
        "Rank2019": "NPerf_Q_rank2019",
        "Rank2019_Manufacturing": "NPerf_Q_manu2019",
    },
}

SCENARIO_LABELS = {
    "ALL": "All",
    "RANK2019": "Rank2019",
    "RANK2019_MANUFACTURING": "Rank2019_Manufacturing",
}

SCENARIO_ORDER = [
    "ALL",
    "MANUFACTURING",
    "RANK2019",
    "RANK2019_MANUFACTURING",
]

QUANTILE_ORDER = {
    "Q10": 1,
    "Q50": 2,
    "Q90": 3,
}

SCENARIO_METADATA = {
    "ALL": {
        "sample": "All",
        "filter": None,
        "label": "All firms",
    },
    "RANK2019": {
        "sample": "Rank2019",
        "filter": None,
        "label": "2019 ranking firms",
    },
    "MANUFACTURING": {
        "sample": None,
        "filter": "manufacturing == 1",
        "label": "Manufacturing firms",
    },
    "RANK2019_MANUFACTURING": {
        "sample": "Rank2019_Manufacturing",
        "filter": None,
        "label": "2019 ranking manufacturing firms",
    },
}


def get_scenario_definitions() -> dict[str, dict[str, Any]]:
    """Return the validated, ordered scenario configuration used by every analysis."""
    missing = [name for name in SCENARIO_ORDER if name not in SCENARIO_METADATA]
    extra = [name for name in SCENARIO_METADATA if name not in SCENARIO_ORDER]
    if missing or extra:
        raise ValueError(
            f"Scenario configuration mismatch: missing metadata={missing}; "
            f"metadata absent from SCENARIO_ORDER={extra}"
        )
    return {name: dict(SCENARIO_METADATA[name]) for name in SCENARIO_ORDER}


def validate_scenario_alignment(
    trajectory_scenarios: list[str],
    ols_scenarios: list[str],
) -> None:
    """Fail loudly if trajectory and OLS scenario names or ordering diverge."""
    missing_from_trajectory = [name for name in ols_scenarios if name not in trajectory_scenarios]
    missing_from_ols = [name for name in trajectory_scenarios if name not in ols_scenarios]
    if missing_from_trajectory or missing_from_ols:
        details = []
        if missing_from_trajectory:
            details.append(
                "trajectory analysis is missing scenarios used in OLS: "
                + ", ".join(missing_from_trajectory)
            )
        if missing_from_ols:
            details.append(
                "OLS is missing scenarios used in trajectory analysis: "
                + ", ".join(missing_from_ols)
            )
        raise ValueError("Scenario mismatch: " + "; ".join(details))
    if trajectory_scenarios != ols_scenarios:
        raise ValueError(
            "Scenario mismatch: trajectory and OLS scenario ordering differs: "
            f"trajectory={trajectory_scenarios}; OLS={ols_scenarios}"
        )


def build_scenario_mask(
    df: pd.DataFrame,
    scenario_name: str,
    complete_flag_column: str | None = None,
) -> pd.Series:
    """Build one scenario mask from the shared scenario metadata."""
    scenarios = get_scenario_definitions()
    if scenario_name not in scenarios:
        raise ValueError(f"Unknown scenario {scenario_name!r}. Expected one of: {list(scenarios)}")

    metadata = scenarios[scenario_name]
    sample_name = metadata["sample"]
    if sample_name is not None:
        return build_sample_mask(df, sample_name, complete_flag_column)

    mask = ~manual_exclusion_mask(df)
    if complete_flag_column is not None:
        if complete_flag_column not in df.columns:
            raise ValueError(f"Complete-flag column not found: {complete_flag_column}")
        mask &= pd.to_numeric(df[complete_flag_column], errors="coerce").eq(1)

    filter_query = metadata["filter"]
    if filter_query == "manufacturing == 1":
        if "manufacturing" not in df.columns:
            raise ValueError(
                "Scenario MANUFACTURING requires missing sector indicator column: manufacturing"
            )
        mask &= pd.to_numeric(df["manufacturing"], errors="coerce").eq(1)
    elif filter_query is not None:
        try:
            selected_index = df.query(filter_query, engine="python").index
        except Exception as exc:
            raise ValueError(
                f"Scenario {scenario_name!r} filter failed: {filter_query!r}. "
                f"Available columns include: {sorted(df.columns.tolist())}"
            ) from exc
        mask &= df.index.isin(selected_index)
    return mask

VARIABLE_ORDER = {
    "const": 0,
    "ln_sales": 10,
    "profit_margin": 20,
    "export_ratio": 30,
    "asset_turnover": 40,
    "capital_ratio": 50,
    "sales_per_employee": 60,
    "owner_num": 100,
    "lag_growth": 200,
    "interaction": 300,
    "sector": 1000,
}


def resolve_variable_order(raw_variable: str) -> int:
    if raw_variable == "const":
        return VARIABLE_ORDER["const"]
    if raw_variable.startswith("ln_sales"):
        return VARIABLE_ORDER["ln_sales"]
    if raw_variable.startswith("profit_margin"):
        return VARIABLE_ORDER["profit_margin"]
    if raw_variable.startswith("export_ratio"):
        return VARIABLE_ORDER["export_ratio"]
    if raw_variable.startswith("asset_turnover"):
        return VARIABLE_ORDER["asset_turnover"]
    if raw_variable.startswith("capital_ratio"):
        return VARIABLE_ORDER["capital_ratio"]
    if raw_variable.startswith("sales_per_employee"):
        return VARIABLE_ORDER["sales_per_employee"]
    if raw_variable == "owner_num":
        return VARIABLE_ORDER["owner_num"]
    if raw_variable.startswith("lag_"):
        return VARIABLE_ORDER["lag_growth"]
    if raw_variable.startswith("foreign_x_") or "_x_" in raw_variable:
        return VARIABLE_ORDER["interaction"]
    if raw_variable.startswith("sector_"):
        return VARIABLE_ORDER["sector"]
    return 9999

PROFILE_VARIABLES = {
    "ln_sales_start_P1": "Size: ln sales",
    "profit_margin_start_P1": "Profit margin",
    "export_ratio_start_P1": "Export ratio",
    "asset_turnover_start_P1": "Asset turnover",
    "capital_ratio_start_P1": "Capital ratio",
}

VARIABLE_LABELS = {
    "ln_sales": "Log sales",
    "ln_sales_start_P1": "Log sales at start of P1",
    "ln_sales_start_P2": "Log sales at start of P2",
    "ln_sales_start_P3": "Log sales at start of P3",
    "profit_margin": "Profit margin",
    "profit_margin_start_P1": "Profit margin at start of P1",
    "profit_margin_start_P2": "Profit margin at start of P2",
    "profit_margin_start_P3": "Profit margin at start of P3",
    "export_ratio": "Export ratio",
    "export_ratio_start_P1": "Export ratio at start of P1",
    "export_ratio_start_P2": "Export ratio at start of P2",
    "export_ratio_start_P3": "Export ratio at start of P3",
    "asset_turnover": "Asset turnover",
    "asset_turnover_start_P1": "Asset turnover at start of P1",
    "asset_turnover_start_P2": "Asset turnover at start of P2",
    "asset_turnover_start_P3": "Asset turnover at start of P3",
    "capital_ratio": "Capital ratio",
    "capital_ratio_start_P1": "Capital ratio at start of P1",
    "capital_ratio_start_P2": "Capital ratio at start of P2",
    "capital_ratio_start_P3": "Capital ratio at start of P3",
    "sales_per_employee": "Sales per employee",
    "sales_per_employee_start_P1": "Sales per employee at start of P1",
    "sales_per_employee_start_P2": "Sales per employee at start of P2",
    "sales_per_employee_start_P3": "Sales per employee at start of P3",
    "owner_num": "Foreign ownership indicator",
    "Foreign": "Foreign ownership indicator",
    "sector_en": "Sector",
    "manufacturing": "Manufacturing indicator",
    "in_rank_2019": "In 2019 ranking sample",
    "rgrowth_log_ann_P1": "Real annualised log growth P1",
    "rgrowth_log_ann_P2": "Real annualised log growth P2",
    "rgrowth_log_ann_P3": "Real annualised log growth P3",
    "rgrowth_log_ann_2019_2024": "Real annualised log growth 2019-2024",
    "ngrowth_log_ann_P1": "Nominal annualised log growth P1",
    "ngrowth_log_ann_P2": "Nominal annualised log growth P2",
    "ngrowth_log_ann_P3": "Nominal annualised log growth P3",
    "ngrowth_log_ann_2019_2024": "Nominal annualised log growth 2019-2024",
    "lag_rgrowth_log_ann_P1": "Lagged real annualised log growth P1, based on 2018-2019",
    "lag_rgrowth_log_ann_P2": "Lagged real annualised log growth P2",
    "lag_rgrowth_log_ann_P3": "Lagged real annualised log growth P3",
    "lag_ngrowth_log_ann_P1": "Lagged nominal annualised log growth P1, based on 2018-2019",
    "lag_ngrowth_log_ann_P2": "Lagged nominal annualised log growth P2",
    "lag_ngrowth_log_ann_P3": "Lagged nominal annualised log growth P3",
}


def build_ownership_labels(df: pd.DataFrame) -> pd.Series:
    if "owner_num" in df.columns:
        owner_num = pd.to_numeric(df["owner_num"], errors="coerce")
        ownership = pd.Series(pd.NA, index=df.index, dtype="string")
        ownership.loc[owner_num.eq(1)] = "Foreign"
        ownership.loc[owner_num.eq(0)] = "Domestic"
        return ownership.fillna("Unknown")

    if "Foreign" in df.columns:
        foreign = df["Foreign"]
        if pd.api.types.is_bool_dtype(foreign):
            return pd.Series(foreign, index=df.index).map({True: "Foreign", False: "Domestic"}).astype("string")
        numeric = pd.to_numeric(foreign, errors="coerce")
        if numeric.notna().any():
            ownership = pd.Series(pd.NA, index=df.index, dtype="string")
            ownership.loc[numeric.eq(1)] = "Foreign"
            ownership.loc[numeric.eq(0)] = "Domestic"
            ownership.loc[numeric.isna()] = foreign.astype("string").str.strip()
        else:
            ownership = foreign.astype("string").str.strip()
        return ownership.replace(
            {
                "foreign": "Foreign",
                "Foreign": "Foreign",
                "domestic": "Domestic",
                "Domestic": "Domestic",
                "": "Unknown",
            }
        ).fillna("Unknown")

    for column in ["owner", "owner_type"]:
        if column in df.columns:
            return (
                df[column]
                .astype("string")
                .str.strip()
                .replace(
                    {
                        "foreign": "Foreign",
                        "Foreign": "Foreign",
                        "domestic": "Domestic",
                        "Domestic": "Domestic",
                        "": "Unknown",
                    }
                )
                .fillna("Unknown")
            )

    return pd.Series("Unknown", index=df.index, dtype="string")
