"""
Shared analytical definitions for the GRIP 2019-2024 resilience analysis.

This file contains stable project-wide definitions only.
Script-specific run settings remain inside individual scripts.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


PERIODS = {
    "P1": {"start": 2019, "end": 2020, "years": 1},
    "P2": {"start": 2020, "end": 2022, "years": 2},
    "P3": {"start": 2022, "end": 2024, "years": 2},
}

SAMPLE_ORDER = ["All", "Rank2019", "Rank2019_Manufacturing"]

SAMPLE_LABELS = {
    "All": "All",
    "Rank2019": "Rank2019",
    "Rank2019_Manufacturing": "Rank2019_Manufacturing",
}


def build_sample_mask(df: pd.DataFrame, sample_name: str, complete_flag_column: str | None = None) -> pd.Series:
    if sample_name not in SAMPLE_ORDER:
        raise ValueError(f"Unknown sample_name {sample_name!r}. Expected one of: {SAMPLE_ORDER}.")

    mask = pd.Series(True, index=df.index, dtype=bool)
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
    "lag_rgrowth_log_ann_P2": "Lagged real annualised log growth P2",
    "lag_rgrowth_log_ann_P3": "Lagged real annualised log growth P3",
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
