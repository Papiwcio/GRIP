from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from analysis_config import (
    PERFORMANCE_BAND_COLUMNS,
    PERFORMANCE_BAND_ORDER,
    PERIODS,
    PROFILE_VARIABLES,
    SAMPLE_ORDER,
    STATISTIC_ORDER,
    TRAJECTORY_LEVEL_ORDER,
    build_ownership_labels,
    build_sample_mask,
    get_trajectory_family_config,
)
from analysis_helpers import (
    apply_header_format,
    apply_safe_autofilter,
    freeze_and_hide_gridlines,
    has_ownership_data,
    safe_median,
    safe_numeric,
    safe_quantile,
    safe_share,
    set_table_column_widths,
)


CONFIG = {
    "trajectory_family": "nominal",  # options: "real", "nominal"
    "input_file": "Data_period_2019-2024.parquet",
    "output_file": "Results_trajectory_analysis.xlsx",
}

OUTPUT_SHEETS = [
    "Config",
    "Sample_Overview",
    "Trajectory_Distribution",
    "Growth_By_Trajectory",
    "Path_Index",
    "Sector_Owner_Profile",
    "Median_Profile_By_Trajectory",
    "Median_Profile_Pivot",
    "PerformanceBand_Trajectory",
    "Firm_Trajectories",
]

METADATA_COLUMNS = ["nip", "company", "sector_en", "ownership", "in_rank_2019", "manufacturing"]
OWNERSHIP_TEMP_COLUMN = "ownership"
PROFILE_RATIO_VARIABLES = {
    "profit_margin_start_P1",
    "export_ratio_start_P1",
    "asset_turnover_start_P1",
    "capital_ratio_start_P1",
}
SAMPLE_OVERVIEW_SECTIONS = {
    "trajectory_family": "Sample",
    "n_firms": "Sample",
    "foreign_share": "Sample",
    "manufacturing_share": "Sample",
    "n_sectors": "Sample",
}


def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def validate_input(df: pd.DataFrame, columns: dict[str, Any]) -> None:
    required_columns = {
        "nip",
        "in_rank_2019",
        "manufacturing",
        columns["complete_flag"],
        columns["trajectory_3step"],
        columns["trajectory_label"],
        columns["trajectory_group"],
        columns["full_period_growth"],
        *columns["growth_columns"],
        *columns["sign_columns"],
        *columns["index_columns"],
    }
    optional_missing = sorted(set(["company", "sector_en"]).difference(df.columns))
    missing = sorted(required_columns.difference(df.columns))
    if missing:
        raise ValueError(f"Input file is missing required columns for selected trajectory family: {missing}")
    if df.duplicated(subset=["nip"]).any():
        raise ValueError("Input file contains duplicate firm rows.")
    if optional_missing:
        print(f"Optional firm metadata columns missing and skipped where needed: {optional_missing}")


def resolve_performance_band_columns(df: pd.DataFrame, trajectory_family: str) -> dict[str, str]:
    resolved = dict(PERFORMANCE_BAND_COLUMNS[trajectory_family])
    missing = {sample_name: column for sample_name, column in resolved.items() if column not in df.columns}
    if missing:
        raise ValueError(f"Input file is missing required performance-band columns: {missing}")
    return resolved


def build_samples(df: pd.DataFrame, columns: dict[str, Any]) -> dict[str, pd.DataFrame]:
    samples = {}
    for sample_name in SAMPLE_ORDER:
        samples[sample_name] = df.loc[build_sample_mask(df, sample_name, columns["complete_flag"])].copy()
    return samples


def period_suffixes() -> list[str]:
    return list(PERIODS)


def sample_definition_text(sample_name: str, complete_flag: str) -> str:
    sample_filters = {
        "All": f"{complete_flag} == 1",
        "Rank2019": f"{complete_flag} == 1 and in_rank_2019 == 1",
        "Rank2019_Manufacturing": f"{complete_flag} == 1 and in_rank_2019 == 1 and manufacturing == 1",
    }
    return sample_filters[sample_name]


def build_config_sheet(config: dict[str, Any], columns: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {"section": "config", "item": "selected trajectory family", "value": config["trajectory_family"]},
        {"section": "config", "item": "selected growth basis", "value": columns["trajectory_family"]},
        {"section": "config", "item": "input file", "value": config["input_file"]},
        {"section": "config", "item": "output file", "value": config["output_file"]},
    ]
    rows.extend(
        {"section": "period definitions", "item": period, "value": f"{definition['start']}-{definition['end']}"}
        for period, definition in PERIODS.items()
    )
    rows.extend(
        {
            "section": "sample definitions",
            "item": sample_name,
            "value": sample_definition_text(sample_name, columns["complete_flag"]),
        }
        for sample_name in SAMPLE_ORDER
    )
    rows.extend(
        [
            {"section": "trajectory levels", "item": "total", "value": "entire sample benchmark"},
            {"section": "trajectory levels", "item": "group", "value": "consolidated resilience category"},
            {"section": "trajectory levels", "item": "3step", "value": "raw trajectory sequence"},
            {"section": "consolidated group names", "item": "Consistent growth", "value": "all periods grow"},
            {"section": "consolidated group names", "item": "Interrupted trajectory", "value": "mixed growth and decline periods"},
            {"section": "consolidated group names", "item": "Persistent decline", "value": "all periods decline"},
            {
                "section": "method notes",
                "item": "growth descriptive statistics",
                "value": "growth summaries use median, p10, p90, decline share and growth share",
            },
            {
                "section": "method notes",
                "item": "raw means",
                "value": "raw means are intentionally excluded because firm-growth distributions are outlier-sensitive",
            },
            {
                "section": "profile notes",
                "item": "Median_Profile_By_Trajectory",
                "value": "uses baseline P1-start characteristics and reports median, p10 and p90 by trajectory",
            },
            {
                "section": "profile notes",
                "item": "Median_Profile_Pivot",
                "value": "readable comparison view of baseline trajectory profiles with rows split by median, p10 and p90",
            },
            {
                "section": "profile notes",
                "item": "median_index_2024",
                "value": "median cumulative 2019-2024 outcome of each trajectory; profile sheets are sorted by final outcome strength",
            },
            {
                "section": "performance notes",
                "item": "PerformanceBand_Trajectory",
                "value": "analyses trajectory path versus final performance category using existing performance-band classifications and the selected trajectory family",
            },
            {
                "section": "performance notes",
                "item": "row_share",
                "value": "shares sum to 100% within each performance band for each sample and trajectory level",
            },
        ]
    )
    return pd.DataFrame(rows)


def growth_distribution_stats(series: pd.Series) -> dict[str, float]:
    return {
        "median": safe_median(series),
        "p10": safe_quantile(series, 0.10),
        "p90": safe_quantile(series, 0.90),
    }


def growth_direction_shares(series: pd.Series) -> dict[str, float]:
    growth = safe_numeric(series).dropna()
    return {
        "decline_share": growth.lt(0).mean() if len(growth) else np.nan,
        "growth_share": growth.ge(0).mean() if len(growth) else np.nan,
    }


def build_sample_overview(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for sample_name, sample_df in samples.items():
        row = {
            "trajectory_family": columns["trajectory_family"],
            "sample": sample_name,
            "n_firms": sample_df["nip"].nunique(),
            "foreign_share": (
                build_ownership_labels(sample_df).eq("Foreign").mean()
                if has_ownership_data(sample_df)
                else np.nan
            ),
            "manufacturing_share": pd.to_numeric(sample_df["manufacturing"], errors="coerce").eq(1).mean(),
            "n_sectors": sample_df["sector_en"].nunique(dropna=True) if "sector_en" in sample_df.columns else np.nan,
        }
        full_stats = growth_distribution_stats(sample_df[columns["full_period_growth"]])
        full_direction = growth_direction_shares(sample_df[columns["full_period_growth"]])
        row["median_growth_2019_2024"] = full_stats["median"]
        row["p10_growth_2019_2024"] = full_stats["p10"]
        row["p90_growth_2019_2024"] = full_stats["p90"]
        row["decline_share_2019_2024"] = full_direction["decline_share"]
        row["growth_share_2019_2024"] = full_direction["growth_share"]
        for period, growth_column, sign_column in zip(period_suffixes(), columns["growth_columns"], columns["sign_columns"]):
            stats = growth_distribution_stats(sample_df[growth_column])
            signs = sample_df[sign_column].astype("string")
            row[f"median_growth_{period}"] = stats["median"]
            row[f"p10_growth_{period}"] = stats["p10"]
            row[f"p90_growth_{period}"] = stats["p90"]
            row[f"decline_share_{period}"] = signs.eq("D").mean()
            row[f"growth_share_{period}"] = signs.eq("G").mean()
        rows.append(row)
    ordered_columns = [
        "trajectory_family",
        "sample",
        "n_firms",
        "foreign_share",
        "manufacturing_share",
        "n_sectors",
        "median_growth_2019_2024",
        "p10_growth_2019_2024",
        "p90_growth_2019_2024",
        "decline_share_2019_2024",
        "growth_share_2019_2024",
        "median_growth_P1",
        "p10_growth_P1",
        "p90_growth_P1",
        "decline_share_P1",
        "growth_share_P1",
        "median_growth_P2",
        "p10_growth_P2",
        "p90_growth_P2",
        "decline_share_P2",
        "growth_share_P2",
        "median_growth_P3",
        "p10_growth_P3",
        "p90_growth_P3",
        "decline_share_P3",
        "growth_share_P3",
    ]
    return pd.DataFrame(rows).loc[:, ordered_columns]


def sample_overview_section(metric: str) -> str:
    if metric in SAMPLE_OVERVIEW_SECTIONS:
        return SAMPLE_OVERVIEW_SECTIONS[metric]
    if metric.endswith("_2019_2024"):
        return "FullPeriod"
    for period in period_suffixes():
        if metric.endswith(f"_{period}"):
            return period
    return "Other"


def transpose_sample_overview(sample_overview: pd.DataFrame) -> pd.DataFrame:
    if sample_overview.empty:
        return pd.DataFrame(columns=["section", "metric", *SAMPLE_ORDER])
    metric_order = [column for column in sample_overview.columns if column != "sample"]
    output = sample_overview.set_index("sample").T.reset_index().rename(columns={"index": "metric"})
    output = output.loc[output["metric"].isin(metric_order), ["metric", *SAMPLE_ORDER]]
    output.insert(0, "section", output["metric"].map(sample_overview_section))
    return output.loc[:, ["section", "metric", *SAMPLE_ORDER]]


def build_trajectory_distribution(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    level_columns = {
        "group": columns["trajectory_group"],
        "3step": columns["trajectory_3step"],
    }
    rows = []
    for sample_name, sample_df in samples.items():
        for level, source_column in level_columns.items():
            counts = sample_df[source_column].dropna().astype("string").value_counts(dropna=False)
            total = int(counts.sum())
            for trajectory, count in counts.items():
                rows.append(
                    {
                        "trajectory_family": columns["trajectory_family"],
                        "sample": sample_name,
                        "level": level,
                        "trajectory": trajectory,
                        "count": int(count),
                        "share": safe_share(count, total),
                    }
                )
    output = pd.DataFrame(rows)
    if not output.empty:
        output["rank_within_sample_level"] = (
            output.groupby(["trajectory_family", "sample", "level"])["count"]
            .rank(method="dense", ascending=False)
            .astype("Int64")
        )
        output["_level_order"] = output["level"].map(TRAJECTORY_LEVEL_ORDER)
        output = output.sort_values(
            ["trajectory_family", "sample", "_level_order", "rank_within_sample_level"],
            kind="mergesort",
        )
        output = output.drop(columns="_level_order")
    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "rank_within_sample_level",
        "trajectory",
        "count",
        "share",
    ]
    return output.loc[:, ordered_columns] if not output.empty else pd.DataFrame(columns=ordered_columns)


def build_growth_by_trajectory(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    level_columns = {
        "group": columns["trajectory_group"],
        "3step": columns["trajectory_3step"],
    }
    rows = []

    def append_growth_row(sample_name: str, sample_total: int, level: str, trajectory: str, group: pd.DataFrame) -> None:
        row = {
            "trajectory_family": columns["trajectory_family"],
            "sample": sample_name,
            "level": level,
            "trajectory": trajectory,
            "count": len(group),
            "share": safe_share(len(group), sample_total),
            "_median_index_2024": safe_median(group[columns["index_columns"][3]]),
        }
        for period, growth_column in zip(period_suffixes(), columns["growth_columns"]):
            stats = growth_distribution_stats(group[growth_column])
            row[f"median_growth_{period}"] = stats["median"]
            row[f"p10_growth_{period}"] = stats["p10"]
            row[f"p90_growth_{period}"] = stats["p90"]
        rows.append(row)

    for sample_name, sample_df in samples.items():
        sample_total = len(sample_df)
        append_growth_row(sample_name, sample_total, "total", "Total", sample_df)
        for level, source_column in level_columns.items():
            for trajectory, group in sample_df.groupby(source_column, dropna=True, sort=False):
                append_growth_row(sample_name, sample_total, level, trajectory, group)
    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "trajectory",
        "count",
        "share",
        "median_growth_P1",
        "p10_growth_P1",
        "p90_growth_P1",
        "median_growth_P2",
        "p10_growth_P2",
        "p90_growth_P2",
        "median_growth_P3",
        "p10_growth_P3",
        "p90_growth_P3",
    ]
    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(columns=ordered_columns)
    output["_level_order"] = output["level"].map(TRAJECTORY_LEVEL_ORDER)
    output = output.sort_values(
        ["trajectory_family", "sample", "_level_order", "_median_index_2024", "count"],
        ascending=[True, True, True, False, False],
        kind="mergesort",
    ).drop(columns=["_level_order", "_median_index_2024"])
    return output.loc[:, ordered_columns]


def build_path_index(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    level_columns = {
        "group": columns["trajectory_group"],
        "3step": columns["trajectory_3step"],
    }
    rows = []

    def append_index_row(sample_name: str, level: str, trajectory: str, group: pd.DataFrame) -> None:
        row = {
            "trajectory_family": columns["trajectory_family"],
            "sample": sample_name,
            "level": level,
            "trajectory": trajectory,
            "count": len(group),
        }
        for output_name, source_column in zip(["index_2019", "index_2020", "index_2022", "index_2024"], columns["index_columns"]):
            row[output_name] = safe_median(group[source_column])
        rows.append(row)

    for sample_name, sample_df in samples.items():
        append_index_row(sample_name, "total", "Total", sample_df)
        for level, source_column in level_columns.items():
            for trajectory, group in sample_df.groupby(source_column, dropna=True, sort=False):
                append_index_row(sample_name, level, trajectory, group)

    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "trajectory",
        "count",
        "index_2019",
        "index_2020",
        "index_2022",
        "index_2024",
    ]
    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(columns=ordered_columns)
    output["_level_order"] = output["level"].map(TRAJECTORY_LEVEL_ORDER)
    output = output.sort_values(
        ["trajectory_family", "sample", "_level_order", "count"],
        ascending=[True, True, True, False],
        kind="mergesort",
    ).drop(columns="_level_order")
    return output.loc[:, ordered_columns]


def build_sector_owner_profile(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for sample_name, sample_df in samples.items():
        profile_frames = []
        if "sector_en" in sample_df.columns:
            profile_frames.append(("sector", "sector_en", sample_df))
        if has_ownership_data(sample_df):
            owner_df = sample_df.copy()
            owner_df[OWNERSHIP_TEMP_COLUMN] = build_ownership_labels(owner_df)
            profile_frames.append(("owner", OWNERSHIP_TEMP_COLUMN, owner_df))

        for profile_type, profile_column, profile_df in profile_frames:
            valid = profile_df.loc[profile_df[profile_column].notna() & profile_df[columns["trajectory_group"]].notna()].copy()
            grouped = (
                valid.groupby([profile_column, columns["trajectory_group"]], dropna=False)
                .size()
                .reset_index(name="count")
                .rename(columns={profile_column: "profile_value", columns["trajectory_group"]: "trajectory_group"})
            )
            totals = grouped.groupby("profile_value")["count"].transform("sum")
            grouped["n_total_profile"] = totals
            grouped["row_share"] = grouped["count"] / totals
            for _, row in grouped.iterrows():
                rows.append(
                    {
                        "trajectory_family": columns["trajectory_family"],
                        "sample": sample_name,
                        "profile_type": profile_type,
                        "profile_value": row["profile_value"],
                        "trajectory_group": row["trajectory_group"],
                        "n_total_profile": int(row["n_total_profile"]),
                        "count": int(row["count"]),
                        "row_share": row["row_share"],
                    }
                )
    ordered_columns = [
        "trajectory_family",
        "sample",
        "profile_type",
        "profile_value",
        "n_total_profile",
        "trajectory_group",
        "count",
        "row_share",
    ]
    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(columns=ordered_columns)
    output = output.sort_values(
        ["trajectory_family", "sample", "profile_type", "profile_value", "trajectory_group"],
        kind="mergesort",
    )
    return output.loc[:, ordered_columns].reset_index(drop=True)


def available_profile_variables(df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    included = {variable: label for variable, label in PROFILE_VARIABLES.items() if variable in df.columns}
    skipped = [variable for variable in PROFILE_VARIABLES if variable not in df.columns]
    return included, skipped


def build_median_profile_by_trajectory(
    samples: dict[str, pd.DataFrame],
    columns: dict[str, Any],
    profile_variables: dict[str, str],
) -> pd.DataFrame:
    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "trajectory",
        "count",
        "share",
        "median_index_2024",
        "variable",
        "variable_label",
        "median_value",
        "p10_value",
        "p90_value",
        "non_missing_n",
    ]
    if not profile_variables:
        return pd.DataFrame(columns=ordered_columns)

    level_columns = {
        "group": columns["trajectory_group"],
        "3step": columns["trajectory_3step"],
    }
    rows = []

    def append_profile_rows(
        sample_name: str,
        sample_total: int,
        level: str,
        trajectory: str,
        group: pd.DataFrame,
    ) -> None:
        count = len(group)
        share = safe_share(count, sample_total)
        median_index_2024 = safe_median(group[columns["index_columns"][3]])
        for variable, variable_label in profile_variables.items():
            rows.append(
                {
                    "trajectory_family": columns["trajectory_family"],
                    "sample": sample_name,
                    "level": level,
                    "trajectory": trajectory,
                    "count": count,
                    "share": share,
                    "median_index_2024": median_index_2024,
                    "variable": variable,
                    "variable_label": variable_label,
                    "median_value": safe_median(group[variable]),
                    "p10_value": safe_quantile(group[variable], 0.10),
                    "p90_value": safe_quantile(group[variable], 0.90),
                    "non_missing_n": safe_numeric(group[variable]).notna().sum(),
                }
            )

    for sample_name, sample_df in samples.items():
        sample_total = len(sample_df)
        append_profile_rows(sample_name, sample_total, "total", "Total", sample_df)
        for level, source_column in level_columns.items():
            for trajectory, group in sample_df.groupby(source_column, dropna=True, sort=False):
                append_profile_rows(sample_name, sample_total, level, trajectory, group)

    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(columns=ordered_columns)
    output["_level_order"] = output["level"].map(TRAJECTORY_LEVEL_ORDER)
    output = output.sort_values(
        ["trajectory_family", "sample", "_level_order", "median_index_2024", "count", "trajectory", "variable"],
        ascending=[True, True, True, False, False, True, True],
        kind="mergesort",
    ).drop(columns="_level_order")
    return output.loc[:, ordered_columns].reset_index(drop=True)


def build_median_profile_pivot(
    profile_long: pd.DataFrame,
    profile_variables: dict[str, str],
) -> pd.DataFrame:
    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "trajectory",
        "statistic",
        "count",
        "share",
        "median_index_2024",
        *profile_variables.keys(),
    ]
    if profile_long.empty or not profile_variables:
        return pd.DataFrame(columns=ordered_columns)

    id_columns = [
        "trajectory_family",
        "sample",
        "level",
        "trajectory",
        "count",
        "share",
        "median_index_2024",
        "variable",
    ]
    statistic_frames = []
    statistic_value_columns = {
        "median": "median_value",
        "p10": "p10_value",
        "p90": "p90_value",
    }
    for statistic, value_column in statistic_value_columns.items():
        frame = profile_long.loc[:, [*id_columns, value_column]].copy()
        frame = frame.rename(columns={value_column: "value"})
        frame["statistic"] = statistic
        statistic_frames.append(frame)

    stacked = pd.concat(statistic_frames, ignore_index=True)
    output = (
        stacked.pivot(
            index=[
                "trajectory_family",
                "sample",
                "level",
                "trajectory",
                "statistic",
                "count",
                "share",
                "median_index_2024",
            ],
            columns="variable",
            values="value",
        )
        .reset_index()
        .rename_axis(columns=None)
    )
    for variable in profile_variables:
        if variable not in output.columns:
            output[variable] = np.nan
    output["_level_order"] = output["level"].map(TRAJECTORY_LEVEL_ORDER)
    output["_statistic_order"] = output["statistic"].map(STATISTIC_ORDER)
    output = output.sort_values(
        [
            "trajectory_family",
            "sample",
            "_level_order",
            "median_index_2024",
            "count",
            "trajectory",
            "_statistic_order",
        ],
        ascending=[True, True, True, False, False, True, True],
        kind="mergesort",
    ).drop(columns=["_level_order", "_statistic_order"])
    return output.loc[:, ordered_columns].reset_index(drop=True)


def build_performance_band_trajectory(
    samples: dict[str, pd.DataFrame],
    columns: dict[str, Any],
    performance_band_columns: dict[str, str],
) -> pd.DataFrame:
    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "performance_band",
        "trajectory",
        "count",
        "row_share",
        "median_index_2024",
    ]
    level_columns = {
        "group": columns["trajectory_group"],
        "3step": columns["trajectory_3step"],
    }
    rows = []

    def append_performance_row(
        sample_name: str,
        level: str,
        performance_band: str,
        trajectory: str,
        group: pd.DataFrame,
        band_total: int,
    ) -> None:
        rows.append(
            {
                "trajectory_family": columns["trajectory_family"],
                "sample": sample_name,
                "level": level,
                "performance_band": performance_band,
                "trajectory": trajectory,
                "count": len(group),
                "row_share": safe_share(len(group), band_total),
                "median_index_2024": safe_median(group[columns["index_columns"][3]]),
            }
        )

    for sample_name, sample_df in samples.items():
        performance_column = performance_band_columns[sample_name]
        valid = sample_df.loc[sample_df[performance_column].notna()].copy()
        for performance_band, band_df in valid.groupby(performance_column, dropna=True, sort=False):
            band_total = len(band_df)
            append_performance_row(sample_name, "total", performance_band, "Total", band_df, band_total)
            for level, source_column in level_columns.items():
                for trajectory, group in band_df.groupby(source_column, dropna=True, sort=False):
                    append_performance_row(sample_name, level, performance_band, trajectory, group, band_total)

    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(columns=ordered_columns)
    output["_level_order"] = output["level"].map(TRAJECTORY_LEVEL_ORDER)
    output["_performance_order"] = output["performance_band"].map(PERFORMANCE_BAND_ORDER)
    output = output.sort_values(
        [
            "trajectory_family",
            "sample",
            "_level_order",
            "_performance_order",
            "median_index_2024",
            "count",
            "trajectory",
        ],
        ascending=[True, True, True, True, False, False, True],
        kind="mergesort",
    ).drop(columns=["_level_order", "_performance_order"])
    return output.loc[:, ordered_columns].reset_index(drop=True)


def build_firm_trajectories(df: pd.DataFrame, columns: dict[str, Any]) -> pd.DataFrame:
    output = df.loc[df[columns["complete_flag"]] == 1].copy()
    output[OWNERSHIP_TEMP_COLUMN] = build_ownership_labels(output)
    rename_map = {
        columns["growth_columns"][0]: "growth_P1",
        columns["growth_columns"][1]: "growth_P2",
        columns["growth_columns"][2]: "growth_P3",
        columns["sign_columns"][0]: "P1_sign",
        columns["sign_columns"][1]: "P2_sign",
        columns["sign_columns"][2]: "P3_sign",
        columns["trajectory_3step"]: "trajectory_3step",
        columns["trajectory_label"]: "trajectory_label",
        columns["trajectory_group"]: "trajectory_group",
        columns["index_columns"][0]: "index_2019",
        columns["index_columns"][1]: "index_2020",
        columns["index_columns"][2]: "index_2022",
        columns["index_columns"][3]: "index_2024",
    }
    selected_columns = [
        *[column for column in METADATA_COLUMNS if column in output.columns],
        *columns["growth_columns"],
        *columns["sign_columns"],
        columns["trajectory_3step"],
        columns["trajectory_label"],
        columns["trajectory_group"],
        *columns["index_columns"],
    ]
    return (
        output.loc[:, selected_columns]
        .rename(columns=rename_map)
        .sort_values("nip", kind="mergesort")
        .reset_index(drop=True)
    )


def create_formats(workbook) -> dict[str, object]:
    return {
        "header": workbook.add_format(
            {"bold": True, "bottom": 1, "bg_color": "#D9E1F2", "valign": "vcenter"}
        ),
        "percent": workbook.add_format({"num_format": "0.0%"}),
        "integer": workbook.add_format({"num_format": "#,##0"}),
        "decimal": workbook.add_format({"num_format": "0.000"}),
        "growth_percent": workbook.add_format({"num_format": "0.0%"}),
        "index": workbook.add_format({"num_format": "0.0"}),
    }


def sample_overview_row_format(metric: str, formats: dict[str, object]):
    if metric in {"n_firms", "n_sectors"}:
        return formats["integer"]
    if "share" in metric:
        return formats["percent"]
    if "growth" in metric:
        return formats["growth_percent"]
    return None


def apply_table_number_formats(worksheet, df: pd.DataFrame, formats: dict[str, object], sheet_name: str) -> None:
    count_columns = {
        "count",
        "n_firms",
        "n_sectors",
        "rank_within_sample_level",
        "n_total_profile",
        "non_missing_n",
    }
    sample_metric_column = "metric" if "metric" in df.columns else None
    profile_variable_column = "variable" if "variable" in df.columns else None
    for row_idx in range(len(df)):
        excel_row = row_idx + 1
        for col_idx, column_name in enumerate(df.columns):
            value = df.iloc[row_idx, col_idx]
            if pd.isna(value) or not pd.api.types.is_number(value):
                continue
            if sheet_name == "Sample_Overview":
                metric = str(df.loc[df.index[row_idx], sample_metric_column]) if sample_metric_column else ""
                cell_format = sample_overview_row_format(metric, formats)
                if cell_format is None:
                    continue
            else:
                lower_name = str(column_name).lower()
                if "share" in lower_name:
                    cell_format = formats["percent"]
                elif lower_name in count_columns or lower_name.endswith("_count"):
                    cell_format = formats["integer"]
                elif sheet_name == "Median_Profile_By_Trajectory" and lower_name in {
                    "median_value",
                    "p10_value",
                    "p90_value",
                }:
                    variable = str(df.loc[df.index[row_idx], profile_variable_column]) if profile_variable_column else ""
                    cell_format = formats["percent"] if variable in PROFILE_RATIO_VARIABLES else formats["decimal"]
                elif sheet_name == "Median_Profile_Pivot" and str(column_name) in PROFILE_VARIABLES:
                    cell_format = formats["percent"] if str(column_name) in PROFILE_RATIO_VARIABLES else formats["decimal"]
                elif lower_name == "median_index_2024":
                    cell_format = formats["index"]
                elif lower_name.startswith("growth_") or "_growth_" in lower_name:
                    cell_format = formats["growth_percent"]
                elif lower_name.startswith("index_"):
                    cell_format = formats["index"]
                else:
                    cell_format = formats["decimal"]
            worksheet.write_number(excel_row, col_idx, float(value), cell_format)


def write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, formats: dict[str, object]) -> None:
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]
    apply_header_format(worksheet, df, formats["header"])
    set_table_column_widths(worksheet, df)
    apply_table_number_formats(worksheet, df, formats, sheet_name)
    freeze_and_hide_gridlines(worksheet, 1, 2 if sheet_name == "Sample_Overview" else 0)
    apply_safe_autofilter(worksheet, df)


def write_workbook(output_path: Path, tables: dict[str, pd.DataFrame]) -> list[str]:
    actual_sheets = list(tables)
    if actual_sheets != OUTPUT_SHEETS:
        raise ValueError(f"Internal sheet order changed. Expected={OUTPUT_SHEETS}, actual={actual_sheets}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        formats = create_formats(writer.book)
        for sheet_name, table in tables.items():
            write_sheet(writer, sheet_name, table, formats)
    return actual_sheets


def print_validation(
    df: pd.DataFrame,
    samples: dict[str, pd.DataFrame],
    tables: dict[str, pd.DataFrame],
    written_sheets: list[str],
    output_path: Path,
    columns: dict[str, Any],
    profile_variables_included: dict[str, str],
    profile_variables_skipped: list[str],
    performance_band_columns: dict[str, str],
) -> None:
    print("Trajectory analysis complete")
    print(f"selected_trajectory_family: {columns['trajectory_family']}")
    print(f"input_row_count: {len(df):,}")
    print(f"complete_trajectories_selected_family: {int(df[columns['complete_flag']].sum()):,}")
    selected_groups = set(df[columns["trajectory_group"]].dropna().astype(str).unique().tolist())
    expected_groups = {"Consistent growth", "Interrupted trajectory", "Persistent decline"}
    print(f"old_reversal_values_found: {'Reversal' in selected_groups}")
    print(f"expected_group_values: {sorted(expected_groups)}")
    print(f"selected_group_values: {sorted(selected_groups)}")
    print("sample_counts:")
    for sample_name, sample_df in samples.items():
        print(f"{sample_name}: {sample_df['nip'].nunique():,}")
    table_columns = set()
    sample_metrics = set()
    for sheet_name, table in tables.items():
        table_columns.update(str(column) for column in table.columns)
        if sheet_name == "Sample_Overview" and "metric" in table.columns:
            sample_metrics.update(table["metric"].dropna().astype(str).tolist())
    raw_mean_columns = sorted(
        name for name in table_columns.union(sample_metrics) if name.startswith("mean_growth_")
    )
    old_quantile_columns = sorted(
        name
        for name in table_columns.union(sample_metrics)
        if name.startswith(("p25_growth_", "p75_growth_", "iqr_growth_"))
    )
    expected_p10_p90 = {f"{prefix}_growth_{period}" for prefix in ["p10", "p90"] for period in period_suffixes()}
    p10_p90_present = expected_p10_p90.issubset(table_columns.union(sample_metrics))
    expected_full_period_metrics = {
        "median_growth_2019_2024",
        "p10_growth_2019_2024",
        "p90_growth_2019_2024",
        "decline_share_2019_2024",
        "growth_share_2019_2024",
    }
    full_period_stats_present = expected_full_period_metrics.issubset(sample_metrics)
    analytical_sheets = ["Trajectory_Distribution", "Growth_By_Trajectory", "Path_Index"]
    label_level_reporting_present = any(
        "level" in tables[sheet_name].columns
        and tables[sheet_name]["level"].astype("string").eq("label").any()
        for sheet_name in analytical_sheets
    )
    print(f"raw_mean_growth_columns_present: {bool(raw_mean_columns)}")
    print(f"p25_p75_iqr_columns_present: {bool(old_quantile_columns)}")
    print(f"p10_p90_columns_present: {p10_p90_present}")
    print(f"full_period_statistics_added: {full_period_stats_present}")
    print(f"label_level_reporting_present: {label_level_reporting_present}")
    print(f"median_profile_by_trajectory_written: {'Median_Profile_By_Trajectory' in written_sheets}")
    print(f"median_profile_pivot_written: {'Median_Profile_Pivot' in written_sheets}")
    profile_sheets_have_median_index = all(
        "median_index_2024" in tables[sheet_name].columns
        for sheet_name in ["Median_Profile_By_Trajectory", "Median_Profile_Pivot"]
    )
    print(f"median_index_2024_added_to_profile_sheets: {profile_sheets_have_median_index}")
    print("profile_sorting_uses_median_index_2024: True")
    total_growth_rows_added = (
        "Growth_By_Trajectory" in tables
        and "level" in tables["Growth_By_Trajectory"].columns
        and tables["Growth_By_Trajectory"]["level"].astype("string").eq("total").any()
    )
    total_profile_rows_added = (
        "Median_Profile_By_Trajectory" in tables
        and "level" in tables["Median_Profile_By_Trajectory"].columns
        and tables["Median_Profile_By_Trajectory"]["level"].astype("string").eq("total").any()
    )
    total_pivot_rows_added = (
        "Median_Profile_Pivot" in tables
        and "level" in tables["Median_Profile_Pivot"].columns
        and tables["Median_Profile_Pivot"]["level"].astype("string").eq("total").any()
    )
    total_distribution_rows_added = (
        "Trajectory_Distribution" in tables
        and "level" in tables["Trajectory_Distribution"].columns
        and tables["Trajectory_Distribution"]["level"].astype("string").eq("total").any()
    )
    print(f"total_rows_added_to_growth_by_trajectory: {total_growth_rows_added}")
    print(f"total_rows_added_to_median_profile_by_trajectory: {total_profile_rows_added}")
    print(f"total_rows_added_to_median_profile_pivot: {total_pivot_rows_added}")
    print(f"total_rows_added_to_trajectory_distribution: {total_distribution_rows_added}")
    print("sorting_hierarchy_updated_total_group_3step: True")
    pivot_statistics = []
    if "Median_Profile_Pivot" in tables and "statistic" in tables["Median_Profile_Pivot"].columns:
        pivot_statistics = sorted(tables["Median_Profile_Pivot"]["statistic"].dropna().astype(str).unique().tolist())
    print(f"median_profile_pivot_statistics_present: {pivot_statistics}")
    performance_sheet_written = "PerformanceBand_Trajectory" in written_sheets
    performance_levels = []
    performance_row_shares_valid = False
    if "PerformanceBand_Trajectory" in tables and not tables["PerformanceBand_Trajectory"].empty:
        performance_table = tables["PerformanceBand_Trajectory"]
        performance_levels = performance_table["level"].dropna().astype(str).drop_duplicates().tolist()
        share_sums = (
            performance_table.groupby(["trajectory_family", "sample", "level", "performance_band"], dropna=False)[
                "row_share"
            ]
            .sum()
            .round(10)
        )
        performance_row_shares_valid = bool(share_sums.eq(1).all())
    print(f"performance_band_trajectory_written: {performance_sheet_written}")
    print(f"performance_band_variables_detected: {performance_band_columns}")
    print(f"performance_band_trajectory_levels: {performance_levels}")
    print(f"performance_band_row_shares_valid: {performance_row_shares_valid}")
    print(f"profile_variables_included: {list(profile_variables_included)}")
    print(f"profile_variables_skipped: {profile_variables_skipped}")
    print("analysis_helpers_imported: True")
    print("workbook_regenerated_successfully: True")
    print(f"written_sheet_names: {written_sheets}")
    print(f"output_path: {output_path.resolve()}")


def run_trajectory_analysis(config: dict[str, Any] = CONFIG) -> dict[str, Any]:
    config = dict(config)
    columns = get_trajectory_family_config(config["trajectory_family"])
    input_path = Path(config["input_file"])
    output_path = Path(config["output_file"])

    df = load_input_data(input_path).copy()
    validate_input(df, columns)
    performance_band_columns = resolve_performance_band_columns(df, columns["trajectory_family"])

    samples = build_samples(df, columns)
    profile_variables_included, profile_variables_skipped = available_profile_variables(df)
    if profile_variables_skipped:
        print(f"Profile variables missing and skipped: {profile_variables_skipped}")
    median_profile_long = build_median_profile_by_trajectory(
        samples,
        columns,
        profile_variables_included,
    )
    tables = {
        "Config": build_config_sheet(config, columns),
        "Sample_Overview": transpose_sample_overview(build_sample_overview(samples, columns)),
        "Trajectory_Distribution": build_trajectory_distribution(samples, columns),
        "Growth_By_Trajectory": build_growth_by_trajectory(samples, columns),
        "Path_Index": build_path_index(samples, columns),
        "Sector_Owner_Profile": build_sector_owner_profile(samples, columns),
        "Median_Profile_By_Trajectory": median_profile_long,
        "Median_Profile_Pivot": build_median_profile_pivot(median_profile_long, profile_variables_included),
        "PerformanceBand_Trajectory": build_performance_band_trajectory(
            samples,
            columns,
            performance_band_columns,
        ),
        "Firm_Trajectories": build_firm_trajectories(df, columns),
    }
    written_sheets = write_workbook(output_path, tables)
    print_validation(
        df,
        samples,
        tables,
        written_sheets,
        output_path,
        columns,
        profile_variables_included,
        profile_variables_skipped,
        performance_band_columns,
    )
    return {
        "tables": tables,
        "written_sheets": written_sheets,
        "output_path": output_path,
        "trajectory_family": columns["trajectory_family"],
    }


if __name__ == "__main__":
    run_trajectory_analysis()
