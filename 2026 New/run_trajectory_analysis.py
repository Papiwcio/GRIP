from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


CONFIG = {
    "trajectory_family": "nominal",  # options: "real", "nominal"
    "input_file": "Data_period_2019-2024.parquet",
    "output_file": "Results_trajectory_analysis.xlsx",
}

PERIOD_DEFINITIONS = {
    "P1": "2019-2020",
    "P2": "2020-2022",
    "P3": "2022-2024",
}

SAMPLE_DEFINITIONS = {
    "All": "{complete_flag} == 1",
    "Rank2019": "{complete_flag} == 1 and in_rank_2019 == 1",
    "Rank2019_Manufacturing": "{complete_flag} == 1 and in_rank_2019 == 1 and manufacturing == 1",
}

OUTPUT_SHEETS = [
    "Config",
    "Sample_Overview",
    "Trajectory_Distribution",
    "Growth_By_Trajectory",
    "Path_Index",
    "Sector_Owner_Profile",
    "Firm_Trajectories",
]

METADATA_COLUMNS = ["nip", "company", "sector_en", "ownership", "in_rank_2019", "manufacturing"]
OWNERSHIP_TEMP_COLUMN = "ownership"


def get_trajectory_columns(config: dict[str, Any]) -> dict[str, Any]:
    family = config["trajectory_family"]
    if family == "real":
        return {
            "trajectory_family": "real",
            "growth_columns": ["rgrowth_P1", "rgrowth_P2", "rgrowth_P3"],
            "complete_flag": "has_complete_rtrajectory",
            "sign_columns": ["rP1_sign", "rP2_sign", "rP3_sign"],
            "trajectory_3step": "rtrajectory_3step",
            "trajectory_label": "rtrajectory_label",
            "trajectory_group": "rtrajectory_group",
            "index_columns": ["rindex_2019", "rindex_2020", "rindex_2022", "rindex_2024"],
        }
    if family == "nominal":
        return {
            "trajectory_family": "nominal",
            "growth_columns": ["ngrowth_P1", "ngrowth_P2", "ngrowth_P3"],
            "complete_flag": "has_complete_ntrajectory",
            "sign_columns": ["nP1_sign", "nP2_sign", "nP3_sign"],
            "trajectory_3step": "ntrajectory_3step",
            "trajectory_label": "ntrajectory_label",
            "trajectory_group": "ntrajectory_group",
            "index_columns": ["nindex_2019", "nindex_2020", "nindex_2022", "nindex_2024"],
        }
    raise ValueError("CONFIG['trajectory_family'] must be either 'real' or 'nominal'.")


def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def has_ownership_data(df: pd.DataFrame) -> bool:
    return "Foreign" in df.columns or "owner_num" in df.columns or "owner" in df.columns or "owner_type" in df.columns


def build_ownership_labels(df: pd.DataFrame) -> pd.Series:
    if "Foreign" in df.columns:
        ownership = df["Foreign"].astype("string")
    elif "owner_num" in df.columns:
        ownership = np.where(pd.to_numeric(df["owner_num"], errors="coerce") == 1, "Foreign", "Domestic")
        ownership = pd.Series(ownership, index=df.index, dtype="string")
    elif "owner" in df.columns:
        ownership = df["owner"].astype("string")
    elif "owner_type" in df.columns:
        ownership = df["owner_type"].astype("string")
    else:
        return pd.Series(pd.NA, index=df.index, dtype="string")
    return ownership.replace({"foreign": "Foreign", "domestic": "Domestic"}).astype("string")


def validate_input(df: pd.DataFrame, columns: dict[str, Any]) -> None:
    required_columns = {
        "nip",
        "in_rank_2019",
        "manufacturing",
        columns["complete_flag"],
        columns["trajectory_3step"],
        columns["trajectory_label"],
        columns["trajectory_group"],
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


def build_samples(df: pd.DataFrame, columns: dict[str, Any]) -> dict[str, pd.DataFrame]:
    samples = {}
    for sample_name, query_template in SAMPLE_DEFINITIONS.items():
        query = query_template.format(complete_flag=columns["complete_flag"])
        samples[sample_name] = df.query(query, engine="python").copy()
    return samples


def period_suffixes() -> list[str]:
    return ["P1", "P2", "P3"]


def build_config_sheet(config: dict[str, Any], columns: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {"section": "config", "item": "selected trajectory family", "value": config["trajectory_family"]},
        {"section": "config", "item": "input file", "value": config["input_file"]},
        {"section": "config", "item": "output file", "value": config["output_file"]},
    ]
    rows.extend(
        {"section": "period definitions", "item": period, "value": definition}
        for period, definition in PERIOD_DEFINITIONS.items()
    )
    rows.extend(
        {
            "section": "sample definitions",
            "item": sample_name,
            "value": query_template.format(complete_flag=columns["complete_flag"]),
        }
        for sample_name, query_template in SAMPLE_DEFINITIONS.items()
    )
    rows.extend(
        [
            {"section": "trajectory levels", "item": "3step", "value": "raw sequence"},
            {"section": "trajectory levels", "item": "label", "value": "8-pattern descriptive label"},
            {"section": "trajectory levels", "item": "group", "value": "consolidated 3-group interpretation"},
        ]
    )
    return pd.DataFrame(rows)


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
        for period, growth_column, sign_column in zip(period_suffixes(), columns["growth_columns"], columns["sign_columns"]):
            growth = pd.to_numeric(sample_df[growth_column], errors="coerce")
            signs = sample_df[sign_column].astype("string")
            row[f"median_growth_{period}"] = growth.median()
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
        "median_growth_P1",
        "median_growth_P2",
        "median_growth_P3",
        "decline_share_P1",
        "decline_share_P2",
        "decline_share_P3",
        "growth_share_P1",
        "growth_share_P2",
        "growth_share_P3",
    ]
    return pd.DataFrame(rows).loc[:, ordered_columns]


def build_trajectory_distribution(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    level_columns = {
        "3step": columns["trajectory_3step"],
        "label": columns["trajectory_label"],
        "group": columns["trajectory_group"],
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
                        "share": count / total if total else np.nan,
                    }
                )
    return pd.DataFrame(rows)


def build_growth_by_trajectory(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    level_columns = {
        "label": columns["trajectory_label"],
        "group": columns["trajectory_group"],
    }
    rows = []
    for sample_name, sample_df in samples.items():
        sample_total = len(sample_df)
        for level, source_column in level_columns.items():
            for trajectory, group in sample_df.groupby(source_column, dropna=True, sort=False):
                row = {
                    "trajectory_family": columns["trajectory_family"],
                    "sample": sample_name,
                    "level": level,
                    "trajectory": trajectory,
                    "count": len(group),
                    "share": len(group) / sample_total if sample_total else np.nan,
                }
                for period, growth_column in zip(period_suffixes(), columns["growth_columns"]):
                    growth = pd.to_numeric(group[growth_column], errors="coerce")
                    row[f"median_growth_{period}"] = growth.median()
                    row[f"mean_growth_{period}"] = growth.mean()
                rows.append(row)
    ordered_columns = [
        "trajectory_family",
        "sample",
        "level",
        "trajectory",
        "count",
        "share",
        "median_growth_P1",
        "median_growth_P2",
        "median_growth_P3",
        "mean_growth_P1",
        "mean_growth_P2",
        "mean_growth_P3",
    ]
    return (
        pd.DataFrame(rows)
        .loc[:, ordered_columns]
        .sort_values(["sample", "level", "count"], ascending=[True, True, False], kind="mergesort")
    )


def build_path_index(samples: dict[str, pd.DataFrame], columns: dict[str, Any]) -> pd.DataFrame:
    level_columns = {
        "label": columns["trajectory_label"],
        "group": columns["trajectory_group"],
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
            row[output_name] = pd.to_numeric(group[source_column], errors="coerce").median()
        rows.append(row)

    for sample_name, sample_df in samples.items():
        append_index_row(sample_name, "total", "Total", sample_df)
        for level, source_column in level_columns.items():
            for trajectory, group in sample_df.groupby(source_column, dropna=True, sort=False):
                append_index_row(sample_name, level, trajectory, group)

    return pd.DataFrame(rows)


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
            grouped["row_share"] = grouped["count"] / totals
            for _, row in grouped.iterrows():
                rows.append(
                    {
                        "trajectory_family": columns["trajectory_family"],
                        "sample": sample_name,
                        "profile_type": profile_type,
                        "profile_value": row["profile_value"],
                        "trajectory_group": row["trajectory_group"],
                        "count": int(row["count"]),
                        "row_share": row["row_share"],
                    }
                )
    return pd.DataFrame(rows)


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
        "header": workbook.add_format({"bold": True, "bottom": 1}),
        "percent": workbook.add_format({"num_format": "0.0%"}),
        "integer": workbook.add_format({"num_format": "#,##0"}),
        "decimal": workbook.add_format({"num_format": "0.000"}),
    }


def set_table_column_widths(worksheet, df: pd.DataFrame) -> None:
    for col_idx, column_name in enumerate(df.columns):
        series = df[column_name]
        if series.empty:
            max_content_width = len(str(column_name))
        else:
            sample_width = series.astype("string").fillna("").map(len).max()
            max_content_width = max(len(str(column_name)), int(sample_width))
        width = min(max(max_content_width + 2, 12), 32)
        worksheet.set_column(col_idx, col_idx, width)


def apply_header_format(worksheet, df: pd.DataFrame, header_format) -> None:
    for col_idx, column_name in enumerate(df.columns):
        worksheet.write(0, col_idx, column_name, header_format)


def apply_table_number_formats(worksheet, df: pd.DataFrame, formats: dict[str, object]) -> None:
    count_columns = {"count", "n_firms", "n_sectors"}
    for row_idx in range(len(df)):
        excel_row = row_idx + 1
        for col_idx, column_name in enumerate(df.columns):
            value = df.iloc[row_idx, col_idx]
            if pd.isna(value) or not pd.api.types.is_number(value):
                continue
            lower_name = str(column_name).lower()
            if "share" in lower_name:
                cell_format = formats["percent"]
            elif lower_name in count_columns or lower_name.endswith("_count"):
                cell_format = formats["integer"]
            else:
                cell_format = formats["decimal"]
            worksheet.write_number(excel_row, col_idx, float(value), cell_format)


def write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, formats: dict[str, object]) -> None:
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]
    apply_header_format(worksheet, df, formats["header"])
    set_table_column_widths(worksheet, df)
    apply_table_number_formats(worksheet, df, formats)
    worksheet.freeze_panes(1, 0)
    if not df.empty:
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)


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
    written_sheets: list[str],
    output_path: Path,
    columns: dict[str, Any],
) -> None:
    print("Trajectory analysis complete")
    print(f"selected_trajectory_family: {columns['trajectory_family']}")
    print(f"input_row_count: {len(df):,}")
    print(f"complete_trajectories_selected_family: {int(df[columns['complete_flag']].sum()):,}")
    print("sample_counts:")
    for sample_name, sample_df in samples.items():
        print(f"{sample_name}: {sample_df['nip'].nunique():,}")
    print(f"written_sheet_names: {written_sheets}")
    print(f"output_path: {output_path.resolve()}")


def run_trajectory_analysis(config: dict[str, Any] = CONFIG) -> dict[str, Any]:
    config = dict(config)
    columns = get_trajectory_columns(config)
    input_path = Path(config["input_file"])
    output_path = Path(config["output_file"])

    df = load_input_data(input_path).copy()
    validate_input(df, columns)

    samples = build_samples(df, columns)
    tables = {
        "Config": build_config_sheet(config, columns),
        "Sample_Overview": build_sample_overview(samples, columns),
        "Trajectory_Distribution": build_trajectory_distribution(samples, columns),
        "Growth_By_Trajectory": build_growth_by_trajectory(samples, columns),
        "Path_Index": build_path_index(samples, columns),
        "Sector_Owner_Profile": build_sector_owner_profile(samples, columns),
        "Firm_Trajectories": build_firm_trajectories(df, columns),
    }
    written_sheets = write_workbook(output_path, tables)
    print_validation(df, samples, written_sheets, output_path, columns)
    return {
        "tables": tables,
        "written_sheets": written_sheets,
        "output_path": output_path,
        "trajectory_family": columns["trajectory_family"],
    }


if __name__ == "__main__":
    run_trajectory_analysis()
