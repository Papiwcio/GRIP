from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path("Data_period_2019-2024.parquet")
OUTPUT_XLSX_PATH = Path("Results_trajectory_analysis.xlsx")

GROWTH_COLUMNS = ["growth_real_P1", "growth_real_P2", "growth_real_P3"]
INDEX_COLUMNS = ["index_2019", "index_2020", "index_2022", "index_2024"]
SAMPLE_SPECS = {
    "All": {
        "sheet_prefix": "All",
        "mask": lambda df: df["has_complete_trajectory"] == 1,
    },
    "Rank2019": {
        "sheet_prefix": "R19",
        "mask": lambda df: (df["has_complete_trajectory"] == 1) & (df["in_rank_2019"] == 1),
    },
    "Rank2019_Manufacturing": {
        "sheet_prefix": "R19M",
        "mask": lambda df: (df["has_complete_trajectory"] == 1) & (df["in_rank_2019"] == 1) & (df["manufacturing"] == 1),
    },
}
SHEET_SUFFIXES = ["Overview", "Traj3Step", "TrajGroup", "GrowthByGroup", "PathIndex", "BySector", "ByOwner"]
REQUIRED_COLUMNS = [
    "nip",
    "has_complete_trajectory",
    "trajectory_3step",
    "trajectory_group",
    "in_rank_2019",
    "manufacturing",
    "sector_en",
    *GROWTH_COLUMNS,
    *INDEX_COLUMNS,
]


def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def validate_input(df: pd.DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS).difference(df.columns))
    if missing:
        raise ValueError(f"Input file is missing required columns: {missing}")
    if df.duplicated(subset=["nip"]).any():
        raise ValueError("Input file contains duplicate firm rows.")


def get_ownership_series(df: pd.DataFrame) -> pd.Series:
    if "Foreign" in df.columns:
        ownership = df["Foreign"].astype("string")
    elif "owner_num" in df.columns:
        ownership = np.where(df["owner_num"] == 1, "Foreign", "Domestic")
        ownership = pd.Series(ownership, index=df.index, dtype="string")
    else:
        raise ValueError("Neither Foreign nor owner_num is available for ownership reporting.")
    return ownership


def build_sample(df: pd.DataFrame, sample_name: str) -> pd.DataFrame:
    mask = SAMPLE_SPECS[sample_name]["mask"](df)
    return df.loc[mask].copy()


def build_growth_descriptives(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for column in GROWTH_COLUMNS:
        series = pd.to_numeric(df[column], errors="coerce")
        rows.append(
            {
                "variable": column,
                "count": int(series.count()),
                "mean": series.mean(),
                "median": series.median(),
                "std": series.std(),
                "p01": series.quantile(0.01),
                "p05": series.quantile(0.05),
                "p25": series.quantile(0.25),
                "p75": series.quantile(0.75),
                "p95": series.quantile(0.95),
                "p99": series.quantile(0.99),
                "min": series.min(),
                "max": series.max(),
            }
        )
    return pd.DataFrame(rows)


def build_decline_growth_shares(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for column in GROWTH_COLUMNS:
        series = pd.to_numeric(df[column], errors="coerce")
        non_missing = int(series.notna().sum())
        decline_count = int((series < 0).sum())
        growth_count = int((series >= 0).sum())
        rows.append(
            {
                "period": column.replace("growth_real_", ""),
                "decline_count": decline_count,
                "growth_count": growth_count,
                "decline_share": decline_count / non_missing if non_missing else np.nan,
                "growth_share": growth_count / non_missing if non_missing else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_trajectory_frequency(df: pd.DataFrame, column: str) -> pd.DataFrame:
    counts = df[column].dropna().astype("string").value_counts(dropna=False)
    total = int(counts.sum())
    output = counts.rename_axis(column).reset_index(name="count")
    output["share"] = output["count"] / total if total else np.nan
    return output


def build_growth_by_group(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("trajectory_group", dropna=False)
        .agg(
            count=("nip", "size"),
            median_growth_real_P1=("growth_real_P1", "median"),
            median_growth_real_P2=("growth_real_P2", "median"),
            median_growth_real_P3=("growth_real_P3", "median"),
            mean_growth_real_P1=("growth_real_P1", "mean"),
            mean_growth_real_P2=("growth_real_P2", "mean"),
            mean_growth_real_P3=("growth_real_P3", "mean"),
        )
        .reset_index()
    )
    total = int(grouped["count"].sum())
    grouped["share"] = grouped["count"] / total if total else np.nan
    ordered_columns = [
        "trajectory_group",
        "count",
        "share",
        "median_growth_real_P1",
        "median_growth_real_P2",
        "median_growth_real_P3",
        "mean_growth_real_P1",
        "mean_growth_real_P2",
        "mean_growth_real_P3",
    ]
    return grouped.loc[:, ordered_columns].sort_values("count", ascending=False, kind="mergesort").reset_index(drop=True)


def build_path_index_summary(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2019, 2020, 2022, 2024],
            "mean_index": [df["index_2019"].mean(), df["index_2020"].mean(), df["index_2022"].mean(), df["index_2024"].mean()],
            "median_index": [df["index_2019"].median(), df["index_2020"].median(), df["index_2022"].median(), df["index_2024"].median()],
        }
    )


def build_crosstab(df: pd.DataFrame, column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid = df.loc[df["trajectory_group"].notna() & df[column].notna()].copy()
    counts = pd.crosstab(valid["trajectory_group"], valid[column], dropna=False)
    shares = counts.div(counts.sum(axis=1), axis=0)
    counts.index.name = "trajectory_group"
    shares.index.name = "trajectory_group"
    return counts.reset_index(), shares.reset_index()


def build_overview_parts(sample_name: str, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sample_description = pd.DataFrame(
        {
            "sample_name": [sample_name],
            "row_count": [len(df)],
            "unique_firms": [df["nip"].nunique()],
        }
    )
    descriptives = build_growth_descriptives(df)
    decline_growth = build_decline_growth_shares(df)
    return sample_description, descriptives, decline_growth


def build_firm_trajectories(df: pd.DataFrame) -> pd.DataFrame:
    output = df.loc[df["has_complete_trajectory"] == 1].copy()
    output["Foreign"] = get_ownership_series(output)
    columns = [
        "nip",
        "company",
        "sector_en",
        "Foreign",
        "in_rank_2019",
        "manufacturing",
        "growth_real_P1",
        "growth_real_P2",
        "growth_real_P3",
        "P1_sign",
        "P2_sign",
        "P3_sign",
        "trajectory_3step",
        "trajectory_group",
        "index_2019",
        "index_2020",
        "index_2022",
        "index_2024",
    ]
    available_columns = [column for column in columns if column in output.columns]
    return output.loc[:, available_columns].sort_values("nip", kind="mergesort").reset_index(drop=True)


def sheet_name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def write_workbook(output_path: Path, sample_frames: dict[str, pd.DataFrame], firm_trajectories: pd.DataFrame) -> list[str]:
    written_sheets: list[str] = []

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sample_name, df in sample_frames.items():
            prefix = SAMPLE_SPECS[sample_name]["sheet_prefix"]
            sample_description, descriptives, decline_growth = build_overview_parts(sample_name, df)

            overview_sheet = sheet_name(prefix, "Overview")
            sample_description.to_excel(writer, sheet_name=overview_sheet, index=False, startrow=0)
            descriptives.to_excel(writer, sheet_name=overview_sheet, index=False, startrow=len(sample_description) + 3)
            decline_growth.to_excel(
                writer,
                sheet_name=overview_sheet,
                index=False,
                startrow=len(sample_description) + len(descriptives) + 7,
            )
            written_sheets.append(overview_sheet)

            traj3_sheet = sheet_name(prefix, "Traj3Step")
            build_trajectory_frequency(df, "trajectory_3step").to_excel(writer, sheet_name=traj3_sheet, index=False)
            written_sheets.append(traj3_sheet)

            traj_group_sheet = sheet_name(prefix, "TrajGroup")
            build_trajectory_frequency(df, "trajectory_group").to_excel(writer, sheet_name=traj_group_sheet, index=False)
            written_sheets.append(traj_group_sheet)

            growth_group_sheet = sheet_name(prefix, "GrowthByGroup")
            build_growth_by_group(df).to_excel(writer, sheet_name=growth_group_sheet, index=False)
            written_sheets.append(growth_group_sheet)

            path_sheet = sheet_name(prefix, "PathIndex")
            build_path_index_summary(df).to_excel(writer, sheet_name=path_sheet, index=False)
            written_sheets.append(path_sheet)

            sector_counts, sector_shares = build_crosstab(df, "sector_en")
            sector_sheet = sheet_name(prefix, "BySector")
            sector_counts.to_excel(writer, sheet_name=sector_sheet, index=False, startrow=0)
            sector_shares.to_excel(writer, sheet_name=sector_sheet, index=False, startrow=len(sector_counts) + 3)
            written_sheets.append(sector_sheet)

            owner_df = df.copy()
            owner_df["ownership"] = get_ownership_series(owner_df)
            owner_counts, owner_shares = build_crosstab(owner_df, "ownership")
            owner_sheet = sheet_name(prefix, "ByOwner")
            owner_counts.to_excel(writer, sheet_name=owner_sheet, index=False, startrow=0)
            owner_shares.to_excel(writer, sheet_name=owner_sheet, index=False, startrow=len(owner_counts) + 3)
            written_sheets.append(owner_sheet)

        if not firm_trajectories.empty:
            firm_trajectories.to_excel(writer, sheet_name="Firm_Trajectories", index=False)
            written_sheets.append("Firm_Trajectories")

    return written_sheets


def print_validation(df: pd.DataFrame, sample_frames: dict[str, pd.DataFrame], written_sheets: list[str], workbook_path: Path, firm_sheet_written: bool) -> None:
    print("Trajectory analysis complete")
    print(f"Input row count: {len(df):,}")
    print(f"Number of firms with has_complete_trajectory == 1: {int(df['has_complete_trajectory'].sum()):,}")
    print("Sample counts:")
    for sample_name, sample_df in sample_frames.items():
        print(f"{sample_name}: {sample_df['nip'].nunique():,}")
    print("trajectory_3step categories by sample:")
    for sample_name, sample_df in sample_frames.items():
        categories = sorted(sample_df["trajectory_3step"].dropna().astype(str).unique().tolist())
        print(f"{sample_name}: {categories}")
    print("trajectory_group categories by sample:")
    for sample_name, sample_df in sample_frames.items():
        categories = sorted(sample_df["trajectory_group"].dropna().astype(str).unique().tolist())
        print(f"{sample_name}: {categories}")
    print(f"Firm_Trajectories sheet written: {firm_sheet_written}")
    print(f"Final workbook sheet names: {written_sheets}")
    print(f"Final workbook path: {workbook_path.resolve()}")


def run_trajectory_analysis(input_path: Path = INPUT_PATH, output_path: Path = OUTPUT_XLSX_PATH) -> list[str]:
    df = load_input_data(input_path).copy()
    validate_input(df)

    sample_frames = {sample_name: build_sample(df, sample_name) for sample_name in SAMPLE_SPECS}
    firm_trajectories = build_firm_trajectories(df)
    written_sheets = write_workbook(output_path, sample_frames, firm_trajectories)
    print_validation(df, sample_frames, written_sheets, output_path, not firm_trajectories.empty)
    return written_sheets


if __name__ == "__main__":
    run_trajectory_analysis()
