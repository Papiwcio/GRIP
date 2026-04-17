from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path("Data_core_2019-2024.parquet")
OUTPUT_PARQUET_PATH = Path("Data_period_2019-2024.parquet")
OUTPUT_XLSX_PATH = Path("Data_period_2019-2024.xlsx")

KEY_COLUMN = "nip"
YEAR_COLUMN = "year"

PERIODS = {
    "P1": {"start": 2019, "end": 2020, "years": 1},
    "P2": {"start": 2020, "end": 2022, "years": 2},
    "P3": {"start": 2022, "end": 2024, "years": 2},
}

STABLE_DESCRIPTOR_CANDIDATES = [
    "company",
    "rank_2019",
    "in_rank_2019",
    "pkd",
    "pkd_description",
    "sector",
    "manufacturing",
    "owner_type",
    "owner",
    "owner_num",
    "gpw",
    "city",
    "legal_form",
]

START_COVARIATE_BASE_COLUMNS = [
    "ln_sales",
    "ln_total_assets",
    "ln_employment",
    "profit_margin",
    "operating_margin",
    "export_ratio",
    "asset_turnover",
    "capital_ratio",
    "equity_multiplier",
    "roa",
    "roe",
    "depreciation_ratio",
    "wage_intensity",
    "sales_per_employee",
    "assets_per_employee",
]

BLOCK_1_COLUMNS = ["nip", "company", "rank_2019", "in_rank_2019"]
BLOCK_2_COLUMNS = [
    "pkd",
    "pkd_description",
    "sector",
    "manufacturing",
    "owner_type",
    "owner",
    "owner_num",
    "gpw",
    "city",
    "legal_form",
]
BLOCK_3_COLUMNS = ["growth_real_P1", "growth_real_P2", "growth_real_P3"]
BLOCK_4_COLUMNS = ["growth_log_P1", "growth_log_P2", "growth_log_P3"]
BLOCK_5_COLUMNS = ["growth_log_ann_P1", "growth_log_ann_P2", "growth_log_ann_P3"]
BLOCK_6_COLUMNS = [
    "lag_growth_real_P2",
    "lag_growth_real_P3",
    "lag_growth_log_P2",
    "lag_growth_log_P3",
    "lag_growth_log_ann_P2",
    "lag_growth_log_ann_P3",
]
BLOCK_10_COLUMNS = ["has_P1_data", "has_P2_data", "has_P3_data"]


def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def first_non_missing(series: pd.Series):
    non_missing = series.dropna()
    if non_missing.empty:
        return pd.NA
    return non_missing.iloc[0]


def safe_growth_ratio(start: pd.Series, end: pd.Series) -> pd.Series:
    valid = start.notna() & end.notna() & start.ne(0)
    output = pd.Series(np.nan, index=start.index, dtype=float)
    output.loc[valid] = end.loc[valid] / start.loc[valid] - 1
    return output


def safe_log_growth(start: pd.Series, end: pd.Series) -> pd.Series:
    valid = start.notna() & end.notna() & start.gt(0) & end.gt(0)
    output = pd.Series(np.nan, index=start.index, dtype=float)
    output.loc[valid] = np.log(end.loc[valid]) - np.log(start.loc[valid])
    return output


def validate_input(df: pd.DataFrame) -> None:
    required_columns = {KEY_COLUMN, YEAR_COLUMN, "sales_real", *START_COVARIATE_BASE_COLUMNS}
    required_columns.update(column for column in STABLE_DESCRIPTOR_CANDIDATES if column not in {"gpw", "city", "legal_form"})
    missing = sorted(required_columns.difference(df.columns))
    if missing:
        raise ValueError(f"Input file is missing required columns: {missing}")

    if df.duplicated([KEY_COLUMN, YEAR_COLUMN]).any():
        raise ValueError("Input file contains duplicate (nip, year) rows.")


def build_stable_descriptors(df: pd.DataFrame) -> pd.DataFrame:
    available_columns = [column for column in STABLE_DESCRIPTOR_CANDIDATES if column in df.columns]
    stable = (
        df.sort_values([KEY_COLUMN, YEAR_COLUMN], kind="mergesort")
        .groupby(KEY_COLUMN, dropna=False)[available_columns]
        .agg(first_non_missing)
        .reset_index()
    )
    return stable


def build_year_extract(df: pd.DataFrame, year: int, columns: list[str]) -> pd.DataFrame:
    available_columns = [column for column in columns if column in df.columns]
    year_frame = df.loc[df[YEAR_COLUMN] == year, [KEY_COLUMN] + available_columns].copy()
    if year_frame.duplicated(KEY_COLUMN).any():
        raise ValueError(f"Multiple rows found for some firms in year {year}.")
    return year_frame


def build_start_covariates(df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for period_name, period in PERIODS.items():
        start_year = period["start"]
        frame = build_year_extract(df, start_year, START_COVARIATE_BASE_COLUMNS)
        rename_map = {column: f"{column}_start_{period_name}" for column in START_COVARIATE_BASE_COLUMNS if column in frame.columns}
        frames.append(frame.rename(columns=rename_map))

    output = frames[0]
    for frame in frames[1:]:
        output = output.merge(frame, on=KEY_COLUMN, how="outer", validate="one_to_one")

    return output


def build_sales_real_wide(df: pd.DataFrame) -> pd.DataFrame:
    years = sorted({details["start"] for details in PERIODS.values()} | {details["end"] for details in PERIODS.values()})
    sales_real = build_year_extract(df, years[0], ["sales_real"])[[KEY_COLUMN]].copy()

    for year in years:
        year_frame = build_year_extract(df, year, ["sales_real"]).rename(columns={"sales_real": f"sales_real_{year}"})
        sales_real = sales_real.merge(year_frame, on=KEY_COLUMN, how="outer", validate="one_to_one")

    return sales_real


def build_period_outcomes(sales_real_wide: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    output = sales_real_wide[[KEY_COLUMN]].copy()
    blocked_log_counts: dict[str, int] = {}

    for period_name, period in PERIODS.items():
        start_year = period["start"]
        end_year = period["end"]
        duration = period["years"]

        start = sales_real_wide[f"sales_real_{start_year}"]
        end = sales_real_wide[f"sales_real_{end_year}"]

        growth_real = safe_growth_ratio(start, end)
        growth_log = safe_log_growth(start, end)

        output[f"growth_real_{period_name}"] = growth_real
        output[f"growth_log_{period_name}"] = growth_log
        output[f"growth_log_ann_{period_name}"] = growth_log / duration
        output[f"has_{period_name}_data"] = (start.notna() & end.notna() & start.gt(0)).astype("Int64")

        blocked_log_counts[period_name] = int(((start.notna() & start.le(0)) | (end.notna() & end.le(0))).sum())

    return output, blocked_log_counts


def build_lagged_growth(outcomes: pd.DataFrame) -> pd.DataFrame:
    output = outcomes[[KEY_COLUMN]].copy()
    output["lag_growth_real_P2"] = outcomes["growth_real_P1"]
    output["lag_growth_real_P3"] = outcomes["growth_real_P2"]
    output["lag_growth_log_P2"] = outcomes["growth_log_P1"]
    output["lag_growth_log_P3"] = outcomes["growth_log_P2"]
    output["lag_growth_log_ann_P2"] = outcomes["growth_log_ann_P1"]
    output["lag_growth_log_ann_P3"] = outcomes["growth_log_ann_P2"]
    return output


def final_column_order(df: pd.DataFrame) -> list[str]:
    ordered_columns = []

    for block in [BLOCK_1_COLUMNS, BLOCK_2_COLUMNS, BLOCK_3_COLUMNS, BLOCK_4_COLUMNS, BLOCK_5_COLUMNS, BLOCK_6_COLUMNS]:
        ordered_columns.extend([column for column in block if column in df.columns])

    for period_name in PERIODS:
        ordered_columns.extend(
            [f"{column}_start_{period_name}" for column in START_COVARIATE_BASE_COLUMNS if f"{column}_start_{period_name}" in df.columns]
        )

    ordered_columns.extend([column for column in BLOCK_10_COLUMNS if column in df.columns])
    return ordered_columns


def build_period_dataset(input_path: Path = INPUT_PATH) -> tuple[pd.DataFrame, dict[str, int], int]:
    annual = load_input_data(input_path)
    validate_input(annual)
    input_row_count = len(annual)

    stable = build_stable_descriptors(annual)
    start_covariates = build_start_covariates(annual)
    sales_real_wide = build_sales_real_wide(annual)
    outcomes, blocked_log_counts = build_period_outcomes(sales_real_wide)
    lags = build_lagged_growth(outcomes)

    output = stable.merge(outcomes, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(lags, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(start_covariates, on=KEY_COLUMN, how="outer", validate="one_to_one")

    output = output.loc[:, final_column_order(output)].sort_values(KEY_COLUMN, kind="mergesort").reset_index(drop=True)

    if output.duplicated(KEY_COLUMN).any():
        raise ValueError("Output contains duplicate firm rows.")

    return output, blocked_log_counts, input_row_count


def write_outputs(df: pd.DataFrame, parquet_path: Path, xlsx_path: Path) -> None:
    df.to_parquet(parquet_path, index=False)
    df.to_excel(xlsx_path, index=False, engine="xlsxwriter")


def print_missing_counts(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        print(f"{column}: {int(df[column].isna().sum()):,}")


def print_distribution_summary(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        series = df[column]
        print(f"{column}: min={series.min(skipna=True):.6f}, median={series.median(skipna=True):.6f}, max={series.max(skipna=True):.6f}")


def print_build_summary(df: pd.DataFrame, input_row_count: int, blocked_log_counts: dict[str, int]) -> None:
    growth_real_columns = BLOCK_3_COLUMNS
    growth_log_columns = BLOCK_4_COLUMNS
    growth_log_ann_columns = BLOCK_5_COLUMNS
    start_covariate_check = {
        "P1 start covariates": all(f"{column}_start_P1" in df.columns for column in START_COVARIATE_BASE_COLUMNS),
        "P2 start covariates": all(f"{column}_start_P2" in df.columns for column in START_COVARIATE_BASE_COLUMNS),
        "P3 start covariates": all(f"{column}_start_P3" in df.columns for column in START_COVARIATE_BASE_COLUMNS),
        "annualised log growth variables": all(column in df.columns for column in BLOCK_5_COLUMNS),
        "period availability flags": all(column in df.columns for column in BLOCK_10_COLUMNS),
    }

    print("Period dataset build complete")
    print(f"Input row count: {input_row_count:,}")
    print(f"Output row count: {len(df):,}")
    print(f"Unique firms: {df[KEY_COLUMN].nunique():,}")
    print(f"One row per nip: {not df.duplicated(KEY_COLUMN).any()}")
    print("Availability counts:")
    for column in BLOCK_10_COLUMNS:
        print(f"{column}: {int(df[column].sum()):,}")
    print("Missing counts:")
    print_missing_counts(df, growth_real_columns + growth_log_columns + growth_log_ann_columns)
    print("Distribution summary:")
    print_distribution_summary(df, growth_real_columns + growth_log_ann_columns)
    print("Non-positive sales_real values preventing log-growth:")
    for period_name in PERIODS:
        print(f"{period_name}: {blocked_log_counts[period_name]:,}")
    print("Block existence:")
    for label, exists in start_covariate_check.items():
        print(f"{label}: {exists}")
    print("Final columns:")
    for column in df.columns:
        print(column)
    print(f"Final column count: {len(df.columns)}")


if __name__ == "__main__":
    period_dataset, blocked_log_counts, input_row_count = build_period_dataset()
    write_outputs(period_dataset, OUTPUT_PARQUET_PATH, OUTPUT_XLSX_PATH)
    print_build_summary(period_dataset, input_row_count, blocked_log_counts)
