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
    "sector_en",
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
    "sector_en",
    "manufacturing",
    "owner_type",
    "owner",
    "owner_num",
    "gpw",
    "city",
    "legal_form",
]
REAL_GROWTH_COLUMNS = ["rgrowth_P1", "rgrowth_P2", "rgrowth_P3"]
REAL_GROWTH_LOG_COLUMNS = ["rgrowth_log_P1", "rgrowth_log_P2", "rgrowth_log_P3"]
REAL_GROWTH_LOG_ANN_COLUMNS = ["rgrowth_log_ann_P1", "rgrowth_log_ann_P2", "rgrowth_log_ann_P3"]
REAL_LAG_COLUMNS = [
    "lag_rgrowth_P2",
    "lag_rgrowth_P3",
    "lag_rgrowth_log_P2",
    "lag_rgrowth_log_P3",
    "lag_rgrowth_log_ann_P2",
    "lag_rgrowth_log_ann_P3",
]
NOMINAL_GROWTH_COLUMNS = ["ngrowth_P1", "ngrowth_P2", "ngrowth_P3"]
NOMINAL_GROWTH_LOG_COLUMNS = ["ngrowth_log_P1", "ngrowth_log_P2", "ngrowth_log_P3"]
NOMINAL_GROWTH_LOG_ANN_COLUMNS = ["ngrowth_log_ann_P1", "ngrowth_log_ann_P2", "ngrowth_log_ann_P3"]
NOMINAL_LAG_COLUMNS = [
    "lag_ngrowth_P2",
    "lag_ngrowth_P3",
    "lag_ngrowth_log_P2",
    "lag_ngrowth_log_P3",
    "lag_ngrowth_log_ann_P2",
    "lag_ngrowth_log_ann_P3",
]
REAL_AVAILABILITY_COLUMNS = ["has_rP1_data", "has_rP2_data", "has_rP3_data"]
NOMINAL_AVAILABILITY_COLUMNS = ["has_nP1_data", "has_nP2_data", "has_nP3_data"]
REAL_TRAJECTORY_COLUMNS = [
    "has_complete_rtrajectory",
    "rP1_sign",
    "rP2_sign",
    "rP3_sign",
    "rtrajectory_3step",
    "rtrajectory_label",
    "rtrajectory_group",
    "rindex_2019",
    "rindex_2020",
    "rindex_2022",
    "rindex_2024",
]
NOMINAL_TRAJECTORY_COLUMNS = [
    "has_complete_ntrajectory",
    "nP1_sign",
    "nP2_sign",
    "nP3_sign",
    "ntrajectory_3step",
    "ntrajectory_label",
    "ntrajectory_group",
    "nindex_2019",
    "nindex_2020",
    "nindex_2022",
    "nindex_2024",
]
SGROWTH_COLUMNS = [
    "SGrowth_NR",
    "rgrowth_ann_2019_2024",
    "rgrowth_log_ann_2019_2024",
    "ngrowth_ann_2019_2024",
    "ngrowth_log_ann_2019_2024",
]
PERFORMANCE_COLUMNS = [
    "RPerf_Q_all",
    "RPerf_Q_rank2019",
    "RPerf_Q_manu2019",
    "NPerf_Q_all",
    "NPerf_Q_rank2019",
    "NPerf_Q_manu2019",
]
OLD_AMBIGUOUS_COLUMNS = [
    "growth_real_P1",
    "growth_real_P2",
    "growth_real_P3",
    "growth_log_P1",
    "growth_log_P2",
    "growth_log_P3",
    "growth_log_ann_P1",
    "growth_log_ann_P2",
    "growth_log_ann_P3",
    "lag_growth_real_P2",
    "lag_growth_real_P3",
    "lag_growth_log_P2",
    "lag_growth_log_P3",
    "lag_growth_log_ann_P2",
    "lag_growth_log_ann_P3",
    "has_P1_data",
    "has_P2_data",
    "has_P3_data",
    "has_complete_trajectory",
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
    "D-G-G": "Reversal",
    "D-D-G": "Reversal",
    "G-G-G": "Consistent growth",
    "G-D-D": "Reversal",
    "D-G-D": "Reversal",
    "G-D-G": "Reversal",
    "G-G-D": "Reversal",
}


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


def safe_positive_growth_ratio(start: pd.Series, end: pd.Series) -> pd.Series:
    valid = start.notna() & end.notna() & start.gt(0) & end.gt(0)
    output = pd.Series(np.nan, index=start.index, dtype=float)
    output.loc[valid] = end.loc[valid] / start.loc[valid] - 1
    return output


def safe_log_growth(start: pd.Series, end: pd.Series) -> pd.Series:
    valid = start.notna() & end.notna() & start.gt(0) & end.gt(0)
    output = pd.Series(np.nan, index=start.index, dtype=float)
    output.loc[valid] = np.log(end.loc[valid]) - np.log(start.loc[valid])
    return output


def safe_annualised_growth(start: pd.Series, end: pd.Series, years: int) -> pd.Series:
    valid = start.notna() & end.notna() & start.gt(0) & end.gt(0)
    output = pd.Series(np.nan, index=start.index, dtype=float)
    output.loc[valid] = (end.loc[valid] / start.loc[valid]) ** (1 / years) - 1
    return output


def classify_sgrowth_nr(
    sales_start: pd.Series,
    sales_end: pd.Series,
    sales_real_start: pd.Series,
    sales_real_end: pd.Series,
    real_growth: pd.Series,
    nominal_growth: pd.Series,
) -> pd.Series:
    output = pd.Series(pd.NA, index=real_growth.index, dtype="string")
    valid = sales_start.gt(0) & sales_end.gt(0) & sales_real_start.gt(0) & sales_real_end.gt(0)

    output.loc[valid & real_growth.ge(0) & nominal_growth.gt(0.20)] = "R2"
    output.loc[valid & real_growth.ge(0) & nominal_growth.le(0.20)] = "R1"
    output.loc[valid & real_growth.lt(0) & nominal_growth.ge(0)] = "N1"
    output.loc[valid & nominal_growth.lt(0)] = "N2"

    return output


def build_sgrowth_valid_mask(
    sales_start: pd.Series,
    sales_end: pd.Series,
    sales_real_start: pd.Series,
    sales_real_end: pd.Series,
) -> pd.Series:
    return sales_start.gt(0) & sales_end.gt(0) & sales_real_start.gt(0) & sales_real_end.gt(0)


def calculate_quantile_thresholds(series: pd.Series, threshold_mask: pd.Series) -> tuple[float, float, int]:
    threshold_series = series.loc[threshold_mask & series.notna()].astype(float)
    if threshold_series.empty:
        return np.nan, np.nan, 0
    return (
        float(threshold_series.quantile(0.10)),
        float(threshold_series.quantile(0.90)),
        int(threshold_series.shape[0]),
    )


def classify_with_thresholds(series: pd.Series, valid_mask: pd.Series, p10: float, p90: float) -> pd.Series:
    output = pd.Series(pd.NA, index=series.index, dtype="string")
    if pd.isna(p10) or pd.isna(p90):
        return output

    eligible = valid_mask & series.notna()
    output.loc[eligible & series.le(p10)] = "Bottom 10%"
    output.loc[eligible & series.ge(p90)] = "Top 10%"
    output.loc[eligible & series.gt(p10) & series.lt(0)] = "Moderate decline"
    output.loc[eligible & series.ge(0) & series.lt(p90)] = "Moderate growth"
    return output


def classify_quantile_performance(
    series: pd.Series,
    valid_mask: pd.Series,
    threshold_mask: pd.Series,
) -> tuple[pd.Series, dict[str, float | int]]:
    p10, p90, n_used = calculate_quantile_thresholds(series, threshold_mask)
    classified = classify_with_thresholds(series, valid_mask, p10, p90)
    return classified, {"p10": p10, "p90": p90, "n_used_for_threshold": n_used}


def validate_input(df: pd.DataFrame) -> None:
    required_columns = {KEY_COLUMN, YEAR_COLUMN, "sales_real", "sales", *START_COVARIATE_BASE_COLUMNS}
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


def build_sales_wide(df: pd.DataFrame, value_column: str, value_prefix: str) -> pd.DataFrame:
    years = sorted({details["start"] for details in PERIODS.values()} | {details["end"] for details in PERIODS.values()})
    output = build_year_extract(df, years[0], [value_column])[[KEY_COLUMN]].copy()

    for year in years:
        year_frame = build_year_extract(df, year, [value_column]).rename(columns={value_column: f"{value_prefix}_sales_{year}"})
        output = output.merge(year_frame, on=KEY_COLUMN, how="outer", validate="one_to_one")

    return output


def build_sgrowth_nr(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, int, int, int]:
    real_annual = build_sales_wide(df, "sales_real", "real")
    nominal_annual = build_sales_wide(df, "sales", "nominal")
    descriptors_2019 = build_year_extract(df, 2019, ["in_rank_2019", "manufacturing"])
    annual = real_annual.merge(nominal_annual, on=KEY_COLUMN, how="outer", validate="one_to_one")
    annual = annual.merge(descriptors_2019, on=KEY_COLUMN, how="left", validate="one_to_one")

    real_growth_ann = safe_annualised_growth(annual["real_sales_2019"], annual["real_sales_2024"], years=5)
    nominal_growth_ann = safe_annualised_growth(annual["nominal_sales_2019"], annual["nominal_sales_2024"], years=5)
    real_growth_log_ann = safe_log_growth(annual["real_sales_2019"], annual["real_sales_2024"]) / 5
    nominal_growth_log_ann = safe_log_growth(annual["nominal_sales_2019"], annual["nominal_sales_2024"]) / 5
    valid = build_sgrowth_valid_mask(
        annual["nominal_sales_2019"],
        annual["nominal_sales_2024"],
        annual["real_sales_2019"],
        annual["real_sales_2024"],
    )
    rank2019_mask = valid & annual["in_rank_2019"].eq(1)
    manu2019_mask = rank2019_mask & annual["manufacturing"].eq(1)

    output = annual[[KEY_COLUMN]].copy()
    output["rgrowth_ann_2019_2024"] = real_growth_ann
    output["rgrowth_log_ann_2019_2024"] = real_growth_log_ann
    output["ngrowth_ann_2019_2024"] = nominal_growth_ann
    output["ngrowth_log_ann_2019_2024"] = nominal_growth_log_ann
    output["SGrowth_NR"] = classify_sgrowth_nr(
        annual["nominal_sales_2019"],
        annual["nominal_sales_2024"],
        annual["real_sales_2019"],
        annual["real_sales_2024"],
        real_growth_ann,
        nominal_growth_ann,
    )

    threshold_rows = []
    performance_specs = [
        ("real", "all", "RPerf_Q_all", real_growth_ann, valid),
        ("real", "rank2019", "RPerf_Q_rank2019", real_growth_ann, rank2019_mask),
        ("real", "manu2019", "RPerf_Q_manu2019", real_growth_ann, manu2019_mask),
        ("nominal", "all", "NPerf_Q_all", nominal_growth_ann, valid),
        ("nominal", "rank2019", "NPerf_Q_rank2019", nominal_growth_ann, rank2019_mask),
        ("nominal", "manu2019", "NPerf_Q_manu2019", nominal_growth_ann, manu2019_mask),
    ]
    for growth_family, benchmark, column_name, growth_series, threshold_mask in performance_specs:
        classified, threshold_info = classify_quantile_performance(growth_series, valid, threshold_mask)
        output[column_name] = classified
        threshold_rows.append(
            {
                "growth_type": growth_family,
                "benchmark_group": benchmark,
                "performance_column": column_name,
                "p10": threshold_info["p10"],
                "p90": threshold_info["p90"],
                "n_used_for_threshold": threshold_info["n_used_for_threshold"],
            }
        )

    performance_thresholds = pd.DataFrame(threshold_rows)[
        ["growth_type", "benchmark_group", "performance_column", "p10", "p90", "n_used_for_threshold"]
    ].sort_values(["growth_type", "benchmark_group"], kind="mergesort").reset_index(drop=True)
    if performance_thresholds["n_used_for_threshold"].eq(0).any():
        raise ValueError("Quantile thresholds computed on empty sample for some benchmark.")

    valid_real_count = int(real_growth_ann.notna().sum())
    valid_nominal_count = int(nominal_growth_ann.notna().sum())
    collapse_count = int(
        (annual["nominal_sales_2019"].gt(0) & annual["nominal_sales_2024"].notna() & annual["nominal_sales_2024"].le(0)).sum()
    )

    return output, performance_thresholds, valid_real_count, valid_nominal_count, collapse_count


def build_period_outcomes_from_wide(
    wide_df: pd.DataFrame,
    value_prefix: str,
    output_prefix: str,
    availability_prefix: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    output = wide_df[[KEY_COLUMN]].copy()
    blocked_log_counts: dict[str, int] = {}

    for period_name, period in PERIODS.items():
        start_year = period["start"]
        end_year = period["end"]
        duration = period["years"]

        start = wide_df[f"{value_prefix}_sales_{start_year}"]
        end = wide_df[f"{value_prefix}_sales_{end_year}"]
        has_valid_data = start.notna() & end.notna() & start.gt(0) & end.gt(0)
        growth = safe_positive_growth_ratio(start, end)
        growth_log = safe_log_growth(start, end)

        output[f"{output_prefix}_{period_name}"] = growth
        output[f"{output_prefix}_log_{period_name}"] = growth_log
        output[f"{output_prefix}_log_ann_{period_name}"] = growth_log / duration
        output[f"has_{availability_prefix}{period_name}_data"] = has_valid_data.astype("Int64")

        blocked_log_counts[period_name] = int(((start.notna() & start.le(0)) | (end.notna() & end.le(0))).sum())

    return output, blocked_log_counts


def build_lagged_growth(outcomes: pd.DataFrame, output_prefix: str) -> pd.DataFrame:
    output = outcomes[[KEY_COLUMN]].copy()
    output[f"lag_{output_prefix}_P2"] = outcomes[f"{output_prefix}_P1"]
    output[f"lag_{output_prefix}_P3"] = outcomes[f"{output_prefix}_P2"]
    output[f"lag_{output_prefix}_log_P2"] = outcomes[f"{output_prefix}_log_P1"]
    output[f"lag_{output_prefix}_log_P3"] = outcomes[f"{output_prefix}_log_P2"]
    output[f"lag_{output_prefix}_log_ann_P2"] = outcomes[f"{output_prefix}_log_ann_P1"]
    output[f"lag_{output_prefix}_log_ann_P3"] = outcomes[f"{output_prefix}_log_ann_P2"]
    return output


def build_trajectory_descriptors(outcomes: pd.DataFrame, growth_prefix: str, trajectory_prefix: str) -> pd.DataFrame:
    output = outcomes[[KEY_COLUMN]].copy()
    growth_columns = [f"{growth_prefix}_{period_name}" for period_name in PERIODS]
    output[f"has_complete_{trajectory_prefix}trajectory"] = outcomes[growth_columns].notna().all(axis=1).astype("Int64")

    for period_name in PERIODS:
        growth_column = f"{growth_prefix}_{period_name}"
        sign_column = f"{trajectory_prefix}{period_name}_sign"
        output[sign_column] = np.where(
            outcomes[growth_column].notna(),
            np.where(outcomes[growth_column] < 0, "D", "G"),
            pd.NA,
        )
        output[sign_column] = pd.Series(output[sign_column], index=output.index, dtype="string")

    sign_columns = [f"{trajectory_prefix}{period_name}_sign" for period_name in PERIODS]
    complete_signs = output[sign_columns].notna().all(axis=1)
    output[f"{trajectory_prefix}trajectory_3step"] = pd.Series(pd.NA, index=output.index, dtype="string")
    output.loc[complete_signs, f"{trajectory_prefix}trajectory_3step"] = output.loc[complete_signs, sign_columns].agg("-".join, axis=1)
    output[f"{trajectory_prefix}trajectory_label"] = (
        output[f"{trajectory_prefix}trajectory_3step"]
        .map(TRAJECTORY_LABEL_MAP)
        .astype("string")
    )
    output[f"{trajectory_prefix}trajectory_group"] = output[f"{trajectory_prefix}trajectory_3step"].map(TRAJECTORY_GROUP_MAP).astype("string")

    output[f"{trajectory_prefix}index_2019"] = np.where(output[f"has_complete_{trajectory_prefix}trajectory"] == 1, 100.0, np.nan)
    output[f"{trajectory_prefix}index_2020"] = output[f"{trajectory_prefix}index_2019"] * (1 + outcomes[f"{growth_prefix}_P1"])
    output[f"{trajectory_prefix}index_2022"] = output[f"{trajectory_prefix}index_2020"] * (1 + outcomes[f"{growth_prefix}_P2"])
    output[f"{trajectory_prefix}index_2024"] = output[f"{trajectory_prefix}index_2022"] * (1 + outcomes[f"{growth_prefix}_P3"])

    return output


def final_column_order(df: pd.DataFrame) -> list[str]:
    ordered_columns = []

    for block in [
        BLOCK_1_COLUMNS,
        BLOCK_2_COLUMNS,
        REAL_GROWTH_COLUMNS,
        REAL_GROWTH_LOG_COLUMNS,
        REAL_GROWTH_LOG_ANN_COLUMNS,
        REAL_LAG_COLUMNS,
        NOMINAL_GROWTH_COLUMNS,
        NOMINAL_GROWTH_LOG_COLUMNS,
        NOMINAL_GROWTH_LOG_ANN_COLUMNS,
        NOMINAL_LAG_COLUMNS,
    ]:
        ordered_columns.extend([column for column in block if column in df.columns])

    for period_name in PERIODS:
        ordered_columns.extend(
            [f"{column}_start_{period_name}" for column in START_COVARIATE_BASE_COLUMNS if f"{column}_start_{period_name}" in df.columns]
        )

    ordered_columns.extend([column for column in REAL_AVAILABILITY_COLUMNS if column in df.columns])
    ordered_columns.extend([column for column in NOMINAL_AVAILABILITY_COLUMNS if column in df.columns])
    ordered_columns.extend([column for column in REAL_TRAJECTORY_COLUMNS if column in df.columns])
    ordered_columns.extend([column for column in NOMINAL_TRAJECTORY_COLUMNS if column in df.columns])
    ordered_columns.extend([column for column in SGROWTH_COLUMNS if column in df.columns])
    ordered_columns.extend([column for column in PERFORMANCE_COLUMNS if column in df.columns])
    return ordered_columns


def validate_output_schema(df: pd.DataFrame) -> None:
    if df.duplicated(KEY_COLUMN).any():
        raise ValueError("Output contains duplicate firm rows.")
    if df.columns.duplicated().any():
        duplicate_columns = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(f"Output contains duplicate columns: {duplicate_columns}")

    legacy_columns_present = sorted(set(OLD_AMBIGUOUS_COLUMNS).intersection(df.columns))
    if legacy_columns_present:
        raise ValueError(f"Output still contains old ambiguous columns: {legacy_columns_present}")

    required_families = [
        *REAL_GROWTH_COLUMNS,
        *REAL_GROWTH_LOG_COLUMNS,
        *REAL_GROWTH_LOG_ANN_COLUMNS,
        *NOMINAL_GROWTH_COLUMNS,
        *NOMINAL_GROWTH_LOG_COLUMNS,
        *NOMINAL_GROWTH_LOG_ANN_COLUMNS,
        *REAL_TRAJECTORY_COLUMNS,
        *NOMINAL_TRAJECTORY_COLUMNS,
        *REAL_AVAILABILITY_COLUMNS,
        *NOMINAL_AVAILABILITY_COLUMNS,
        "SGrowth_NR",
        "rgrowth_ann_2019_2024",
        "rgrowth_log_ann_2019_2024",
        "ngrowth_ann_2019_2024",
        "ngrowth_log_ann_2019_2024",
        *PERFORMANCE_COLUMNS,
    ]
    missing_columns = [column for column in required_families if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Output is missing required columns: {missing_columns}")


def build_period_dataset(input_path: Path = INPUT_PATH) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, int]], int, int, int, int]:
    annual = load_input_data(input_path)
    validate_input(annual)
    input_row_count = len(annual)

    stable = build_stable_descriptors(annual)
    start_covariates = build_start_covariates(annual)
    real_sales_wide = build_sales_wide(annual, "sales_real", "real")
    nominal_sales_wide = build_sales_wide(annual, "sales", "nominal")
    real_outcomes, real_blocked_log_counts = build_period_outcomes_from_wide(real_sales_wide, "real", "rgrowth", "r")
    nominal_outcomes, nominal_blocked_log_counts = build_period_outcomes_from_wide(nominal_sales_wide, "nominal", "ngrowth", "n")
    real_lags = build_lagged_growth(real_outcomes, "rgrowth")
    nominal_lags = build_lagged_growth(nominal_outcomes, "ngrowth")
    real_trajectory = build_trajectory_descriptors(real_outcomes, "rgrowth", "r")
    nominal_trajectory = build_trajectory_descriptors(nominal_outcomes, "ngrowth", "n")
    sgrowth_nr, performance_thresholds, valid_real_growth_count, valid_nominal_growth_count, collapse_count = build_sgrowth_nr(annual)

    output = stable.merge(real_outcomes, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(real_lags, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(nominal_outcomes, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(nominal_lags, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(start_covariates, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(real_trajectory, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(nominal_trajectory, on=KEY_COLUMN, how="outer", validate="one_to_one")
    output = output.merge(sgrowth_nr, on=KEY_COLUMN, how="outer", validate="one_to_one")

    output = output.loc[:, final_column_order(output)].sort_values(KEY_COLUMN, kind="mergesort").reset_index(drop=True)
    validate_output_schema(output)

    return (
        output,
        performance_thresholds,
        {"real": real_blocked_log_counts, "nominal": nominal_blocked_log_counts},
        input_row_count,
        valid_real_growth_count,
        valid_nominal_growth_count,
        collapse_count,
    )


def write_outputs(df: pd.DataFrame, performance_thresholds: pd.DataFrame, parquet_path: Path, xlsx_path: Path) -> None:
    df.to_parquet(parquet_path, index=False)
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        performance_thresholds.to_excel(writer, index=False, sheet_name="Performance_Thresholds")


def print_missing_counts(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        print(f"{column}: {int(df[column].isna().sum()):,}")


def print_distribution_summary(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        series = df[column]
        print(f"{column}: min={series.min(skipna=True):.6f}, median={series.median(skipna=True):.6f}, max={series.max(skipna=True):.6f}")


def print_frequency_table(df: pd.DataFrame, column: str) -> None:
    print(f"{column} frequency table:")
    print(df[column].value_counts(dropna=False).to_string())
    print(f"Missing {column}: {int(df[column].isna().sum()):,}")
    print(f"{column} percentage distribution:")
    print(df[column].value_counts(dropna=False, normalize=True).mul(100).round(2).to_string())


def print_build_summary(
    df: pd.DataFrame,
    performance_thresholds: pd.DataFrame,
    input_row_count: int,
    blocked_log_counts: dict[str, dict[str, int]],
    valid_real_growth_count: int,
    valid_nominal_growth_count: int,
    collapse_count: int,
) -> None:
    real_growth_columns = REAL_GROWTH_COLUMNS
    nominal_growth_columns = NOMINAL_GROWTH_COLUMNS
    start_covariate_check = {
        "P1 start covariates": all(f"{column}_start_P1" in df.columns for column in START_COVARIATE_BASE_COLUMNS),
        "P2 start covariates": all(f"{column}_start_P2" in df.columns for column in START_COVARIATE_BASE_COLUMNS),
        "P3 start covariates": all(f"{column}_start_P3" in df.columns for column in START_COVARIATE_BASE_COLUMNS),
        "real annualised log growth variables": all(column in df.columns for column in REAL_GROWTH_LOG_ANN_COLUMNS),
        "nominal annualised log growth variables": all(column in df.columns for column in NOMINAL_GROWTH_LOG_ANN_COLUMNS),
        "real period availability flags": all(column in df.columns for column in REAL_AVAILABILITY_COLUMNS),
        "nominal period availability flags": all(column in df.columns for column in NOMINAL_AVAILABILITY_COLUMNS),
    }

    print("Period dataset build complete")
    print(f"Input row count: {input_row_count:,}")
    print(f"Output row count: {len(df):,}")
    print(f"Unique firms: {df[KEY_COLUMN].nunique():,}")
    print(f"One row per nip: {not df.duplicated(KEY_COLUMN).any()}")
    print(f"Duplicate columns present: {df.columns.duplicated().any()}")
    print(f"sector_en present in output: {'sector_en' in df.columns}")
    if "sector_en" in df.columns:
        print(f"Unique sector_en values: {sorted(df['sector_en'].dropna().astype(str).unique().tolist())}")
    print(f"Count of firms with has_complete_rtrajectory = 1: {int(df['has_complete_rtrajectory'].sum()):,}")
    print(f"Count of firms with has_complete_ntrajectory = 1: {int(df['has_complete_ntrajectory'].sum()):,}")
    print("Old ambiguous columns present:")
    legacy_columns_present = [column for column in OLD_AMBIGUOUS_COLUMNS if column in df.columns]
    print(legacy_columns_present if legacy_columns_present else "None")
    print("Real sign values:")
    for column in ["rP1_sign", "rP2_sign", "rP3_sign"]:
        print(f"{column}: {sorted(df[column].dropna().astype(str).unique().tolist())}")
    print("Nominal sign values:")
    for column in ["nP1_sign", "nP2_sign", "nP3_sign"]:
        print(f"{column}: {sorted(df[column].dropna().astype(str).unique().tolist())}")
    print("Real trajectory frequency table:")
    print(df["rtrajectory_3step"].value_counts(dropna=False).to_string())
    print("Real trajectory label frequency table:")
    print(df["rtrajectory_label"].value_counts(dropna=False).to_string())
    print("Real trajectory group frequency table:")
    print(df["rtrajectory_group"].value_counts(dropna=False).to_string())
    print("Nominal trajectory frequency table:")
    print(df["ntrajectory_3step"].value_counts(dropna=False).to_string())
    print("Nominal trajectory label frequency table:")
    print(df["ntrajectory_label"].value_counts(dropna=False).to_string())
    print("Nominal trajectory group frequency table:")
    print(df["ntrajectory_group"].value_counts(dropna=False).to_string())
    print_frequency_table(df, "SGrowth_NR")
    print(f"Missing SGrowth_NR: {int(df['SGrowth_NR'].isna().sum()):,}")
    print(f"N2 count: {int(df['SGrowth_NR'].eq('N2').sum()):,}")
    for column in PERFORMANCE_COLUMNS:
        print_frequency_table(df, column)
    for (growth_type, benchmark_group), group in performance_thresholds.groupby(["growth_type", "benchmark_group"], sort=True):
        print(f"\nThresholds: {growth_type} | {benchmark_group}")
        print(group[["performance_column", "p10", "p90", "n_used_for_threshold"]].to_string(index=False))
    print(f"Firms with sales_2024 <= 0 and sales_2019 > 0: {collapse_count:,}")
    print(f"Valid annualised real sales growth (2019-2024): {valid_real_growth_count:,}")
    print(f"Valid annualised nominal sales growth (2019-2024): {valid_nominal_growth_count:,}")
    print("Missing annualised 2019-2024 growth counts:")
    print_missing_counts(
        df,
        [
            "rgrowth_ann_2019_2024",
            "rgrowth_log_ann_2019_2024",
            "ngrowth_ann_2019_2024",
            "ngrowth_log_ann_2019_2024",
        ],
    )
    print("Annualised 2019-2024 growth distribution summary:")
    print_distribution_summary(
        df,
        [
            "rgrowth_ann_2019_2024",
            "rgrowth_log_ann_2019_2024",
            "ngrowth_ann_2019_2024",
            "ngrowth_log_ann_2019_2024",
        ],
    )
    print("Real availability counts:")
    for column in REAL_AVAILABILITY_COLUMNS:
        print(f"{column}: {int(df[column].sum()):,}")
    print("Nominal availability counts:")
    for column in NOMINAL_AVAILABILITY_COLUMNS:
        print(f"{column}: {int(df[column].sum()):,}")
    print("Missing counts:")
    print("Real growth missing counts:")
    print_missing_counts(df, REAL_GROWTH_COLUMNS + REAL_GROWTH_LOG_COLUMNS + REAL_GROWTH_LOG_ANN_COLUMNS)
    print("Nominal growth missing counts:")
    print_missing_counts(df, NOMINAL_GROWTH_COLUMNS + NOMINAL_GROWTH_LOG_COLUMNS + NOMINAL_GROWTH_LOG_ANN_COLUMNS)
    print("Real growth distribution summary:")
    print_distribution_summary(df, real_growth_columns + REAL_GROWTH_LOG_ANN_COLUMNS)
    print("Nominal growth distribution summary:")
    print_distribution_summary(df, nominal_growth_columns + NOMINAL_GROWTH_LOG_ANN_COLUMNS)
    print("Real index summary statistics:")
    print_distribution_summary(df, ["rindex_2019", "rindex_2020", "rindex_2022", "rindex_2024"])
    print("Nominal index summary statistics:")
    print_distribution_summary(df, ["nindex_2019", "nindex_2020", "nindex_2022", "nindex_2024"])
    print("Non-positive real sales values preventing log-growth:")
    for period_name in PERIODS:
        print(f"{period_name}: {blocked_log_counts['real'][period_name]:,}")
    print("Non-positive nominal sales values preventing log-growth:")
    for period_name in PERIODS:
        print(f"{period_name}: {blocked_log_counts['nominal'][period_name]:,}")
    print("Block existence:")
    for label, exists in start_covariate_check.items():
        print(f"{label}: {exists}")
    print("Final columns:")
    for column in df.columns:
        print(column)
    print(f"Final column count: {len(df.columns)}")


if __name__ == "__main__":
    (
        period_dataset,
        performance_thresholds,
        blocked_log_counts,
        input_row_count,
        valid_real_growth_count,
        valid_nominal_growth_count,
        collapse_count,
    ) = build_period_dataset()
    write_outputs(period_dataset, performance_thresholds, OUTPUT_PARQUET_PATH, OUTPUT_XLSX_PATH)
    print_build_summary(
        period_dataset,
        performance_thresholds,
        input_row_count,
        blocked_log_counts,
        valid_real_growth_count,
        valid_nominal_growth_count,
        collapse_count,
    )
