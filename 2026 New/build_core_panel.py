from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path("Data_panel_2018-2024.parquet")
OUTPUT_PARQUET_PATH = Path("Data_core_2018-2024.parquet")
OUTPUT_XLSX_PATH = Path("Data_core_2018-2024.xlsx")

PRICE_INDEX_BY_YEAR = {
    # Statistics Poland annual CPI for 2019 versus 2018 was 1.023;
    # invert it to preserve the existing 2019 = 1.0 base.
    2018: 0.977517106549,
    2019: 1.000000000,
    2020: 1.034000000,
    2021: 1.086734000,
    2022: 1.243223696,
    2023: 1.384951196,
    2024: 1.434809439,
}

EXPECTED_YEARS = {2018, 2019, 2020, 2021, 2022, 2023, 2024}
P1_LAG_SOURCE_YEAR = 2018
MAIN_ANALYSIS_START_YEAR = 2019

KEY_COLUMNS = ["nip", "year"]

STATIC_COLUMNS = [
    "company",
    "rank_2019",
    "in_rank_2019",
    "pkd",
    "pkd_description",
    "manufacturing",
    "owner_type",
    "owner",
    "owner_num",
    "city",
    "regon",
    "krs",
    "legal_form",
    "sj",
]

ANNUAL_COLUMNS = [
    "sales",
    "operating_result",
    "profit_before_tax",
    "income_tax",
    "net_profit",
    "depreciation",
    "exports",
    "employment",
    "wages_total",
    "total_assets",
    "fixed_assets",
    "current_assets",
    "equity",
    "zobowiazania_i_rezerwy_na_zobowiazania",
    "zobowiazania_dlugoterminowe",
    "zobowiazania_krotkoterminow",
    "liabilities_provisions",
    "total_liabilities",
]

DERIVED_COLUMNS = [
    "price_index",
    "sales_real",
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
    "sales_growth_yoy",
    "sales_real_growth_yoy",
    "sales_log_growth_yoy",
    "has_sales",
    "has_assets",
    "has_employment",
]

DROP_COLUMNS = [
    "rank_2020",
    "rank_2021",
    "rank_2022",
    "rank_2023",
    "rank_2024",
]

CORE_COLUMNS = KEY_COLUMNS + STATIC_COLUMNS + ANNUAL_COLUMNS + DERIVED_COLUMNS
INPUT_REQUIRED_COLUMNS = [
    column
    for column in KEY_COLUMNS + STATIC_COLUMNS + ANNUAL_COLUMNS
    if column not in {"in_rank_2019", "owner", "owner_num", "manufacturing"}
] + DROP_COLUMNS

INTEGER_COLUMNS = ["year", "rank_2019", "in_rank_2019", "regon", "krs", "pkd", "owner_type"]
FLOAT_COLUMNS = [
    "sales",
    "operating_result",
    "profit_before_tax",
    "income_tax",
    "net_profit",
    "depreciation",
    "exports",
    "employment",
    "wages_total",
    "total_assets",
    "fixed_assets",
    "current_assets",
    "equity",
    "zobowiazania_i_rezerwy_na_zobowiazania",
    "zobowiazania_dlugoterminowe",
    "zobowiazania_krotkoterminow",
    "liabilities_provisions",
    "total_liabilities",
]

STRING_COLUMNS = ["nip", "company", "city", "legal_form", "pkd_description", "sj"]

UNAVAILABLE_SOURCE_COLUMNS = [
    "business_start_year",
    "gpw",
    "incorporation_year_krs",
    "sector",
    "sector_en",
]


def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def validate_expected_columns(df: pd.DataFrame) -> None:
    expected = set(INPUT_REQUIRED_COLUMNS)
    missing = sorted(expected.difference(df.columns))
    if missing:
        raise ValueError(f"Input file is missing expected columns: {missing}")


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    for column in STRING_COLUMNS:
        series = df[column].astype("string").str.strip()
        df[column] = series.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
    return df


def coerce_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    for column in INTEGER_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").round().astype("Int64")

    for column in FLOAT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)

    return df


def add_rank_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["rank_2019"] = pd.to_numeric(df["rank_2019"], errors="coerce").round().astype("Int64")
    df["in_rank_2019"] = df["rank_2019"].notna().astype("Int64")
    return df


def safe_log(series: pd.Series) -> pd.Series:
    positive = series.where(series > 0)
    return np.log(positive)


def safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.where(denominator != 0)
    return numerator.divide(denominator)


def build_derived_variables(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    owner_type_string = (
        pd.to_numeric(df["owner_type"], errors="coerce")
        .round()
        .astype("Int64")
        .astype("string")
    )
    df["owner"] = np.where(owner_type_string.str.startswith("5", na=False), "Foreign", "Domestic")
    df["owner"] = pd.Series(df["owner"], index=df.index, dtype="string")
    df["owner_num"] = np.where(df["owner"] == "Foreign", 1, 0)
    df["owner_num"] = pd.Series(df["owner_num"], index=df.index, dtype="Int64")

    pkd_prefix = (
        pd.to_numeric(df["pkd"], errors="coerce")
        .round()
        .astype("Int64")
        .astype("string")
        .str.zfill(4)
        .str[:2]
    )
    pkd_section = pd.to_numeric(pkd_prefix, errors="coerce").astype("Int64")
    df["manufacturing"] = pkd_section.between(10, 33, inclusive="both").astype("Int64")

    df["price_index"] = df["year"].map(PRICE_INDEX_BY_YEAR).astype(float)
    df["sales_real"] = safe_ratio(df["sales"], df["price_index"])

    df["ln_sales"] = safe_log(df["sales"])
    df["ln_total_assets"] = safe_log(df["total_assets"])
    df["ln_employment"] = safe_log(df["employment"])

    df["profit_margin"] = safe_ratio(df["net_profit"], df["sales"])
    df["operating_margin"] = safe_ratio(df["operating_result"], df["sales"])
    df["export_ratio"] = safe_ratio(df["exports"], df["sales"])
    df["asset_turnover"] = safe_ratio(df["sales"], df["total_assets"])
    df["capital_ratio"] = safe_ratio(df["equity"], df["total_assets"])
    df["equity_multiplier"] = safe_ratio(df["total_assets"], df["equity"])
    df["roa"] = safe_ratio(df["net_profit"], df["total_assets"])
    df["roe"] = safe_ratio(df["net_profit"], df["equity"])
    df["depreciation_ratio"] = safe_ratio(df["depreciation"], df["total_assets"])
    df["wage_intensity"] = safe_ratio(df["wages_total"], df["sales"])

    df["sales_per_employee"] = safe_ratio(df["sales"], df["employment"])
    df["assets_per_employee"] = safe_ratio(df["total_assets"], df["employment"])

    df["has_sales"] = df["sales"].gt(0).fillna(False).astype("Int64")
    df["has_assets"] = df["total_assets"].gt(0).fillna(False).astype("Int64")
    df["has_employment"] = df["employment"].gt(0).fillna(False).astype("Int64")

    df = df.sort_values(KEY_COLUMNS, kind="mergesort")
    lag_sales = df.groupby("nip", sort=False)["sales"].shift(1)
    lag_sales_real = df.groupby("nip", sort=False)["sales_real"].shift(1)
    lag_ln_sales = df.groupby("nip", sort=False)["ln_sales"].shift(1)

    df["sales_growth_yoy"] = np.where(lag_sales > 0, df["sales"] / lag_sales - 1, np.nan)
    df["sales_real_growth_yoy"] = np.where(lag_sales_real > 0, df["sales_real"] / lag_sales_real - 1, np.nan)
    df["sales_log_growth_yoy"] = df["ln_sales"] - lag_ln_sales

    return df


def select_core_columns(df: pd.DataFrame) -> pd.DataFrame:
    extra_columns = sorted(set(df.columns).difference(CORE_COLUMNS + DROP_COLUMNS))
    if extra_columns:
        raise ValueError(f"Unexpected columns found in input: {extra_columns}")
    return df.loc[:, CORE_COLUMNS].copy()


def drop_invalid_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=KEY_COLUMNS).copy()
    df["year"] = df["year"].astype("Int64")
    return df


def deduplicate_firm_year(df: pd.DataFrame) -> pd.DataFrame:
    if not df.duplicated(subset=KEY_COLUMNS).any():
        return df

    working = df.copy()
    value_columns = [column for column in working.columns if column not in KEY_COLUMNS]
    working["_non_null_count"] = working[value_columns].notna().sum(axis=1)

    working = working.sort_values(
        by=KEY_COLUMNS + ["_non_null_count", "company"],
        ascending=[True, True, False, True],
        kind="mergesort",
    )

    working = working.drop_duplicates(subset=KEY_COLUMNS, keep="first")
    working = working.drop(columns="_non_null_count")
    return working


def sort_panel(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(KEY_COLUMNS, kind="mergesort").reset_index(drop=True)


def validate_output(df: pd.DataFrame) -> None:
    if df.duplicated(subset=KEY_COLUMNS).any():
        raise ValueError("Output still contains duplicate (nip, year) rows.")

    if list(df.columns) != CORE_COLUMNS:
        raise ValueError("Output columns do not match the expected canonical schema.")

    output_years = set(df["year"].dropna().astype(int).unique())
    if output_years != EXPECTED_YEARS:
        raise ValueError(f"Output years do not match the expected 2018-2024 range: {sorted(output_years)}")

    rows_2018 = df["year"].eq(P1_LAG_SOURCE_YEAR)
    if not rows_2018.any():
        raise ValueError("Output does not contain 2018 rows needed for P1 lag growth.")
    if df.loc[rows_2018, "sales"].notna().sum() == 0:
        raise ValueError("Output has no observed 2018 sales values for P1 lag growth.")
    if df.loc[rows_2018, "price_index"].isna().any():
        raise ValueError("Output has missing 2018 price_index values.")


def write_outputs(df: pd.DataFrame, parquet_path: Path, xlsx_path: Path) -> None:
    df.to_parquet(parquet_path, index=False)
    df.to_excel(xlsx_path, index=False, engine="xlsxwriter")


def build_core_panel(input_path: Path = INPUT_PATH) -> tuple[pd.DataFrame, dict]:
    df = load_input_data(input_path)
    input_row_count = len(df)
    validate_expected_columns(df)
    df = add_rank_indicators(df)
    df = coerce_numeric_types(df)
    df = build_derived_variables(df)
    df = select_core_columns(df)
    df = normalize_strings(df)
    df = drop_invalid_keys(df)
    df = deduplicate_firm_year(df)
    df = sort_panel(df)
    validate_output(df)
    metadata = {
        "input_row_count": input_row_count,
        "output_row_count": len(df),
        "unique_firms": df["nip"].nunique(),
        "unavailable_source_columns": UNAVAILABLE_SOURCE_COLUMNS,
        "duplicate_firm_year_count": int(df.duplicated(KEY_COLUMNS).sum()),
        "output_years": sorted(df["year"].dropna().astype(int).unique().tolist()),
        "sales_2018_present_count": int(
            df.loc[df["year"].eq(P1_LAG_SOURCE_YEAR), "sales"].notna().sum()
        ),
        "rows_2018": int(df["year"].eq(P1_LAG_SOURCE_YEAR).sum()),
        "nonmissing_2018_annual_columns": {
            column: int(df.loc[df["year"].eq(P1_LAG_SOURCE_YEAR), column].notna().sum())
            for column in ANNUAL_COLUMNS
        },
    }
    return df, metadata


def print_build_summary(df: pd.DataFrame, metadata: dict) -> None:
    print("Core panel build complete")
    print(f"Input row count: {metadata['input_row_count']:,}")
    print(f"Output row count: {metadata['output_row_count']:,}")
    print(f"Number of unique firms: {metadata['unique_firms']:,}")
    print(f"Duplicate (nip, year) rows: {metadata['duplicate_firm_year_count']:,}")
    print(f"Output years: {metadata['output_years']}")
    print(
        "2018 sales is present: "
        f"{metadata['sales_2018_present_count']:,} of {metadata['rows_2018']:,} rows"
    )
    print(
        "2018 is retained only as the sales base for 2019 P1 lag growth; "
        "main growth, trajectories, SGrowth_NR, and start covariates begin in 2019."
    )
    print(
        "Non-missing 2018 annual-column counts "
        f"(missing non-sales annual values are allowed): {metadata['nonmissing_2018_annual_columns']}"
    )
    print(
        "Columns omitted because they are unavailable in the current source: "
        f"{metadata['unavailable_source_columns']}"
    )
    print("Sector mapping not created because the source has no sector column.")
    print("Final columns:")
    for column in df.columns:
        print(column)
    print(f"Final column count: {len(df.columns)}")
    print("Missing counts for key variables:")
    for column in ["sales", "sales_real", "price_index"]:
        print(f"{column}: {int(df[column].isna().sum()):,}")
    print("Summary statistics for key outcomes:")
    print(
        df[["sales", "sales_real", "sales_growth_yoy", "sales_real_growth_yoy"]]
        .describe()
        .to_string()
    )


if __name__ == "__main__":
    core_panel, build_metadata = build_core_panel()
    write_outputs(core_panel, OUTPUT_PARQUET_PATH, OUTPUT_XLSX_PATH)
    print_build_summary(core_panel, build_metadata)
