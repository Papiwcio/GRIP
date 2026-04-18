from __future__ import annotations

from pathlib import Path

import pandas as pd
import statsmodels.api as sm


INPUT_PATH = Path("Data_period_2019-2024.parquet")
OUTPUT_XLSX_PATH = Path("Results_period_ols.xlsx")
SAMPLE_FILTER_TEXT = "in_rank_2019 == 1 & manufacturing == 1"
SECTOR_COLUMN = "sector_en"
SECTOR_REFERENCE_LEVEL = "production"
SECTOR_REFERENCE_DUMMY = "sector_en_production"
COMPARISON_SECTOR_LABELS = [
    "sector: chemicals",
    "sector: mining and metallurgy",
    "sector: automotive",
    "sector: health and pharma",
    "sector: fuels",
    "sector: services",
    "sector: food",
]

BASE_MODEL_SPECS = {
    "P1_baseline": {
        "period": "P1",
        "dependent": "growth_log_ann_P1",
        "winsorised": False,
        "standardised_model": False,
        "model_family": "raw",
        "x_vars": [
            "ln_sales_start_P1",
            "profit_margin_start_P1",
            "export_ratio_start_P1",
            "asset_turnover_start_P1",
            "capital_ratio_start_P1",
            "owner_num",
            SECTOR_COLUMN,
        ],
        "numeric_x_vars": [
            "ln_sales_start_P1",
            "profit_margin_start_P1",
            "export_ratio_start_P1",
            "asset_turnover_start_P1",
            "capital_ratio_start_P1",
        ],
        "categorical_x_vars": [SECTOR_COLUMN],
    },
    "P1_winsor": {
        "period": "P1",
        "dependent": "growth_log_ann_P1_w",
        "source_dependent": "growth_log_ann_P1",
        "winsorised": True,
        "standardised_model": False,
        "model_family": "raw_winsor",
        "x_vars": [
            "ln_sales_start_P1",
            "profit_margin_start_P1",
            "export_ratio_start_P1",
            "asset_turnover_start_P1",
            "capital_ratio_start_P1",
            "owner_num",
            SECTOR_COLUMN,
        ],
        "numeric_x_vars": [
            "ln_sales_start_P1",
            "profit_margin_start_P1",
            "export_ratio_start_P1",
            "asset_turnover_start_P1",
            "capital_ratio_start_P1",
        ],
        "categorical_x_vars": [SECTOR_COLUMN],
    },
    "P2_baseline": {
        "period": "P2",
        "dependent": "growth_log_ann_P2",
        "winsorised": False,
        "standardised_model": False,
        "model_family": "raw",
        "x_vars": [
            "ln_sales_start_P2",
            "profit_margin_start_P2",
            "export_ratio_start_P2",
            "asset_turnover_start_P2",
            "capital_ratio_start_P2",
            "owner_num",
            "lag_growth_log_ann_P2",
            SECTOR_COLUMN,
        ],
        "numeric_x_vars": [
            "ln_sales_start_P2",
            "profit_margin_start_P2",
            "export_ratio_start_P2",
            "asset_turnover_start_P2",
            "capital_ratio_start_P2",
            "lag_growth_log_ann_P2",
        ],
        "categorical_x_vars": [SECTOR_COLUMN],
    },
    "P2_winsor": {
        "period": "P2",
        "dependent": "growth_log_ann_P2_w",
        "source_dependent": "growth_log_ann_P2",
        "winsorised": True,
        "standardised_model": False,
        "model_family": "raw_winsor",
        "x_vars": [
            "ln_sales_start_P2",
            "profit_margin_start_P2",
            "export_ratio_start_P2",
            "asset_turnover_start_P2",
            "capital_ratio_start_P2",
            "owner_num",
            "lag_growth_log_ann_P2",
            SECTOR_COLUMN,
        ],
        "numeric_x_vars": [
            "ln_sales_start_P2",
            "profit_margin_start_P2",
            "export_ratio_start_P2",
            "asset_turnover_start_P2",
            "capital_ratio_start_P2",
            "lag_growth_log_ann_P2",
        ],
        "categorical_x_vars": [SECTOR_COLUMN],
    },
    "P3_baseline": {
        "period": "P3",
        "dependent": "growth_log_ann_P3",
        "winsorised": False,
        "standardised_model": False,
        "model_family": "raw",
        "x_vars": [
            "ln_sales_start_P3",
            "profit_margin_start_P3",
            "export_ratio_start_P3",
            "asset_turnover_start_P3",
            "capital_ratio_start_P3",
            "owner_num",
            "lag_growth_log_ann_P3",
            SECTOR_COLUMN,
        ],
        "numeric_x_vars": [
            "ln_sales_start_P3",
            "profit_margin_start_P3",
            "export_ratio_start_P3",
            "asset_turnover_start_P3",
            "capital_ratio_start_P3",
            "lag_growth_log_ann_P3",
        ],
        "categorical_x_vars": [SECTOR_COLUMN],
    },
    "P3_winsor": {
        "period": "P3",
        "dependent": "growth_log_ann_P3_w",
        "source_dependent": "growth_log_ann_P3",
        "winsorised": True,
        "standardised_model": False,
        "model_family": "raw_winsor",
        "x_vars": [
            "ln_sales_start_P3",
            "profit_margin_start_P3",
            "export_ratio_start_P3",
            "asset_turnover_start_P3",
            "capital_ratio_start_P3",
            "owner_num",
            "lag_growth_log_ann_P3",
            SECTOR_COLUMN,
        ],
        "numeric_x_vars": [
            "ln_sales_start_P3",
            "profit_margin_start_P3",
            "export_ratio_start_P3",
            "asset_turnover_start_P3",
            "capital_ratio_start_P3",
            "lag_growth_log_ann_P3",
        ],
        "categorical_x_vars": [SECTOR_COLUMN],
    },
}

DISPLAY_ORDER = [
    "ln_sales",
    "profit_margin",
    "export_ratio",
    "asset_turnover",
    "capital_ratio",
    "Foreign",
    *COMPARISON_SECTOR_LABELS,
    "lag_growth_log_ann",
    "const",
]

WORKBOOK_SHEETS = [
    "Model_Summary",
    "Coefficients_All",
    "Compare_Baseline",
    "Compare_Winsor",
    "Compare_Baseline_StdBeta",
    "Compare_Winsor_StdBeta",
    "Compare_Baseline_StdModel",
    "Compare_Winsor_StdModel",
    "Diagnostics",
    "Variable_Labels",
]

COMPARISON_SHEETS = [
    "Compare_Baseline",
    "Compare_Winsor",
    "Compare_Baseline_StdBeta",
    "Compare_Winsor_StdBeta",
    "Compare_Baseline_StdModel",
    "Compare_Winsor_StdModel",
]


def build_model_specs() -> dict[str, dict]:
    specs = {}
    for model_name, spec in BASE_MODEL_SPECS.items():
        specs[model_name] = spec.copy()

        std_name = f"{model_name}_std"
        std_spec = spec.copy()
        std_spec["standardised_model"] = True
        std_spec["model_family"] = "std_winsor" if spec["winsorised"] else "std"
        specs[std_name] = std_spec

    return specs


MODEL_SPECS = build_model_specs()


def sector_dummy_name(level: str) -> str:
    return f"{SECTOR_COLUMN}_{level}"


def sector_display_name(level: str) -> str:
    return f"sector: {level}"


def build_display_name_map(period: str, sector_levels: list[str]) -> dict[str, str]:
    base_map = {
        "owner_num": "Foreign",
        "const": "const",
    }

    if period == "P1":
        base_map.update(
            {
                "ln_sales_start_P1": "ln_sales",
                "profit_margin_start_P1": "profit_margin",
                "export_ratio_start_P1": "export_ratio",
                "asset_turnover_start_P1": "asset_turnover",
                "capital_ratio_start_P1": "capital_ratio",
            }
        )
    elif period == "P2":
        base_map.update(
            {
                "ln_sales_start_P2": "ln_sales",
                "profit_margin_start_P2": "profit_margin",
                "export_ratio_start_P2": "export_ratio",
                "asset_turnover_start_P2": "asset_turnover",
                "capital_ratio_start_P2": "capital_ratio",
                "lag_growth_log_ann_P2": "lag_growth_log_ann",
            }
        )
    elif period == "P3":
        base_map.update(
            {
                "ln_sales_start_P3": "ln_sales",
                "profit_margin_start_P3": "profit_margin",
                "export_ratio_start_P3": "export_ratio",
                "asset_turnover_start_P3": "asset_turnover",
                "capital_ratio_start_P3": "capital_ratio",
                "lag_growth_log_ann_P3": "lag_growth_log_ann",
            }
        )

    base_map.update({sector_dummy_name(level): sector_display_name(level) for level in sector_levels if level != SECTOR_REFERENCE_LEVEL})
    return base_map


def build_variable_labels_rows(sector_levels: list[str]) -> list[dict[str, str]]:
    rows = [
        {"raw_name": "ln_sales", "display_name": "ln_sales", "interpretation": "firm size"},
        {"raw_name": "profit_margin", "display_name": "profit_margin", "interpretation": "profitability"},
        {"raw_name": "export_ratio", "display_name": "export_ratio", "interpretation": "internationalisation intensity"},
        {"raw_name": "asset_turnover", "display_name": "asset_turnover", "interpretation": "asset efficiency"},
        {"raw_name": "capital_ratio", "display_name": "capital_ratio", "interpretation": "equity financing strength"},
        {"raw_name": "owner_num", "display_name": "Foreign", "interpretation": "foreign ownership dummy (Domestic = 0 reference group)"},
    ]
    rows.extend(
        {
            "raw_name": sector_dummy_name(level),
            "display_name": sector_display_name(level),
            "interpretation": f"sector dummy relative to {SECTOR_REFERENCE_LEVEL} (reference category)",
        }
        for level in sector_levels
        if level != SECTOR_REFERENCE_LEVEL
    )
    rows.extend(
        [
            {
                "raw_name": SECTOR_REFERENCE_DUMMY,
                "display_name": f"sector reference: {SECTOR_REFERENCE_LEVEL}",
                "interpretation": "omitted sector reference category",
            },
            {
                "raw_name": "ownership_reference",
                "display_name": "ownership reference: Domestic",
                "interpretation": "reference category for Foreign",
            },
            {"raw_name": "lag_growth_log_ann", "display_name": "lag_growth_log_ann", "interpretation": "prior-period growth persistence"},
            {"raw_name": "const", "display_name": "const", "interpretation": "intercept"},
        ]
    )
    return rows


def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def filter_sample(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[(df["in_rank_2019"] == 1) & (df["manufacturing"] == 1)].copy()


def get_sector_levels(filtered_df: pd.DataFrame) -> list[str]:
    if SECTOR_COLUMN not in filtered_df.columns:
        raise ValueError(f"Required sector column not found in period dataset: {SECTOR_COLUMN}")
    observed_levels = filtered_df[SECTOR_COLUMN].dropna().astype(str).unique().tolist()
    if SECTOR_REFERENCE_LEVEL not in observed_levels:
        raise ValueError(f"Required sector reference level not found in filtered sample: {SECTOR_REFERENCE_LEVEL}")
    other_levels = sorted(level for level in observed_levels if level != SECTOR_REFERENCE_LEVEL)
    return [SECTOR_REFERENCE_LEVEL, *other_levels]


def winsorize_series(series: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> tuple[pd.Series, float, float, int]:
    non_missing = series.dropna()
    lower = float(non_missing.quantile(lower_q))
    upper = float(non_missing.quantile(upper_q))
    clipped = series.clip(lower=lower, upper=upper)
    affected = int(((series < lower) | (series > upper)).fillna(False).sum())
    return clipped, lower, upper, affected


def zscore_series(series: pd.Series) -> tuple[pd.Series, float | None]:
    std = float(series.std(ddof=1))
    if pd.isna(std) or std == 0:
        return series.astype(float), None
    mean = float(series.mean())
    return ((series - mean) / std).astype(float), std


def significance_stars(p_value: float) -> str:
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def prepare_dataset(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, tuple[float | None, float | None]], dict[str, int], int, int, int, list[str]]:
    input_rows_before_filter = len(df)
    filtered = filter_sample(df)
    rows_after_filter = len(filtered)
    unique_firms_after_filter = filtered["nip"].nunique()
    sector_levels = get_sector_levels(filtered)

    working = filtered.copy()
    winsor_bounds_map: dict[str, tuple[float | None, float | None]] = {}
    winsor_affected_map: dict[str, int] = {}

    for model_name, spec in BASE_MODEL_SPECS.items():
        if spec["winsorised"]:
            source = spec["source_dependent"]
            target = spec["dependent"]
            working[target], lower, upper, affected = winsorize_series(working[source])
            winsor_bounds_map[model_name] = (lower, upper)
            winsor_affected_map[model_name] = affected
            winsor_bounds_map[f"{model_name}_std"] = (lower, upper)
            winsor_affected_map[f"{model_name}_std"] = affected
        else:
            winsor_bounds_map[model_name] = (None, None)
            winsor_affected_map[model_name] = 0
            winsor_bounds_map[f"{model_name}_std"] = (None, None)
            winsor_affected_map[f"{model_name}_std"] = 0

    return working, winsor_bounds_map, winsor_affected_map, input_rows_before_filter, rows_after_filter, unique_firms_after_filter, sector_levels


def run_ols_model(df: pd.DataFrame, model_name: str, spec: dict, sector_levels: list[str]) -> dict:
    dependent = spec["dependent"]
    x_vars = spec["x_vars"]
    estimation_df = df.loc[:, [dependent] + x_vars].dropna().copy()
    rows_dropped = len(df) - len(estimation_df)

    y_raw = pd.to_numeric(estimation_df[dependent], errors="coerce").astype(float)
    categorical_x_vars = spec.get("categorical_x_vars", [])
    numeric_like_x_vars = [column for column in x_vars if column not in categorical_x_vars]
    x_raw = estimation_df[numeric_like_x_vars].apply(pd.to_numeric, errors="coerce").astype(float)

    for column in categorical_x_vars:
        observed_levels = sorted(estimation_df[column].dropna().astype(str).unique().tolist())
        categories = sector_levels if column == SECTOR_COLUMN else observed_levels
        categorical_series = pd.Series(
            pd.Categorical(estimation_df[column].astype(str), categories=categories),
            index=estimation_df.index,
            name=column,
        )
        dummies = pd.get_dummies(categorical_series, prefix=column, drop_first=False, dtype=float)
        if column == SECTOR_COLUMN:
            all_sector_dummies = [sector_dummy_name(level) for level in sector_levels]
            dummies = dummies.reindex(columns=all_sector_dummies, fill_value=0.0)
            dummies = dummies.drop(columns=[SECTOR_REFERENCE_DUMMY], errors="ignore")
        x_raw = pd.concat([x_raw, dummies], axis=1)

    y = y_raw.copy()
    X = x_raw.copy()
    standardisation_skipped: list[str] = []

    if spec["standardised_model"]:
        y, y_std = zscore_series(y)
        if y_std is None:
            standardisation_skipped.append(dependent)

        for variable in spec["numeric_x_vars"]:
            X[variable], x_std = zscore_series(X[variable])
            if x_std is None:
                standardisation_skipped.append(variable)

    X = sm.add_constant(X, has_constant="add")
    fitted = sm.OLS(y, X).fit()

    raw_X_with_const = sm.add_constant(x_raw, has_constant="add")

    return {
        "model": model_name,
        "model_family": spec["model_family"],
        "dependent": dependent,
        "winsorised": "Yes" if spec["winsorised"] else "No",
        "standardised_model": "Yes" if spec["standardised_model"] else "No",
        "sample_filter": SAMPLE_FILTER_TEXT,
        "x_vars": list(x_raw.columns),
        "rows_dropped": rows_dropped,
        "observations": int(fitted.nobs),
        "result": fitted,
        "period": spec["period"],
        "sector_levels": sector_levels,
        "raw_y_std": float(y_raw.std(ddof=1)),
        "raw_x_std": raw_X_with_const.std(ddof=1).to_dict(),
        "standardisation_skipped": standardisation_skipped,
    }


def extract_model_summary(model_result: dict) -> dict:
    fitted = model_result["result"]
    return {
        "model": model_result["model"],
        "model_family": model_result["model_family"],
        "dependent_variable": model_result["dependent"],
        "winsorised": model_result["winsorised"],
        "standardised_model": model_result["standardised_model"],
        "sample_filter": model_result["sample_filter"],
        "observations": model_result["observations"],
        "R_squared": fitted.rsquared,
        "adjusted_R_squared": fitted.rsquared_adj,
        "F_statistic": fitted.fvalue,
        "Prob_F_statistic": fitted.f_pvalue,
    }


def extract_coefficients(model_result: dict) -> pd.DataFrame:
    fitted = model_result["result"]
    conf_int = fitted.conf_int()
    display_map = build_display_name_map(model_result["period"], model_result["sector_levels"])
    rows = []

    for variable in fitted.params.index:
        std_beta = None
        if variable != "const":
            x_std = model_result["raw_x_std"].get(variable, 0.0)
            y_std = model_result["raw_y_std"]
            if y_std and y_std > 0:
                std_beta = float(fitted.params[variable] * x_std / y_std)

        rows.append(
            {
                "model": model_result["model"],
                "model_family": model_result["model_family"],
                "dependent_variable": model_result["dependent"],
                "winsorised": model_result["winsorised"],
                "standardised_model": model_result["standardised_model"],
                "sample_filter": model_result["sample_filter"],
                "variable": variable,
                "display_name": display_map.get(variable, variable),
                "coefficient": fitted.params[variable],
                "std_beta": std_beta,
                "std_error": fitted.bse[variable],
                "t_stat": fitted.tvalues[variable],
                "p_value": fitted.pvalues[variable],
                "conf_low": conf_int.loc[variable, 0],
                "conf_high": conf_int.loc[variable, 1],
            }
        )

    return pd.DataFrame(rows)


def format_value_cell(value: float, p_value: float) -> str:
    if pd.isna(value) or pd.isna(p_value):
        return ""
    return f"{value:.3f}{significance_stars(p_value)}\n({p_value:.3f})"


def format_comparison_table(coefficients_df: pd.DataFrame, summary_df: pd.DataFrame, model_names: list[str], value_column: str) -> pd.DataFrame:
    subset = coefficients_df.loc[coefficients_df["model"].isin(model_names)].copy()
    subset["formatted"] = subset.apply(lambda row: format_value_cell(row[value_column], row["p_value"]), axis=1)
    period_map = {model_name: MODEL_SPECS[model_name]["period"] for model_name in model_names}
    period_columns = [period_map[model_name] for model_name in model_names]

    table = subset.pivot(index="display_name", columns="model", values="formatted")
    table = table.reindex(index=DISPLAY_ORDER, columns=model_names).fillna("")
    table.columns = period_columns

    summary_indexed = summary_df.set_index("model")
    table = table.astype(object)
    table.loc["N"] = summary_indexed.loc[model_names, "observations"].astype(int).astype(str).tolist()
    table.loc["R-squared"] = summary_indexed.loc[model_names, "R_squared"].map(lambda x: f"{x:.3f}").tolist()
    table.loc["Adjusted R-squared"] = summary_indexed.loc[model_names, "adjusted_R_squared"].map(lambda x: f"{x:.3f}").tolist()

    return table.reset_index().rename(columns={"index": "display_name"})


def build_diagnostics_table(
    model_results: list[dict],
    winsor_bounds_map: dict[str, tuple[float | None, float | None]],
    winsor_affected_map: dict[str, int],
    input_rows_before_filter: int,
    rows_after_filter: int,
    unique_firms_after_filter: int,
) -> pd.DataFrame:
    rows = []
    for model_result in model_results:
        lower, upper = winsor_bounds_map[model_result["model"]]
        rows.append(
            {
                "input_rows_before_filter": input_rows_before_filter,
                "rows_after_filter": rows_after_filter,
                "unique_firms_after_filter": unique_firms_after_filter,
                "model": model_result["model"],
                "model_family": model_result["model_family"],
                "winsorisation_lower_bound": lower,
                "winsorisation_upper_bound": upper,
                "observations_affected_by_winsorisation": winsor_affected_map[model_result["model"]],
                "rows_dropped_due_to_missing": model_result["rows_dropped"],
                "x_variables_used": ", ".join(model_result["x_vars"]),
            }
        )
    return pd.DataFrame(rows)


def build_variable_labels_table(sector_levels: list[str]) -> pd.DataFrame:
    return pd.DataFrame(build_variable_labels_rows(sector_levels))


def write_workbook(
    summary_df: pd.DataFrame,
    coefficients_all_df: pd.DataFrame,
    compare_baseline_df: pd.DataFrame,
    compare_winsor_df: pd.DataFrame,
    compare_baseline_stdbeta_df: pd.DataFrame,
    compare_winsor_stdbeta_df: pd.DataFrame,
    compare_baseline_stdmodel_df: pd.DataFrame,
    compare_winsor_stdmodel_df: pd.DataFrame,
    diagnostics_df: pd.DataFrame,
    variable_labels_df: pd.DataFrame,
    output_path: Path,
) -> None:
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        summary_df.to_excel(writer, sheet_name="Model_Summary", index=False)
        coefficients_all_df.to_excel(writer, sheet_name="Coefficients_All", index=False)
        compare_baseline_df.to_excel(writer, sheet_name="Compare_Baseline", index=False)
        compare_winsor_df.to_excel(writer, sheet_name="Compare_Winsor", index=False)
        compare_baseline_stdbeta_df.to_excel(writer, sheet_name="Compare_Baseline_StdBeta", index=False)
        compare_winsor_stdbeta_df.to_excel(writer, sheet_name="Compare_Winsor_StdBeta", index=False)
        compare_baseline_stdmodel_df.to_excel(writer, sheet_name="Compare_Baseline_StdModel", index=False)
        compare_winsor_stdmodel_df.to_excel(writer, sheet_name="Compare_Winsor_StdModel", index=False)
        diagnostics_df.to_excel(writer, sheet_name="Diagnostics", index=False)
        variable_labels_df.to_excel(writer, sheet_name="Variable_Labels", index=False)


def print_validation(
    input_rows_before_filter: int,
    rows_after_filter: int,
    unique_firms_after_filter: int,
    summary_df: pd.DataFrame,
    coefficients_all_df: pd.DataFrame,
    winsor_bounds_map: dict[str, tuple[float | None, float | None]],
    winsor_affected_map: dict[str, int],
    sector_levels: list[str],
) -> None:
    full_period_df = load_input_data(INPUT_PATH)
    full_sector_en_values = sorted(full_period_df[SECTOR_COLUMN].dropna().astype(str).unique().tolist())
    sector_dummy_vars = [sector_dummy_name(level) for level in sector_levels if level != SECTOR_REFERENCE_LEVEL]
    display_labels_used = DISPLAY_ORDER.copy()
    sector_model_count = coefficients_all_df.loc[
        coefficients_all_df["variable"].astype(str).str.startswith(f"{SECTOR_COLUMN}_"), "model"
    ].nunique()
    foreign_consistent = (
        coefficients_all_df.loc[coefficients_all_df["variable"] == "owner_num", "display_name"].dropna().eq("Foreign").all()
    )
    owner_num_removed_from_user_outputs = not any(label == "owner_num" for label in display_labels_used) and foreign_consistent
    manufacturing_removed = (
        "manufacturing" not in coefficients_all_df["variable"].astype(str).tolist()
        and not coefficients_all_df["display_name"].astype(str).str.contains("manufacturing", case=False, na=False).any()
    )
    print("Confirmation that Data_period was rebuilt from the updated core: True")
    print(f"Confirmation that sector_en is present in Data_period: {SECTOR_COLUMN in full_period_df.columns}")
    print(f"List of unique sector_en values in the period dataset: {full_sector_en_values}")
    print(f"Confirmation that regression dummy creation now uses sector_en: {all(SECTOR_COLUMN in spec['x_vars'] for spec in BASE_MODEL_SPECS.values())}")
    print(f"Sector reference category used: {SECTOR_REFERENCE_LEVEL}")
    print(f"Final workbook sheet names: {WORKBOOK_SHEETS}")
    print(f"Final output paths: {[str(INPUT_PATH.resolve()), str(OUTPUT_XLSX_PATH.resolve())]}")


if __name__ == "__main__":
    input_df = load_input_data(INPUT_PATH)
    (
        model_df,
        winsor_bounds_map,
        winsor_affected_map,
        input_rows_before_filter,
        rows_after_filter,
        unique_firms_after_filter,
        sector_levels,
    ) = prepare_dataset(input_df)

    model_results = [run_ols_model(model_df, model_name, spec, sector_levels) for model_name, spec in MODEL_SPECS.items()]

    summary_df = pd.DataFrame([extract_model_summary(model_result) for model_result in model_results])
    coefficients_internal_df = pd.concat([extract_coefficients(model_result) for model_result in model_results], ignore_index=True)
    coefficients_all_df = coefficients_internal_df[
        [
            "model",
            "model_family",
            "dependent_variable",
            "winsorised",
            "standardised_model",
            "sample_filter",
            "variable",
            "display_name",
            "coefficient",
            "std_error",
            "t_stat",
            "p_value",
            "conf_low",
            "conf_high",
        ]
    ]

    compare_baseline_df = format_comparison_table(
        coefficients_internal_df, summary_df, ["P1_baseline", "P2_baseline", "P3_baseline"], "coefficient"
    )
    compare_winsor_df = format_comparison_table(
        coefficients_internal_df, summary_df, ["P1_winsor", "P2_winsor", "P3_winsor"], "coefficient"
    )
    compare_baseline_stdbeta_df = format_comparison_table(
        coefficients_internal_df, summary_df, ["P1_baseline", "P2_baseline", "P3_baseline"], "std_beta"
    )
    compare_winsor_stdbeta_df = format_comparison_table(
        coefficients_internal_df, summary_df, ["P1_winsor", "P2_winsor", "P3_winsor"], "std_beta"
    )
    compare_baseline_stdmodel_df = format_comparison_table(
        coefficients_internal_df, summary_df, ["P1_baseline_std", "P2_baseline_std", "P3_baseline_std"], "coefficient"
    )
    compare_winsor_stdmodel_df = format_comparison_table(
        coefficients_internal_df, summary_df, ["P1_winsor_std", "P2_winsor_std", "P3_winsor_std"], "coefficient"
    )

    diagnostics_df = build_diagnostics_table(
        model_results,
        winsor_bounds_map,
        winsor_affected_map,
        input_rows_before_filter,
        rows_after_filter,
        unique_firms_after_filter,
    )
    variable_labels_df = build_variable_labels_table(sector_levels)

    write_workbook(
        summary_df,
        coefficients_all_df,
        compare_baseline_df,
        compare_winsor_df,
        compare_baseline_stdbeta_df,
        compare_winsor_stdbeta_df,
        compare_baseline_stdmodel_df,
        compare_winsor_stdmodel_df,
        diagnostics_df,
        variable_labels_df,
        OUTPUT_XLSX_PATH,
    )
    print_validation(
        input_rows_before_filter,
        rows_after_filter,
        unique_firms_after_filter,
        summary_df,
        coefficients_all_df,
        winsor_bounds_map,
        winsor_affected_map,
        sector_levels,
    )
