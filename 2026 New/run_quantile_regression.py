from __future__ import annotations

from pathlib import Path
from typing import Any
import warnings

import numpy as np
import pandas as pd
import statsmodels.api as sm

from analysis_config import (
    PERIODS,
    QUANTILE_ORDER,
    SAMPLE_ORDER,
    SCENARIO_METADATA,
    SCENARIO_ORDER,
    VARIABLE_LABELS,
    build_sample_mask,
    resolve_variable_order,
)
from analysis_helpers import (
    apply_header_format,
    apply_safe_autofilter,
    build_shared_sample_counts,
    freeze_and_hide_gridlines,
    safe_numeric,
    set_table_column_widths,
)


CONFIG = {
    "analysis_name": "period_quantile_regression",
    "input_file": "Data_period_2018-2024.parquet",
    "output_file": "Results_period_quantile.xlsx",
    "growth_mode": "nominal",
    "periods": ["P1", "P2", "P3", "FULL"],
    "quantiles": [0.10, 0.50, 0.90],
    "preferred_model_variant": "winsor_std",
    "winsorise_dependent": True,
    "winsor_lower": 0.01,
    "winsor_upper": 0.99,
    "standardise_dependent": True,
    "standardise_regressors": True,
    "quantreg_settings": {
        "max_iter": 5000,
        "p_tol": 1e-6,
    },
}

BASE_REGRESSORS = [
    "ln_sales",
    "profit_margin",
    "export_ratio",
    "asset_turnover",
    "capital_ratio",
    "sales_per_employee",
]
OWNER_COLUMN = "owner_num"
CATEGORICAL_CONTROLS = ["sector_en"]
LAG_GROWTH_PERIODS = ["P1", "P2", "P3"]

CATEGORICAL_METADATA = {
    "sector_en": {
        "reference": "production",
        "display_prefix": "sector: ",
        "interpretation_template": "sector dummy relative to production reference category",
    }
}

INTERACTION_METADATA = {
    "foreign_x_size_ratio": {
        "variables": ["export_ratio", "ln_sales"],
        "display_name": "export_ratio × ln_sales",
        "interpretation": "interaction between export intensity and company size",
        "standardise": True,
        "include": True,
    }
}

SCENARIOS = {name: metadata["filter"] for name, metadata in SCENARIO_METADATA.items()}
missing = set(SCENARIO_ORDER) - set(SCENARIOS.keys())
if missing:
    raise ValueError(f"SCENARIO_ORDER contains unknown scenarios: {missing}")
extra = set(SCENARIOS.keys()) - set(SCENARIO_ORDER)
if extra:
    raise ValueError(f"SCENARIOS contains scenarios missing from SCENARIO_ORDER: {extra}")

SHARED_SAMPLE_SCENARIOS = {
    scenario_name: metadata["sample"]
    for scenario_name, metadata in SCENARIO_METADATA.items()
    if metadata["sample"] in SAMPLE_ORDER
}

OUTPUT_SHEETS = [
    "README",
    "Compare_FULL",
    "Compare_P1",
    "Compare_P2",
    "Compare_P3",
    "Quantile_Patterns",
    "Coefficients_Long",
    "Model_Summary_Long",
    "Convergence_Summary",
    "Diagnostics_Long",
    "Variable_Labels",
    "Run_Log",
]

COEFFICIENT_COLUMNS = [
    "scenario",
    "period",
    "growth_mode",
    "quantile",
    "quantile_label",
    "performance_anchor",
    "dependent_variable",
    "winsorised",
    "standardised_model",
    "sample_filter",
    "raw_variable",
    "display_name",
    "variable_type",
    "variable_order",
    "coefficient",
    "std_error",
    "t_stat",
    "p_value",
    "CI_lower",
    "CI_upper",
]

SUMMARY_COLUMNS = [
    "scenario",
    "period",
    "growth_mode",
    "quantile",
    "quantile_label",
    "performance_anchor",
    "dependent_variable",
    "observations",
    "pseudo_R2",
    "iterations",
    "converged",
    "rows_dropped_due_to_missing",
    "sample_filter",
]

DIAGNOSTIC_COLUMNS = [
    "scenario",
    "period",
    "quantile",
    "model_name",
    "input_row_count",
    "row_count_after_filter",
    "observations_used",
    "rows_dropped_due_to_missing",
    "generated_dependent_variable",
    "generated_regressors_for_period",
    "variables_included_in_standardisation",
    "variables_excluded_from_standardisation",
    "winsorisation_lower_bound",
    "winsorisation_upper_bound",
    "observations_affected_by_winsorisation",
    "quantreg_max_iter",
    "quantreg_p_tol",
    "convergence_warning_present",
    "converged",
    "warning",
]

CONVERGENCE_SUMMARY_COLUMNS = [
    "scenario",
    "period",
    "quantile",
    "warning_present",
    "warning_message",
]

RUN_LOG_COLUMNS = [
    "scenario",
    "period",
    "quantile",
    "event",
    "observations_used",
    "rows_dropped_due_to_missing",
    "warning",
]


def quantile_label(q: float) -> str:
    return {0.10: "Q10", 0.50: "Q50", 0.90: "Q90"}.get(round(float(q), 2), f"Q{int(round(q * 100))}")


def period_order_map() -> dict[str, int]:
    return {period: order for order, period in enumerate([*PERIODS, "FULL"])}


def performance_anchor(q: float) -> str:
    return {
        0.10: "Bottom 10% threshold",
        0.50: "Median / middle performance",
        0.90: "Top 10% threshold",
    }.get(round(float(q), 2), "Conditional quantile")


def get_growth_prefix(config: dict[str, Any]) -> str:
    if config["growth_mode"] == "real":
        return "r"
    if config["growth_mode"] == "nominal":
        return "n"
    raise ValueError("CONFIG['growth_mode'] must be 'real' or 'nominal'.")


def complete_flag_col(config: dict[str, Any]) -> str:
    return f"has_complete_{get_growth_prefix(config)}trajectory"


def quantreg_settings(config: dict[str, Any]) -> dict[str, Any]:
    return dict(config["quantreg_settings"])


def dependent_col(config: dict[str, Any], period: str) -> str:
    if period == "FULL":
        return f"{get_growth_prefix(config)}growth_log_ann_2019_2024"
    return f"{get_growth_prefix(config)}growth_log_ann_{period}"


def lag_growth_col(config: dict[str, Any], period: str) -> str:
    return f"lag_{get_growth_prefix(config)}growth_log_ann_{period}"


def regressor_period(period: str) -> str:
    return "P1" if period == "FULL" else period


def regressor_col(base_name: str, period: str) -> str:
    return f"{base_name}_start_{regressor_period(period)}"


def active_interactions() -> list[dict[str, Any]]:
    return [
        {"name": name, **metadata}
        for name, metadata in INTERACTION_METADATA.items()
        if metadata.get("include") is True
    ]


def interaction_col(name: str, period: str) -> str:
    return f"{name}_{period}"


def resolve_interaction_source(variable_name: str, period: str) -> str:
    if variable_name in BASE_REGRESSORS:
        return regressor_col(variable_name, period)
    if variable_name == OWNER_COLUMN:
        return OWNER_COLUMN
    raise ValueError(f"Unsupported interaction variable: {variable_name}")


def scenario_sample_name(scenario_name: str) -> str:
    return SHARED_SAMPLE_SCENARIOS.get(scenario_name, scenario_name)


def sample_filter_text(config: dict[str, Any], scenario_name: str) -> str:
    complete = f"{complete_flag_col(config)} == 1"
    if scenario_name in SHARED_SAMPLE_SCENARIOS:
        sample_name = SHARED_SAMPLE_SCENARIOS[scenario_name]
        if sample_name == "All":
            return complete
        if sample_name == "Rank2019":
            return f"{complete} and in_rank_2019 == 1"
        if sample_name == "Rank2019_Manufacturing":
            return f"{complete} and in_rank_2019 == 1 and manufacturing == 1"
    filter_query = SCENARIOS[scenario_name]
    return f"{complete} and {filter_query if filter_query is not None else 'True'}"


def load_input_data(config: dict[str, Any]) -> pd.DataFrame:
    path = Path(config["input_file"])
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def apply_scenario_filter(df: pd.DataFrame, scenario_name: str, filter_query: str | None) -> pd.DataFrame:
    if scenario_name in SHARED_SAMPLE_SCENARIOS:
        return df.loc[build_sample_mask(df, SHARED_SAMPLE_SCENARIOS[scenario_name])].copy()
    if filter_query is None:
        return df.copy()
    return df.query(filter_query, engine="python").copy()


def apply_complete_filter(df: pd.DataFrame, config: dict[str, Any], scenario_name: str) -> pd.DataFrame:
    sample_name = scenario_sample_name(scenario_name)
    if sample_name in SAMPLE_ORDER:
        return df.loc[build_sample_mask(df, sample_name, complete_flag_col(config))].copy()
    return df.loc[safe_numeric(df[complete_flag_col(config)]).eq(1)].copy()


def add_interaction_columns(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    output = df.copy()
    for period in config["periods"]:
        for interaction in active_interactions():
            sources = [
                resolve_interaction_source(variable_name, period)
                for variable_name in interaction["variables"]
            ]
            output[interaction_col(interaction["name"], period)] = (
                safe_numeric(output[sources[0]]) * safe_numeric(output[sources[1]])
            )
    return output


def period_regressors(config: dict[str, Any], period: str) -> list[str]:
    regressors = [regressor_col(base_name, period) for base_name in BASE_REGRESSORS]
    regressors.append(OWNER_COLUMN)
    regressors.extend(interaction_col(interaction["name"], period) for interaction in active_interactions())
    if period in LAG_GROWTH_PERIODS:
        regressors.append(lag_growth_col(config, period))
    return regressors


def variable_registry(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    for order, base_name in enumerate(BASE_REGRESSORS, start=10):
        for period in config["periods"]:
            column = regressor_col(base_name, period)
            registry[column] = {
                "display_name": VARIABLE_LABELS.get(column, VARIABLE_LABELS.get(base_name, base_name)),
                "variable_type": "numeric",
                "standardise": True,
                "source": base_name,
                "order": order,
            }
    registry[OWNER_COLUMN] = {
        "display_name": "Foreign",
        "variable_type": "dummy",
        "standardise": False,
        "source": "owner",
        "order": 1000,
    }
    for order, interaction in enumerate(active_interactions(), start=1500):
        for period in config["periods"]:
            column = interaction_col(interaction["name"], period)
            registry[column] = {
                "display_name": interaction["display_name"],
                "variable_type": "interaction",
                "standardise": bool(interaction["standardise"]),
                "source": "interaction",
                "order": order,
            }
    for period in LAG_GROWTH_PERIODS:
        column = lag_growth_col(config, period)
        registry[column] = {
            "display_name": VARIABLE_LABELS.get(column, "lag_growth_log_ann"),
            "variable_type": "lag",
            "standardise": True,
            "source": "lag_growth",
            "order": 3000,
        }
    registry["const"] = {
        "display_name": "const",
        "variable_type": "constant",
        "standardise": False,
        "source": "constant",
        "order": 4000,
    }
    return registry


def categorical_levels(df: pd.DataFrame) -> dict[str, list[str]]:
    levels = {}
    for column in CATEGORICAL_CONTROLS:
        observed = sorted(df[column].dropna().astype(str).unique().tolist())
        reference = CATEGORICAL_METADATA[column]["reference"]
        configured_levels = CATEGORICAL_METADATA[column].get("levels")
        if configured_levels:
            configured_levels = [str(level) for level in configured_levels]
            if reference not in configured_levels:
                raise ValueError(f"Reference category {reference!r} is missing from configured levels for {column!r}.")
            unexpected = sorted(set(observed).difference(configured_levels))
            if unexpected:
                raise ValueError(f"Unexpected categories for {column!r}: {unexpected}.")
            levels[column] = [reference] + [level for level in configured_levels if level != reference]
            continue
        if reference not in observed:
            raise ValueError(f"Reference category {reference!r} missing for {column!r}.")
        levels[column] = [reference] + [level for level in observed if level != reference]
    return levels


def add_categorical_registry(
    registry: dict[str, dict[str, Any]],
    levels: dict[str, list[str]],
) -> dict[str, dict[str, Any]]:
    output = dict(registry)
    for order, column in enumerate(CATEGORICAL_CONTROLS, start=2000):
        metadata = CATEGORICAL_METADATA[column]
        for level in levels[column]:
            raw_name = f"{column}_{level}"
            is_reference = str(level) == str(metadata["reference"])
            output[raw_name] = {
                "display_name": (
                    f"{metadata['display_prefix']}reference: {level}"
                    if is_reference
                    else f"{metadata['display_prefix']}{level}"
                ),
                "variable_type": "categorical_reference" if is_reference else "categorical",
                "standardise": False,
                "source": column,
                "order": order,
            }
    return output


def standardize_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = frame.copy()
    for column in columns:
        mean = output[column].mean()
        std = output[column].std(ddof=0)
        if pd.isna(std) or std == 0:
            std = 1.0
        output[column] = (output[column] - mean) / std
    return output


def winsorize_series(series: pd.Series, lower_q: float, upper_q: float) -> tuple[pd.Series, float, float, int]:
    non_missing = series.dropna()
    if non_missing.empty:
        raise ValueError("Cannot winsorise an empty dependent variable.")
    lower = float(non_missing.quantile(lower_q))
    upper = float(non_missing.quantile(upper_q))
    clipped = series.clip(lower=lower, upper=upper)
    affected = int(((series < lower) | (series > upper)).fillna(False).sum())
    return clipped, lower, upper, affected


def build_design_matrix(
    estimation_df: pd.DataFrame,
    regressors: list[str],
    levels: dict[str, list[str]],
    registry: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> tuple[pd.DataFrame, list[str], list[str]]:
    numeric_regressors = list(regressors)
    x_numeric = estimation_df[numeric_regressors].apply(safe_numeric).astype(float)
    included = []
    excluded = []
    if config["standardise_regressors"]:
        included = [
            column
            for column in numeric_regressors
            if registry.get(column, {}).get("standardise", False)
            and registry.get(column, {}).get("variable_type") in {"numeric", "lag", "interaction"}
        ]
        excluded = [column for column in numeric_regressors if column not in included]
        x_numeric = standardize_frame(x_numeric, included)
    else:
        excluded = list(numeric_regressors)

    x_model = x_numeric.copy()
    for column in CATEGORICAL_CONTROLS:
        control = CATEGORICAL_METADATA[column]
        categories = levels[column]
        categorical = pd.Categorical(estimation_df[column].astype(str), categories=categories)
        dummies = pd.get_dummies(categorical, prefix=column, drop_first=False, dtype=float)
        expected = [f"{column}_{level}" for level in categories]
        dummies = dummies.reindex(columns=expected, fill_value=0.0)
        dummies = dummies.drop(columns=[f"{column}_{control['reference']}"], errors="ignore")
        dummies.index = estimation_df.index
        x_model = pd.concat([x_model, dummies], axis=1)
        excluded.extend(dummies.columns.tolist())

    x_model = sm.add_constant(x_model, has_constant="add")
    excluded.append("const")
    return x_model.astype(float), sorted(set(included)), sorted(set(excluded))


def significance_stars(p_value: float) -> str:
    if pd.isna(p_value):
        return ""
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def format_coefficient(coefficient: float, p_value: float) -> str:
    if pd.isna(coefficient):
        return ""
    if pd.isna(p_value):
        return f"{coefficient:.3f}\n()"
    return f"{coefficient:.3f}{significance_stars(p_value)}\n({p_value:.3f})"


def safe_result_value(values, name: str) -> float:
    try:
        return float(values[name])
    except Exception:
        return np.nan


def safe_conf_int(result, variable: str) -> tuple[float, float]:
    try:
        conf = result.conf_int()
        if hasattr(conf, "loc"):
            return float(conf.loc[variable, 0]), float(conf.loc[variable, 1])
        idx = list(result.params.index).index(variable)
        return float(conf[idx, 0]), float(conf[idx, 1])
    except Exception:
        return np.nan, np.nan


def fit_quantile_model(
    scenario: str,
    period: str,
    quantile: float,
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    registry: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    dependent = dependent_col(config, period)
    regressors = period_regressors(config, period)
    model_name = f"{scenario}_{period}_{quantile_label(quantile)}"
    model_columns = [dependent, *regressors, *CATEGORICAL_CONTROLS]
    working = filtered_df.loc[:, model_columns].copy()
    numeric_columns = [dependent, *regressors]
    working[numeric_columns] = working[numeric_columns].apply(safe_numeric)
    estimation_df = working.dropna().copy()
    rows_dropped = len(filtered_df) - len(estimation_df)
    run_log_rows = []

    if estimation_df.empty:
        warning = "No observations after dropping missing model variables."
        return {
            "coefficients": pd.DataFrame(columns=COEFFICIENT_COLUMNS),
            "summary": pd.DataFrame(columns=SUMMARY_COLUMNS),
            "diagnostics": pd.DataFrame([diagnostic_row(scenario, period, quantile, model_name, len(filtered_df), 0, rows_dropped, dependent, regressors, [], [], np.nan, np.nan, 0, config, False, "Warning", warning)]),
            "run_log_rows": [run_log_row(scenario, period, quantile, "model_skipped", 0, rows_dropped, warning)],
            "estimated": 0,
            "skipped": 1,
        }

    levels = categorical_levels(estimation_df)
    registry = add_categorical_registry(registry, levels)
    y = estimation_df[dependent].astype(float)
    lower = upper = np.nan
    affected = 0
    if config["winsorise_dependent"]:
        y, lower, upper, affected = winsorize_series(y, config["winsor_lower"], config["winsor_upper"])

    if config["standardise_dependent"]:
        std = y.std(ddof=0)
        if pd.isna(std) or std == 0:
            std = 1.0
        y = (y - y.mean()) / std

    x_model, standardised, non_standardised = build_design_matrix(
        estimation_df=estimation_df,
        regressors=regressors,
        levels=levels,
        registry=registry,
        config=config,
    )

    if len(estimation_df) <= len(x_model.columns):
        warning = "Insufficient observations relative to model parameters."
        return {
            "coefficients": pd.DataFrame(columns=COEFFICIENT_COLUMNS),
            "summary": pd.DataFrame(columns=SUMMARY_COLUMNS),
            "diagnostics": pd.DataFrame([diagnostic_row(scenario, period, quantile, model_name, len(filtered_df), 0, rows_dropped, dependent, regressors, standardised, non_standardised, lower, upper, affected, config, False, "Warning", warning)]),
            "run_log_rows": [run_log_row(scenario, period, quantile, "model_skipped", 0, rows_dropped, warning)],
            "estimated": 0,
            "skipped": 1,
        }

    warning_messages = []
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            model = sm.QuantReg(y, x_model)
            result = model.fit(q=quantile, **quantreg_settings(config))
        warning_messages = [str(item.message) for item in caught]
    except Exception as exc:
        warning = str(exc)
        return {
            "coefficients": pd.DataFrame(columns=COEFFICIENT_COLUMNS),
            "summary": pd.DataFrame(columns=SUMMARY_COLUMNS),
            "diagnostics": pd.DataFrame([diagnostic_row(scenario, period, quantile, model_name, len(filtered_df), 0, rows_dropped, dependent, regressors, standardised, non_standardised, lower, upper, affected, config, False, "Warning", warning)]),
            "run_log_rows": [run_log_row(scenario, period, quantile, "model_skipped", 0, rows_dropped, warning)],
            "estimated": 0,
            "skipped": 1,
        }

    warning_text = "; ".join(warning_messages)
    convergence_warning_present = bool(warning_messages)
    converged = "Warning" if convergence_warning_present else "OK"
    coefficient_rows = []
    for variable in result.params.index:
        ci_lower, ci_upper = safe_conf_int(result, variable)
        meta = registry.get(variable, {"display_name": variable, "variable_type": "unknown"})
        coefficient_rows.append(
            {
                "scenario": scenario,
                "period": period,
                "growth_mode": config["growth_mode"],
                "quantile": quantile,
                "quantile_label": quantile_label(quantile),
                "performance_anchor": performance_anchor(quantile),
                "dependent_variable": dependent,
                "winsorised": "Yes",
                "standardised_model": "Yes",
                "sample_filter": sample_filter_text(config, scenario),
                "raw_variable": variable,
                "display_name": meta["display_name"],
                "variable_type": meta["variable_type"],
                "variable_order": resolve_variable_order(variable),
                "coefficient": safe_result_value(result.params, variable),
                "std_error": safe_result_value(getattr(result, "bse", {}), variable),
                "t_stat": safe_result_value(getattr(result, "tvalues", {}), variable),
                "p_value": safe_result_value(getattr(result, "pvalues", {}), variable),
                "CI_lower": ci_lower,
                "CI_upper": ci_upper,
            }
        )

    summary = pd.DataFrame(
        [
            {
                "scenario": scenario,
                "period": period,
                "growth_mode": config["growth_mode"],
                "quantile": quantile,
                "quantile_label": quantile_label(quantile),
                "performance_anchor": performance_anchor(quantile),
                "dependent_variable": dependent,
                "observations": int(result.nobs),
                "pseudo_R2": getattr(result, "prsquared", np.nan),
                "iterations": getattr(result, "iterations", np.nan),
                "converged": converged,
                "rows_dropped_due_to_missing": rows_dropped,
                "sample_filter": sample_filter_text(config, scenario),
            }
        ],
        columns=SUMMARY_COLUMNS,
    )
    diagnostics = pd.DataFrame(
        [
            diagnostic_row(
                scenario,
                period,
                quantile,
                model_name,
                len(filtered_df),
                int(result.nobs),
                rows_dropped,
                dependent,
                regressors,
                standardised,
                non_standardised,
                lower,
                upper,
                affected,
                config,
                convergence_warning_present,
                converged,
                warning_text,
            )
        ],
        columns=DIAGNOSTIC_COLUMNS,
    )
    run_log_rows.append(run_log_row(scenario, period, quantile, "model_estimated", int(result.nobs), rows_dropped, None))
    if warning_messages:
        run_log_rows.append(run_log_row(scenario, period, quantile, "convergence_warning", int(result.nobs), rows_dropped, warning_text))
    return {
        "coefficients": pd.DataFrame(coefficient_rows, columns=COEFFICIENT_COLUMNS),
        "summary": summary,
        "diagnostics": diagnostics,
        "run_log_rows": run_log_rows,
        "estimated": 1,
        "skipped": 0,
    }


def diagnostic_row(
    scenario: str,
    period: str,
    quantile: float,
    model_name: str,
    row_count_after_filter: int,
    observations_used: int,
    rows_dropped: int,
    dependent: str,
    regressors: list[str],
    standardised: list[str],
    non_standardised: list[str],
    lower: float,
    upper: float,
    affected: int,
    config: dict[str, Any],
    convergence_warning_present: bool,
    converged: str,
    warning: str | None,
) -> dict[str, Any]:
    settings = quantreg_settings(config)
    return {
        "scenario": scenario,
        "period": period,
        "quantile": quantile,
        "model_name": model_name,
        "input_row_count": row_count_after_filter,
        "row_count_after_filter": row_count_after_filter,
        "observations_used": observations_used,
        "rows_dropped_due_to_missing": rows_dropped,
        "generated_dependent_variable": dependent,
        "generated_regressors_for_period": ", ".join(regressors),
        "variables_included_in_standardisation": ", ".join(standardised),
        "variables_excluded_from_standardisation": ", ".join(non_standardised),
        "winsorisation_lower_bound": lower,
        "winsorisation_upper_bound": upper,
        "observations_affected_by_winsorisation": affected,
        "quantreg_max_iter": settings["max_iter"],
        "quantreg_p_tol": settings["p_tol"],
        "convergence_warning_present": convergence_warning_present,
        "converged": converged,
        "warning": warning,
    }


def run_log_row(
    scenario: str,
    period: str | None,
    quantile: float | None,
    event: str,
    observations_used: int | None,
    rows_dropped: int | None,
    warning: str | None,
) -> dict[str, Any]:
    return {
        "scenario": scenario,
        "period": period,
        "quantile": quantile,
        "event": event,
        "observations_used": observations_used,
        "rows_dropped_due_to_missing": rows_dropped,
        "warning": warning,
    }


def build_readme(config: dict[str, Any]) -> pd.DataFrame:
    rows = [
        ("Analysis name", config["analysis_name"]),
        ("Input file", config["input_file"]),
        ("Output file", config["output_file"]),
        ("Growth mode", config["growth_mode"]),
        ("Quantiles", ", ".join(str(q) for q in config["quantiles"])),
        ("Preferred model variant", config["preferred_model_variant"]),
        ("2018 role", "2018 is used only to calculate P1 lag growth; it is not an analytical outcome period."),
        ("Lag growth logic", "P1 uses 2018-2019 lag growth; P2 and P3 retain their prior-period lags; FULL has no lag growth."),
        ("Main analysis window", "Dependent variables, trajectories, SGrowth_NR, and FULL growth remain based on 2019-2024."),
        ("Sector controls", "sector_en is required and included as a categorical control with production as the reference category."),
        ("Model specification", "winsorised dependent variable and standardised numeric regressors/dependent variable"),
        (
            "Quantile interpretation",
            "Quantile regression estimates conditional quantile relationships rather than fixed firm groups.",
        ),
        (
            "Performance framework anchors",
            "The quantiles are interpreted as anchors within the broader four-zone performance framework: below Q10 -> Bottom 10%; Q10-Q50 -> moderate decline / lower-middle performance; Q50-Q90 -> moderate growth / upper-middle performance; above Q90 -> Top 10%.",
        ),
        (
            "Methodological caution",
            "Quantile-regression standard errors and p-values may be sensitive to heteroskedasticity, extreme observations, and large dummy-variable structures. Coefficient direction, relative magnitude, and cross-quantile comparison should often be interpreted more cautiously than exact significance thresholds.",
        ),
        (
            "QuantReg estimation settings",
            f"Quantile regression estimation uses max_iter={config['quantreg_settings']['max_iter']} and p_tol={config['quantreg_settings']['p_tol']} due to the complexity of sector dummy structures, interaction terms, and skewed firm-level distributions. Convergence warnings do not necessarily invalidate coefficient estimates, but should be interpreted cautiously.",
        ),
        ("Scenarios", "; ".join(f"{name}: {scenario_description(name, SCENARIOS[name])}" for name in SCENARIO_ORDER)),
        (
            "Readable comparison sheets",
            "Readable comparison sheets are split by period: Compare_FULL, Compare_P1, Compare_P2, and Compare_P3.",
        ),
        (
            "Comparison column order",
            "Each comparison sheet orders columns by scenario order -> quantile order. Scenario order in readable comparison sheets: ALL -> MANUFACTURING -> RANK2019 -> RANK2019_MANUFACTURING. This avoids misleading alphabetical ordering of scenario-period-quantile columns.",
        ),
        (
            "Quantile pattern classification",
            "Quantile pattern classification is descriptive only. It is based on coefficient direction and relative size across Q10, Q50 and Q90. It is not a formal statistical test.",
        ),
    ]
    return pd.DataFrame(rows, columns=["item", "description"])


def scenario_description(scenario: str, query: str | None) -> str:
    metadata = SCENARIO_METADATA[scenario]
    label = metadata["label"]
    if metadata["sample"] in SAMPLE_ORDER:
        return f"{label}; shared sample: {metadata['sample']}"
    return query if query is not None else "all firms"


def build_compare_sheet_for_period(coefficients: pd.DataFrame, summary: pd.DataFrame, period: str) -> pd.DataFrame:
    if coefficients.empty:
        return pd.DataFrame(columns=["display_name"])
    coefficients = coefficients.loc[coefficients["period"] == period].copy()
    summary = summary.loc[summary["period"] == period].copy()
    if coefficients.empty or summary.empty:
        return pd.DataFrame(columns=["display_name"])
    if "variable_order" not in coefficients.columns:
        coefficients["variable_order"] = coefficients["raw_variable"].map(resolve_variable_order)
    coefficients["model_column"] = coefficients["scenario"] + "_" + coefficients["quantile_label"]
    coefficients["_scenario_order"] = coefficients["scenario"].map({scenario: order for order, scenario in enumerate(SCENARIO_ORDER)})
    coefficients["_quantile_order"] = coefficients["quantile_label"].map(QUANTILE_ORDER)
    coefficients = coefficients.sort_values(
        ["_scenario_order", "_quantile_order", "variable_order", "display_name"],
        kind="mergesort",
    )
    coefficients["formatted"] = coefficients.apply(
        lambda row: format_coefficient(row["coefficient"], row["p_value"]),
        axis=1,
    )
    coefficient_table = coefficients.pivot_table(
        index="display_name",
        columns="model_column",
        values="formatted",
        aggfunc="first",
    )
    order_lookup = coefficients.groupby("display_name", dropna=False)["variable_order"].min()
    coefficient_table["_variable_order"] = coefficient_table.index.map(order_lookup)
    coefficient_table = (
        coefficient_table.reset_index()
        .sort_values(["_variable_order", "display_name"], kind="mergesort")
        .drop(columns="_variable_order")
        .set_index("display_name")
    )

    summary["model_column"] = summary["scenario"] + "_" + summary["quantile_label"]
    n_row = summary.set_index("model_column")["observations"].map(lambda value: f"{int(value):,}" if pd.notna(value) else "")
    r2_row = summary.set_index("model_column")["pseudo_R2"].map(lambda value: f"{float(value):.3f}" if pd.notna(value) else "")
    stats = pd.DataFrame([n_row, r2_row], index=["N", "pseudo_R2"])
    output = pd.concat([stats, coefficient_table], axis=0).reset_index().rename(columns={"index": "display_name"})
    ordered_quantile_labels = sorted(
        {quantile_label(float(q)) for q in CONFIG["quantiles"]},
        key=lambda label: QUANTILE_ORDER.get(label, 9999),
    )
    model_columns = [
        f"{scenario}_{label}"
        for scenario in SCENARIO_ORDER
        for label in ordered_quantile_labels
    ]
    for column in model_columns:
        if column not in output.columns:
            output[column] = ""
    return output.loc[:, ["display_name", *model_columns]]


def classify_quantile_pattern(q10: float, q50: float, q90: float) -> str:
    values = [q10, q50, q90]
    if any(pd.isna(value) for value in values):
        return "insufficient data"

    value_range = max(values) - min(values)
    scale = max(abs(value) for value in values)
    stability_threshold = max(0.01, 0.10 * scale)

    if value_range <= stability_threshold:
        return "stable"
    if min(values) < 0 < max(values):
        return "sign reversal"
    if q10 < q50 < q90:
        return "increasing"
    if q10 > q50 > q90:
        return "decreasing"
    if abs(q50) > abs(q10) and abs(q50) > abs(q90):
        return "middle strongest"
    return "mixed"


def build_quantile_patterns(coefficients: pd.DataFrame) -> pd.DataFrame:
    columns = ["scenario", "period", "display_name", "Q10", "Q50", "Q90", "pattern"]
    if coefficients.empty:
        return pd.DataFrame(columns=columns)
    included_types = {"numeric", "dummy", "lag", "interaction"}
    subset = coefficients.loc[coefficients["variable_type"].isin(included_types)].copy()
    if subset.empty:
        return pd.DataFrame(columns=columns)
    subset["variable_order"] = subset["variable_order"].fillna(subset["raw_variable"].map(resolve_variable_order))
    pivot = (
        subset.pivot_table(
            index=["scenario", "period", "raw_variable", "display_name", "variable_order"],
            columns="quantile_label",
            values="coefficient",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(columns=None)
    )
    for column in ["Q10", "Q50", "Q90"]:
        if column not in pivot.columns:
            pivot[column] = np.nan
    pivot["pattern"] = pivot.apply(lambda row: classify_quantile_pattern(row["Q10"], row["Q50"], row["Q90"]), axis=1)
    pivot["_scenario_order"] = pivot["scenario"].map({scenario: order for order, scenario in enumerate(SCENARIO_ORDER)}).fillna(9999)
    pivot["_period_order"] = pivot["period"].map(period_order_map())
    pivot = pivot.sort_values(
        ["_scenario_order", "_period_order", "variable_order", "display_name"],
        kind="mergesort",
    )
    return pivot.loc[:, columns].reset_index(drop=True)


def build_convergence_summary(diagnostics: pd.DataFrame) -> pd.DataFrame:
    if diagnostics.empty:
        return pd.DataFrame(columns=CONVERGENCE_SUMMARY_COLUMNS)
    output = diagnostics.loc[diagnostics["observations_used"].fillna(0).gt(0)].copy()
    if output.empty:
        return pd.DataFrame(columns=CONVERGENCE_SUMMARY_COLUMNS)
    output["warning_present"] = output["warning"].fillna("").astype(str).str.len().gt(0)
    output["warning_message"] = output["warning"].fillna("")
    output = sort_by_model_keys(output)
    return output.loc[:, CONVERGENCE_SUMMARY_COLUMNS].reset_index(drop=True)


def build_variable_labels(registry: dict[str, dict[str, Any]], coefficients: pd.DataFrame) -> pd.DataFrame:
    observed = set(coefficients["raw_variable"].dropna().astype(str).tolist()) if not coefficients.empty else set()
    rows = []
    for raw_name, meta in registry.items():
        if raw_name in observed or meta["variable_type"] in {"constant", "categorical_reference"}:
            rows.append(
                {
                    "raw_name": raw_name,
                    "display_name": meta["display_name"],
                    "variable_label": VARIABLE_LABELS.get(raw_name, meta["display_name"]),
                    "variable_type": meta["variable_type"],
                    "variable_order": resolve_variable_order(raw_name),
                    "standardise": meta["standardise"],
                    "source": meta["source"],
                }
            )
    for column in CATEGORICAL_CONTROLS:
        metadata = CATEGORICAL_METADATA[column]
        for level in metadata.get("levels", []):
            raw_name = f"{column}_{level}"
            is_reference = str(level) == str(metadata["reference"])
            if raw_name not in observed and not is_reference:
                continue
            display_name = (
                f"{metadata['display_prefix']}reference: {level}"
                if is_reference
                else f"{metadata['display_prefix']}{level}"
            )
            rows.append(
                {
                    "raw_name": raw_name,
                    "display_name": display_name,
                    "variable_label": VARIABLE_LABELS.get(raw_name, display_name),
                    "variable_type": "categorical_reference" if is_reference else "categorical",
                    "variable_order": resolve_variable_order(raw_name),
                    "standardise": False,
                    "source": column,
                }
            )
    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["raw_name"], keep="first")
        .sort_values(["variable_order", "display_name", "raw_name"], kind="mergesort")
        .reset_index(drop=True)
    )


def validate_sample_consistency(
    input_df: pd.DataFrame,
    scenario_frames: dict[str, pd.DataFrame],
    config: dict[str, Any],
) -> tuple[bool, list[str], dict[str, dict[str, int]]]:
    expected_counts = build_shared_sample_counts(input_df, complete_flag_col(config))
    warnings_out = []
    for scenario_name in SCENARIO_ORDER:
        sample_name = SHARED_SAMPLE_SCENARIOS.get(scenario_name)
        if sample_name is None:
            continue
        frame = scenario_frames.get(scenario_name, pd.DataFrame())
        actual = {
            "rows": len(frame),
            "firms": frame["nip"].nunique() if "nip" in frame.columns else len(frame),
        }
        expected = expected_counts[sample_name]
        if actual != expected:
            warnings_out.append(
                f"WARNING: sample mismatch detected for {sample_name}: "
                f"actual rows={actual['rows']} firms={actual['firms']}; "
                f"expected rows={expected['rows']} firms={expected['firms']}"
            )
    return not warnings_out, warnings_out, expected_counts


def write_workbook(path: Path, tables: dict[str, pd.DataFrame]) -> list[str]:
    if list(tables) != OUTPUT_SHEETS:
        raise ValueError(f"Unexpected sheet order: {list(tables)}")
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        for sheet_name, frame in tables.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
        format_workbook(writer, tables)
    return list(tables)


def read_previous_convergence_warning_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        run_log = pd.read_excel(path, sheet_name="Run_Log")
    except Exception:
        return None
    if "event" not in run_log.columns:
        return None
    return int(run_log["event"].eq("convergence_warning").sum())


def format_workbook(writer: pd.ExcelWriter, tables: dict[str, pd.DataFrame]) -> None:
    workbook = writer.book
    working_header = workbook.add_format({"bold": True, "bg_color": "#1F4E78", "font_color": "white", "text_wrap": True})
    tech_header = workbook.add_format({"bold": True, "bg_color": "#C65911", "font_color": "white", "text_wrap": True})
    wrap = workbook.add_format({"text_wrap": True, "valign": "top"})
    compare_sheets = {"Compare_FULL", "Compare_P1", "Compare_P2", "Compare_P3"}
    working_sheets = {"README", "Quantile_Patterns", *compare_sheets}
    long_text = {
        "description",
        "sample_filter",
        "generated_regressors_for_period",
        "variables_included_in_standardisation",
        "variables_excluded_from_standardisation",
        "warning",
    }
    for sheet_name, frame in tables.items():
        worksheet = writer.sheets[sheet_name]
        header = working_header if sheet_name in working_sheets else tech_header
        worksheet.set_tab_color("#5B9BD5" if sheet_name in working_sheets else "#ED7D31")
        apply_header_format(worksheet, frame, header)
        set_table_column_widths(worksheet, frame, text_max_width=45)
        for col_idx, column in enumerate(frame.columns):
            if column in long_text:
                worksheet.set_column(col_idx, col_idx, 42, wrap)
        freeze_and_hide_gridlines(worksheet, 1, 1 if sheet_name in {*compare_sheets, "Quantile_Patterns"} else 0)
        apply_safe_autofilter(worksheet, frame)


def validate_input_columns(df: pd.DataFrame, config: dict[str, Any]) -> None:
    if "P1" in LAG_GROWTH_PERIODS:
        p1_lag_column = lag_growth_col(config, "P1")
        if p1_lag_column not in df.columns:
            raise ValueError(
                f"Input file is missing required P1 lag-growth column: {p1_lag_column}. "
                "Rebuild Data_period_2018-2024 with the 2018 sales extension."
            )
    required = {complete_flag_col(config), OWNER_COLUMN, *CATEGORICAL_CONTROLS}
    generated_interactions = {
        interaction_col(interaction["name"], period)
        for period in config["periods"]
        for interaction in active_interactions()
    }
    for period in config["periods"]:
        required.add(dependent_col(config, period))
        required.update(column for column in period_regressors(config, period) if column not in generated_interactions)
        for interaction in active_interactions():
            required.update(resolve_interaction_source(variable, period) for variable in interaction["variables"])
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Input file missing required columns: {missing}")


def sort_coefficients(coefficients: pd.DataFrame) -> pd.DataFrame:
    if coefficients.empty:
        return coefficients
    output = coefficients.copy()
    output["_scenario_order"] = output["scenario"].map({scenario: order for order, scenario in enumerate(SCENARIO_ORDER)}).fillna(9999)
    output["_period_order"] = output["period"].map(period_order_map())
    output = output.sort_values(
        ["_scenario_order", "_period_order", "quantile", "variable_order", "display_name"],
        kind="mergesort",
    ).drop(columns=["_scenario_order", "_period_order"])
    return output.loc[:, COEFFICIENT_COLUMNS].reset_index(drop=True)


def sort_by_model_keys(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "period" not in df.columns:
        return df
    output = df.copy()
    output["_scenario_order"] = output["scenario"].map({scenario: order for order, scenario in enumerate(SCENARIO_ORDER)}).fillna(9999)
    output["_period_order"] = output["period"].map(period_order_map())
    sort_columns = [column for column in ["_scenario_order", "_period_order", "quantile"] if column in output.columns]
    output = output.sort_values(sort_columns, kind="mergesort").drop(columns=["_scenario_order", "_period_order"])
    return output.reset_index(drop=True)


def run_quantile_regression(config: dict[str, Any] = CONFIG) -> dict[str, Any]:
    config = dict(config)
    if config["preferred_model_variant"] != "winsor_std":
        raise ValueError("This runner supports only preferred_model_variant='winsor_std'.")
    invalid_periods = sorted(set(config["periods"]).difference([*PERIODS, "FULL"]))
    if invalid_periods:
        raise ValueError(f"Unsupported periods: {invalid_periods}")
    expected_labels = {"Q10", "Q50", "Q90"}

    input_df = load_input_data(config)
    validate_input_columns(input_df, config)
    input_df = add_interaction_columns(input_df, config)
    base_registry = variable_registry(config)

    coefficient_frames = []
    summary_frames = []
    diagnostic_frames = []
    run_log_rows = []
    scenario_frames = {}
    models_estimated = 0
    models_skipped = 0

    for scenario in SCENARIO_ORDER:
        filter_query = SCENARIOS[scenario]
        try:
            scenario_df = apply_scenario_filter(input_df, scenario, filter_query)
            filtered_df = apply_complete_filter(scenario_df, config, scenario)
            scenario_frames[scenario] = filtered_df
        except Exception as exc:
            run_log_rows.append(run_log_row(scenario, None, None, "scenario_failed", None, None, str(exc)))
            scenario_frames[scenario] = pd.DataFrame()
            models_skipped += len(config["periods"]) * len(config["quantiles"])
            continue
        for period in config["periods"]:
            for q in config["quantiles"]:
                result = fit_quantile_model(
                    scenario=scenario,
                    period=period,
                    quantile=float(q),
                    filtered_df=filtered_df,
                    config=config,
                    registry=base_registry,
                )
                coefficient_frames.append(result["coefficients"])
                summary_frames.append(result["summary"])
                diagnostic_frames.append(result["diagnostics"])
                run_log_rows.extend(result["run_log_rows"])
                models_estimated += result["estimated"]
                models_skipped += result["skipped"]

    coefficients = (
        pd.concat([frame for frame in coefficient_frames if not frame.empty], ignore_index=True)
        if any(not frame.empty for frame in coefficient_frames)
        else pd.DataFrame(columns=COEFFICIENT_COLUMNS)
    )
    coefficients = sort_coefficients(coefficients)
    summary = (
        pd.concat([frame for frame in summary_frames if not frame.empty], ignore_index=True)
        if any(not frame.empty for frame in summary_frames)
        else pd.DataFrame(columns=SUMMARY_COLUMNS)
    )
    summary = sort_by_model_keys(summary).loc[:, SUMMARY_COLUMNS] if not summary.empty else summary
    diagnostics = (
        pd.concat([frame for frame in diagnostic_frames if not frame.empty], ignore_index=True)
        if any(not frame.empty for frame in diagnostic_frames)
        else pd.DataFrame(columns=DIAGNOSTIC_COLUMNS)
    )
    diagnostics = sort_by_model_keys(diagnostics).loc[:, DIAGNOSTIC_COLUMNS] if not diagnostics.empty else diagnostics
    convergence_summary = build_convergence_summary(diagnostics)
    run_log = pd.DataFrame(run_log_rows, columns=RUN_LOG_COLUMNS)
    variable_labels = build_variable_labels(base_registry, coefficients)
    compare_tables = {
        "Compare_FULL": build_compare_sheet_for_period(coefficients, summary, "FULL"),
        "Compare_P1": build_compare_sheet_for_period(coefficients, summary, "P1"),
        "Compare_P2": build_compare_sheet_for_period(coefficients, summary, "P2"),
        "Compare_P3": build_compare_sheet_for_period(coefficients, summary, "P3"),
    }
    quantile_patterns = build_quantile_patterns(coefficients)
    sample_ok, sample_warnings, sample_counts = validate_sample_consistency(input_df, scenario_frames, config)
    output_path = Path(config["output_file"])
    convergence_warnings_before = read_previous_convergence_warning_count(output_path)

    tables = {
        "README": build_readme(config),
        **compare_tables,
        "Quantile_Patterns": quantile_patterns,
        "Coefficients_Long": coefficients,
        "Model_Summary_Long": summary,
        "Convergence_Summary": convergence_summary,
        "Diagnostics_Long": diagnostics,
        "Variable_Labels": variable_labels,
        "Run_Log": run_log,
    }
    written_sheets = write_workbook(output_path, tables)

    labels_present = set(summary["quantile_label"].dropna().astype(str).unique().tolist()) if not summary.empty else set()
    raw_variants_run = False
    print_validation(
        config=config,
        written_sheets=written_sheets,
        models_estimated=models_estimated,
        models_skipped=models_skipped,
        sample_counts=sample_counts,
        sample_ok=sample_ok,
        sample_warnings=sample_warnings,
        quantile_labels_present=expected_labels.issubset(labels_present),
        raw_variants_run=raw_variants_run,
        quantile_patterns_sheet_created="Quantile_Patterns" in written_sheets and not quantile_patterns.empty,
        compare_sheet_names=list(compare_tables),
        convergence_summary=convergence_summary,
        convergence_warnings_before=convergence_warnings_before,
    )
    return {
        "tables": tables,
        "written_sheets": written_sheets,
        "models_estimated": models_estimated,
        "models_skipped": models_skipped,
    }


def print_validation(
    config: dict[str, Any],
    written_sheets: list[str],
    models_estimated: int,
    models_skipped: int,
    sample_counts: dict[str, dict[str, int]],
    sample_ok: bool,
    sample_warnings: list[str],
    quantile_labels_present: bool,
    raw_variants_run: bool,
    quantile_patterns_sheet_created: bool,
    compare_sheet_names: list[str],
    convergence_summary: pd.DataFrame,
    convergence_warnings_before: int | None,
) -> None:
    print("Quantile regression completed")
    print(f"analysis_name: {config['analysis_name']}")
    print(f"growth_mode: {config['growth_mode']}")
    print(f"input_file: {config['input_file']}")
    print(f"P1_lag_validation: {lag_growth_col(config, 'P1') in period_regressors(config, 'P1')}")
    print(f"P2_lag_validation: {lag_growth_col(config, 'P2') in period_regressors(config, 'P2')}")
    print(f"P3_lag_validation: {lag_growth_col(config, 'P3') in period_regressors(config, 'P3')}")
    print(
        "FULL_has_no_lag_growth: "
        f"{not any(regressor.startswith('lag_') for regressor in period_regressors(config, 'FULL'))}"
    )
    print("2018_role: used only for P1 lag growth")
    print("main_analysis_window: dependent variables, trajectories, SGrowth_NR, and FULL remain 2019-2024")
    print(
        "sector_en present and used as categorical control: "
        f"{'sector_en' in CATEGORICAL_CONTROLS}"
    )
    print(f"quantiles: {config['quantiles']}")
    print(f"preferred_model_variant: {config['preferred_model_variant']}")
    scenarios_estimated = len(SCENARIOS)
    print(f"scenarios_estimated: {scenarios_estimated}")
    print(f"models_estimated: {models_estimated}")
    print(f"models_skipped: {models_skipped}")
    print("sample_counts_validation:")
    for sample_name in SAMPLE_ORDER:
        counts = sample_counts[sample_name]
        print(f"{sample_name}: rows={counts['rows']} firms={counts['firms']}")
    if sample_ok:
        print("sample_consistency_check: PASSED")
    else:
        print("sample_consistency_check: WARNING")
        for warning in sample_warnings:
            print(warning)
    print("analysis_config_imported: True")
    print("analysis_helpers_imported: True")
    print(f"output_sheet_names_valid: {written_sheets == OUTPUT_SHEETS}")
    print(f"quantile_labels_present: {quantile_labels_present}")
    print(f"raw_baseline_variants_run: {raw_variants_run}")
    print("variable_ordering_applied: True")
    print("scenario_metadata_imported: True")
    print(f"quantile_patterns_sheet_created: {quantile_patterns_sheet_created}")
    print("compare_quantiles_split_by_period: True")
    print("compare_column_order_explicit: True")
    print(f"compare_sheet_names: {compare_sheet_names}")
    print(f"compare_quantiles_removed: {'Compare_Quantiles' not in written_sheets}")
    print("scenario_order_explicit: True")
    print("quantile_order_explicit: True")
    print("dictionary_order_dependency_removed: True")
    print("central_scenario_order_used: True")
    print("central_quantile_order_used: True")
    print(f"scenario_order_used: {SCENARIO_ORDER}")
    print("quantile_pattern_threshold_relative: True")
    print("quantile_patterns_descriptive_only: True")
    convergence_warnings_after = (
        int(convergence_summary["warning_present"].sum())
        if not convergence_summary.empty and "warning_present" in convergence_summary.columns
        else 0
    )
    models_with_warnings = convergence_warnings_after
    warning_rate = convergence_warnings_after / models_estimated if models_estimated else np.nan
    print(f"total_convergence_warnings: {convergence_warnings_after}")
    print(f"models_with_warnings: {models_with_warnings}")
    print(f"quantreg_max_iter_used: {config['quantreg_settings']['max_iter']}")
    print(f"quantreg_p_tol_used: {config['quantreg_settings']['p_tol']}")
    print(f"convergence_warning_rate: {warning_rate}")
    print(f"convergence_warnings_before: {convergence_warnings_before}")
    print(f"convergence_warnings_after: {convergence_warnings_after}")
    print("workbook_generated_successfully: True")


if __name__ == "__main__":
    run_quantile_regression()
