from __future__ import annotations

from pathlib import Path
from typing import Any
import textwrap

import numpy as np
import pandas as pd

from code_config import (
    PERFORMANCE_BAND_COLUMNS,
    PERFORMANCE_BAND_ORDER,
    PERIODS,
    PROFILE_VARIABLES,
    SCENARIO_ORDER,
    STATISTIC_ORDER,
    TRAJECTORY_LEVEL_ORDER,
    build_ownership_labels,
    build_sample_mask,
    apply_manual_exclusions,
    manual_exclusion_readme_rows,
    get_trajectory_family_config,
    build_scenario_mask,
    get_scenario_definitions,
    get_period_model_settings,
    validate_scenario_alignment,
)
from code_helpers import (
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
from code_ols_scenarios import (
    CORRELATION_VARIANTS,
    build_appendix_full_matrices,
    build_correlation_long,
    build_correlation_matrix,
    build_correlation_stability,
    build_correlation_with_dv,
    build_missingness_table,
    build_models,
    build_predictor_risk_summary,
    build_variable_labels_table_for_scenarios,
    build_variable_registry,
    build_winsorisation_impact,
    correlation_variant_frame,
    get_categorical_columns,
    get_estimation_sample,
    get_model_variants,
    normalise_config,
    run_models_for_scenario,
    scenario_definition_text,
    validate_config,
    validate_input_columns,
    validate_registry_against_models,
    validate_user_config,
    variable_label,
)


CONFIG = {
    "trajectory_family": "nominal",  # options: "real", "nominal"
    "input_file": "data_period_2018-2024.parquet",
    "output_file": "results_diagnostics_trajectories.xlsx",
}

OUTPUT_SHEETS = [
    "00_README_STRUCTURE",
    "01_VARIABLES",
    "02_SAMPLE_SUMMARY",
    "03_MISSINGNESS",
    "04_WINSOR_IMPACT",
    "10_TRAJECTORY_SUMMARY",
    "11_TRAJECTORY_BY_SCENARIO",
    "12_TRAJECTORY_BY_SECTOR",
    "13_TRAJECTORY_PROFILE",
    "14_TRAJECTORY_PROFILE_PIVOT",
    "15_PERFORMANCE_BANDS",
    "16_PATH_INDEX",
    "20_CORR_MAIN_BASELINE",
    "21_CORR_MAIN_WINSOR",
    "22_CORR_WITH_DV_BASELINE",
    "23_CORR_WITH_DV_WINSOR",
    "24_CORR_PREDICTOR_RISK",
    "25_CORR_STABILITY_SCENARIOS",
    "30_SCENARIO_SUMMARY",
    "31_SCENARIO_DIAGNOSTICS",
    "90_CORR_LONG_ALL",
    "91_APPENDIX_FULL_MATRICES",
    "92_FIRM_TRAJECTORIES",
    "93_CONFIG_AUDIT",
]

DIAGNOSTIC_SHEET_NAMES = {
    "00_README_STRUCTURE",
    "01_VARIABLES",
    "03_MISSINGNESS",
    "04_WINSOR_IMPACT",
    "20_CORR_MAIN_BASELINE",
    "21_CORR_MAIN_WINSOR",
    "22_CORR_WITH_DV_BASELINE",
    "23_CORR_WITH_DV_WINSOR",
    "24_CORR_PREDICTOR_RISK",
    "25_CORR_STABILITY_SCENARIOS",
    "30_SCENARIO_SUMMARY",
    "31_SCENARIO_DIAGNOSTICS",
    "90_CORR_LONG_ALL",
    "91_APPENDIX_FULL_MATRICES",
}


def build_structure_readme(config: dict[str, Any]) -> pd.DataFrame:
    purpose = (
        "This workbook contains pre-modelling diagnostics and trajectory analysis. "
        "It supports the regression analysis reported separately in "
        "results_ols_scenarios.xlsx. The workbook first documents variables, "
        "sample composition, missingness and winsorisation impact. It then presents "
        "trajectory analysis and correlation diagnostics. Correlations are used to "
        "assess redundancy, direction and stability of associations before regression "
        "modelling. They are not used as mechanical variable-selection criteria. "
        "Standardised correlation sheets are not repeated because Pearson correlations "
        "are invariant to linear standardisation. Winsorised correlations are reported "
        "separately because winsorisation can change extreme values."
    )
    rows = [
        (
            "overview",
            "00",
            "dark blue",
            "Purpose and navigation",
            "Start here before interpreting any diagnostic.",
            purpose,
        ),
        (
            "analytical sequence",
            "01-04",
            "green",
            "Variables, samples, missingness and winsorisation",
            "Confirm definitions, usable sample size and transformation impact.",
            "Read these sheets before trajectory or correlation results.",
        ),
        (
            "legend",
            "10-16",
            "blue",
            "Trajectory results",
            "Assess growth paths, profiles, sectors and performance bands.",
            "Trajectory outputs use the shared scenario definitions.",
        ),
        (
            "legend",
            "20-25",
            "orange",
            "Correlation diagnostics",
            "Review direction, redundancy and sensitivity to winsorisation.",
            "Correlation evidence is diagnostic, not a mechanical selection rule.",
        ),
        (
            "legend",
            "30-31",
            "purple",
            "Scenario-specific diagnostics",
            "Confirm sample alignment and inspect every shared scenario.",
            "All four scenarios are shared with the OLS pipeline.",
        ),
        (
            "legend",
            "90-93",
            "grey",
            "Audit appendices",
            "Use when reproducing or tracing a headline diagnostic.",
            "Contains long correlations, matrices, firm trajectories and configuration.",
        ),
        (
            "method note",
            "20-25",
            "orange",
            "Baseline versus winsorised",
            "Compare only like-for-like scenario-period samples.",
            (
                "Baseline uses raw estimation-sample values. Winsor uses the same "
                "data with the dependent variable clipped at quantiles "
                f"{config['winsor_lower']:.2f} and {config['winsor_upper']:.2f}. "
                "Predictors are not winsorised under the current model specification."
            ),
        ),
        (
            "method note",
            "20-25",
            "orange",
            "Standardised correlations",
            "Do not look for separate standardised tabs.",
            (
                "Separate standardised correlation sheets are intentionally omitted "
                "because non-degenerate linear standardisation does not change "
                "Pearson correlations."
            ),
        ),
        (
            "relationship",
            "30",
            "purple",
            "OLS alignment",
            "Check inclusion flags and scenario counts before comparing outputs.",
            (
                "results_ols_scenarios.xlsx contains regression estimates. "
                "This workbook contains the pre-modelling diagnostics and trajectory "
                "evidence supporting those estimates."
            ),
        ),
        (
            "method note",
            "31",
            "purple",
            "Scenario diagnostic samples",
            "Interpret missingness and descriptive statistics on their stated samples.",
            (
                "Missingness counts use scenario-filtered rows before complete-case "
                "exclusion. Descriptive statistics use the exact complete-case "
                "estimation sample. Winsor variants clip only the dependent variable; "
                "standardised variants are shown on the pre-standardisation scale."
            ),
        ),
    ]
    rows.extend(("manual exclusions", "all", "grey", item, "Apply consistently with every regression.", description)
                for item, description in manual_exclusion_readme_rows(config.get("manual_exclusion_audit", [])))
    return pd.DataFrame(
        rows,
        columns=[
            "block",
            "section_range",
            "colour",
            "analytical_role",
            "reader_use",
            "description",
        ],
    )


def build_shared_full_correlation_matrices(
    correlation_long_df: pd.DataFrame,
    variant: str,
) -> pd.DataFrame:
    blocks = []
    for scenario in SCENARIO_ORDER:
        matrix = build_correlation_matrix(
            correlation_long_df,
            scenario,
            "FULL",
            variant,
        )
        if matrix.empty:
            continue
        section = {column: pd.NA for column in matrix.columns}
        section.update(
            {
                "scenario": scenario,
                "period": "FULL",
                "variant": variant,
                "effective_variable": (
                    f"SCENARIO: {scenario} | FULL period | {variant} matrix"
                ),
                "variable": "SECTION",
            }
        )
        section_df = pd.DataFrame([section])
        section_df.insert(3, "row_type", "section")
        matrix.insert(3, "row_type", "matrix")
        blocks.extend([section_df, matrix])
    return (
        pd.concat(blocks, ignore_index=True, sort=False)
        if blocks
        else pd.DataFrame()
    )


def build_scenario_diagnostics(
    scenario_results: dict[str, dict[str, Any]],
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    rows = []
    categorical_columns = set(get_categorical_columns(config))
    sample_note = (
        "Missingness: scenario pre-complete-case; "
        "descriptives: exact complete-case sample."
    )
    for scenario_name in SCENARIO_ORDER:
        result = scenario_results.get(scenario_name, {})
        filtered_df = result.get("filtered_df", pd.DataFrame())
        variable_registry = result.get("variable_registry", {})
        if filtered_df.empty:
            continue
        scenario_config = {
            **config,
            "sample_name": scenario_name,
            "base_sample_filter": "True",
        }
        for period, model_spec in models.items():
            estimation_df = get_estimation_sample(
                filtered_df,
                scenario_config,
                model_spec,
            )
            model_columns = list(
                dict.fromkeys(
                    [
                        model_spec["dependent"],
                        *model_spec["regressors"],
                        *get_categorical_columns(scenario_config),
                    ]
                )
            )
            for variant in get_model_variants(scenario_config):
                variant_frame, effective_names = correlation_variant_frame(
                    estimation_df,
                    model_spec,
                    "winsor" if variant["winsorised"] else "baseline",
                    scenario_config,
                )
                descriptive_scale = (
                    "pre-standardisation; dependent winsorised"
                    if variant["winsorised"]
                    else "pre-standardisation; raw values"
                )
                for variable in model_columns:
                    source = filtered_df[variable]
                    descriptive = variant_frame[variable]
                    is_categorical = variable in categorical_columns
                    numeric = (
                        pd.to_numeric(descriptive, errors="coerce")
                        if not is_categorical
                        else None
                    )
                    numeric_stats = {
                        "mean": numeric.mean(skipna=True),
                        "median": numeric.median(skipna=True),
                        "sd": numeric.std(skipna=True),
                        "min": numeric.min(skipna=True),
                        "p05": numeric.quantile(0.05),
                        "p95": numeric.quantile(0.95),
                        "max": numeric.max(skipna=True),
                    } if numeric is not None else {
                        statistic: pd.NA
                        for statistic in [
                            "mean",
                            "median",
                            "sd",
                            "min",
                            "p05",
                            "p95",
                            "max",
                        ]
                    }
                    rows.append(
                        {
                            "scenario": scenario_name,
                            "period": period,
                            "model_variant": variant["suffix"],
                            "variable": variable,
                            "effective_variable": effective_names.get(
                                variable,
                                variable,
                            ),
                            "variable_label": (
                                variable
                                if is_categorical
                                else variable_label(variable, variable_registry)
                            ),
                            "variable_type": (
                                "categorical_control"
                                if is_categorical
                                else variable_registry.get(variable, {}).get(
                                    "variable_type",
                                    "numeric",
                                )
                            ),
                            "n_total": len(filtered_df),
                            "n_missing": int(source.isna().sum()),
                            "missing_share": float(source.isna().mean()),
                            "non_missing_n": int(source.notna().sum()),
                            "estimation_sample_n": len(estimation_df),
                            "descriptive_n": int(descriptive.notna().sum()),
                            "unique_non_missing": int(
                                descriptive.nunique(dropna=True)
                            ),
                            **numeric_stats,
                            "descriptive_scale": descriptive_scale,
                            "sample_inclusion_notes": sample_note,
                        }
                    )
    return pd.DataFrame(rows)


def validate_diagnostic_content(tables: dict[str, pd.DataFrame]) -> None:
    expected_winsor_columns = [
        "scenario",
        "period",
        "model_variant",
        "variable",
        "n_before",
        "n_after",
        "n_changed",
        "min_before",
        "min_after",
        "p01_before",
        "p01_after",
        "p05_before",
        "p05_after",
        "median_before",
        "median_after",
        "p95_before",
        "p95_after",
        "p99_before",
        "p99_after",
        "max_before",
        "max_after",
    ]
    winsor = tables["04_WINSOR_IMPACT"]
    if winsor.columns.tolist() != expected_winsor_columns:
        raise ValueError(
            "04_WINSOR_IMPACT columns are not in the required order: "
            f"{winsor.columns.tolist()}"
        )
    if set(winsor["model_variant"]) != {"winsor"}:
        raise ValueError(
            "04_WINSOR_IMPACT must contain only the single winsor transformation."
        )
    if winsor.duplicated(["scenario", "period", "variable"]).any():
        raise ValueError(
            "04_WINSOR_IMPACT contains duplicate transformation rows for a "
            "scenario-period-variable."
        )
    if not winsor.groupby("period", observed=True)["variable"].nunique().eq(1).all():
        raise ValueError(
            "04_WINSOR_IMPACT contains variables other than the period-specific "
            "winsorised dependent variable."
        )
    if (
        winsor["n_changed"].isna().any()
        or winsor["n_changed"].le(0).any()
        or winsor["n_changed"].gt(winsor["n_before"]).any()
    ):
        raise ValueError(
            "04_WINSOR_IMPACT n_changed must be positive and no larger than n_before."
        )

    expected_corr_with_dv_columns = [
        "scenario",
        "period",
        "variant",
        "source_dataframe",
        "dependent_variable",
        "dependent_variable_effective",
        "predictor",
        "correlation",
        "p_value",
        "N",
    ]
    for sheet_name, expected_variant in [
        ("22_CORR_WITH_DV_BASELINE", "baseline"),
        ("23_CORR_WITH_DV_WINSOR", "winsor"),
    ]:
        table = tables[sheet_name]
        if table.columns.tolist() != expected_corr_with_dv_columns:
            raise ValueError(
                f"{sheet_name} columns are not in the required order: "
                f"{table.columns.tolist()}"
            )
        if set(table["variant"].dropna().astype(str)) != {expected_variant}:
            raise ValueError(
                f"{sheet_name} contains variants other than {expected_variant!r}."
            )
        valid_p_values = table["p_value"].dropna().between(0, 1, inclusive="both")
        if not valid_p_values.all():
            raise ValueError(f"{sheet_name} contains p-values outside [0, 1].")
        if not table["correlation"].notna().eq(table["p_value"].notna()).all():
            raise ValueError(
                f"{sheet_name} correlation and p_value availability do not match."
            )

    correlation_long = tables["90_CORR_LONG_ALL"]
    required_long_columns = {"correlation", "p_value", "N"}
    missing_long_columns = sorted(required_long_columns.difference(correlation_long.columns))
    if missing_long_columns:
        raise ValueError(
            "90_CORR_LONG_ALL is missing required correlation fields: "
            f"{missing_long_columns}"
        )
    if not correlation_long["correlation"].notna().eq(correlation_long["p_value"].notna()).all():
        raise ValueError(
            "90_CORR_LONG_ALL correlation and p_value availability do not match."
        )

    for sheet_name in ["20_CORR_MAIN_BASELINE", "21_CORR_MAIN_WINSOR"]:
        if "p_value" in tables[sheet_name].columns:
            raise ValueError(f"{sheet_name} must remain a clean matrix without p-values.")

    expected_scenarios = set(SCENARIO_ORDER)
    for sheet_name in [
        "20_CORR_MAIN_BASELINE",
        "21_CORR_MAIN_WINSOR",
        "22_CORR_WITH_DV_BASELINE",
        "23_CORR_WITH_DV_WINSOR",
        "31_SCENARIO_DIAGNOSTICS",
    ]:
        actual_scenarios = set(
            tables[sheet_name]["scenario"].dropna().astype(str)
        )
        if actual_scenarios != expected_scenarios:
            raise ValueError(
                f"{sheet_name} scenario coverage mismatch: "
                f"expected={sorted(expected_scenarios)}, "
                f"actual={sorted(actual_scenarios)}"
            )

    required_scenario_columns = {
        "scenario",
        "period",
        "model_variant",
        "variable",
        "n_total",
        "n_missing",
        "missing_share",
        "non_missing_n",
        "mean",
        "median",
        "sample_inclusion_notes",
    }
    missing_columns = sorted(
        required_scenario_columns.difference(
            tables["31_SCENARIO_DIAGNOSTICS"].columns
        )
    )
    if missing_columns:
        raise ValueError(
            "31_SCENARIO_DIAGNOSTICS is missing required columns: "
            f"{missing_columns}"
        )


def build_scenario_summary(
    scenario_results: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    rows = []
    for scenario_name, metadata in get_scenario_definitions().items():
        result = scenario_results.get(scenario_name, {})
        filtered_df = result.get("filtered_df", pd.DataFrame())
        rows.append(
            {
                "scenario": scenario_name,
                "filter_logic": scenario_definition_text(
                    scenario_name,
                    metadata["filter"],
                ),
                "observations": len(filtered_df),
                "unique_firms": (
                    int(filtered_df["nip"].nunique())
                    if not filtered_df.empty and "nip" in filtered_df.columns
                    else 0
                ),
                "included_in_ols": result.get("models_estimated", 0) > 0,
                "included_in_trajectory_diagnostics": not filtered_df.empty,
                "shared_definition_source": (
                    "code_config.get_scenario_definitions"
                ),
            }
        )
    return pd.DataFrame(rows)


def build_diagnostic_config(input_file: str | Path) -> dict[str, Any]:
    return {
        "analysis_name": "variable_diagnostics_and_trajectories",
        "input_file": str(input_file),
        "output_file": CONFIG["output_file"],
        "sample_name": "ScenarioSamples",
        "base_sample_filter": "True",
        **get_period_model_settings(),
    }


def build_scenario_analysis_inputs(
    input_file: str | Path,
) -> tuple[
    pd.DataFrame,
    dict[str, Any],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    config = build_diagnostic_config(input_file)
    validate_user_config(config)
    config = normalise_config(config)
    validate_config(config)
    models = build_models(config)
    pre_filter_registry = build_variable_registry(config, models)
    validate_registry_against_models(pre_filter_registry, models)

    input_path = Path(config["input_file"])
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    input_df, config["manual_exclusion_audit"] = apply_manual_exclusions(pd.read_parquet(input_path))
    validate_input_columns(input_df, config, models)

    scenario_definitions = get_scenario_definitions()
    if list(scenario_definitions) != SCENARIO_ORDER:
        raise ValueError(
            "Shared scenario ordering changed unexpectedly: "
            f"{list(scenario_definitions)}"
        )
    scenario_results = {
        scenario_name: run_models_for_scenario(
            input_df=input_df,
            scenario_name=scenario_name,
            filter_query=metadata["filter"],
            config=config,
            models=models,
            pre_filter_registry=pre_filter_registry,
        )
        for scenario_name, metadata in scenario_definitions.items()
    }
    failed_scenarios = {
        scenario_name: result["warnings"]
        for scenario_name, result in scenario_results.items()
        if result["filtered_df"].empty
    }
    if failed_scenarios:
        raise ValueError(
            "Cannot build diagnostics because scenario preparation failed: "
            f"{failed_scenarios}"
        )
    return input_df, config, models, scenario_results


def build_diagnostic_tables(
    input_file: str | Path,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    input_df, config, models, scenario_results = build_scenario_analysis_inputs(
        input_file
    )
    coefficient_frames = [
        result["coefficients_long_df"]
        for result in scenario_results.values()
        if not result["coefficients_long_df"].empty
    ]
    coefficients_long_df = (
        pd.concat(coefficient_frames, ignore_index=True)
        if coefficient_frames
        else pd.DataFrame()
    )
    missingness_df = build_missingness_table(scenario_results, config, models)
    winsorisation_impact_df = build_winsorisation_impact(
        scenario_results,
        config,
        models,
    )
    correlation_long_df = build_correlation_long(
        scenario_results,
        config,
        models,
    )
    stability_df = pd.concat(
        [
            build_correlation_stability(correlation_long_df, variant)
            for variant in CORRELATION_VARIANTS
        ],
        ignore_index=True,
        sort=False,
    )
    tables = {
        "00_README_STRUCTURE": build_structure_readme(config),
        "01_VARIABLES": build_variable_labels_table_for_scenarios(
            scenario_results,
            coefficients_long_df,
        ),
        "03_MISSINGNESS": missingness_df,
        "04_WINSOR_IMPACT": winsorisation_impact_df,
        "20_CORR_MAIN_BASELINE": build_shared_full_correlation_matrices(
            correlation_long_df,
            "baseline",
        ),
        "21_CORR_MAIN_WINSOR": build_shared_full_correlation_matrices(
            correlation_long_df,
            "winsor",
        ),
        "22_CORR_WITH_DV_BASELINE": build_correlation_with_dv(
            correlation_long_df,
            models,
            "baseline",
        ),
        "23_CORR_WITH_DV_WINSOR": build_correlation_with_dv(
            correlation_long_df,
            models,
            "winsor",
        ),
        "24_CORR_PREDICTOR_RISK": build_predictor_risk_summary(
            correlation_long_df,
            models,
        ),
        "25_CORR_STABILITY_SCENARIOS": stability_df,
        "30_SCENARIO_SUMMARY": build_scenario_summary(scenario_results),
        "31_SCENARIO_DIAGNOSTICS": build_scenario_diagnostics(
            scenario_results,
            config,
            models,
        ),
        "90_CORR_LONG_ALL": correlation_long_df,
        "91_APPENDIX_FULL_MATRICES": build_appendix_full_matrices(
            correlation_long_df
        ),
    }
    missing_tables = sorted(DIAGNOSTIC_SHEET_NAMES.difference(tables))
    empty_tables = sorted(
        sheet_name
        for sheet_name, table in tables.items()
        if sheet_name in DIAGNOSTIC_SHEET_NAMES and table.empty
    )
    if missing_tables or empty_tables:
        raise ValueError(
            "Diagnostic table build is incomplete: "
            f"missing={missing_tables}, empty={empty_tables}"
        )
    validate_diagnostic_content(tables)
    metadata = {
        "input_row_count": len(input_df),
        "scenario_count": len(scenario_results),
        "models_estimated": sum(
            result["models_estimated"] for result in scenario_results.values()
        ),
        "models_skipped": sum(
            result["models_skipped"] for result in scenario_results.values()
        ),
        "missingness_row_count": len(missingness_df),
        "winsorisation_impact_row_count": len(winsorisation_impact_df),
        "correlation_row_count": len(correlation_long_df),
        "config": config,
    }
    return tables, metadata


def validate_diagnostic_tables(
    diagnostic_tables: dict[str, pd.DataFrame] | None,
) -> dict[str, pd.DataFrame]:
    if diagnostic_tables is None:
        raise ValueError("diagnostic_tables must be built before validation.")

    missing_sheets = sorted(DIAGNOSTIC_SHEET_NAMES.difference(diagnostic_tables))
    empty_sheets = sorted(
        sheet_name
        for sheet_name in DIAGNOSTIC_SHEET_NAMES.intersection(diagnostic_tables)
        if diagnostic_tables[sheet_name] is None
        or diagnostic_tables[sheet_name].empty
    )
    if missing_sheets or empty_sheets:
        raise ValueError(
            "Required diagnostic tables are incomplete: "
            f"missing={missing_sheets}, empty={empty_sheets}."
        )

    validate_diagnostic_content(diagnostic_tables)
    return diagnostic_tables


TAB_COLOURS = {
    "navigation": "#1F4E78",
    "diagnostics": "#70AD47",
    "trajectory": "#4472C4",
    "correlation": "#ED7D31",
    "scenario": "#8064A2",
    "appendix": "#7F7F7F",
}

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
        "sector",
        "sector_en",
        *columns["growth_columns"],
        *columns["sign_columns"],
        *columns["index_columns"],
    }
    optional_missing = sorted(set(["company"]).difference(df.columns))
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
    for scenario_name in get_scenario_definitions():
        samples[scenario_name] = df.loc[
            build_scenario_mask(df, scenario_name, columns["complete_flag"])
        ].copy()
    return samples


def period_suffixes() -> list[str]:
    return list(PERIODS)


def sample_definition_text(sample_name: str, complete_flag: str) -> str:
    metadata = get_scenario_definitions()[sample_name]
    if metadata["sample"] == "All":
        return f"{complete_flag} == 1"
    if metadata["sample"] == "Rank2019":
        return f"{complete_flag} == 1 and in_rank_2019 == 1"
    if metadata["sample"] == "Rank2019_Manufacturing":
        return f"{complete_flag} == 1 and in_rank_2019 == 1 and manufacturing == 1"
    return f"{complete_flag} == 1 and {metadata['filter']}"


def build_config_sheet(config: dict[str, Any], columns: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {"section": "config", "item": "selected trajectory family", "value": config["trajectory_family"]},
        {"section": "config", "item": "selected growth basis", "value": columns["trajectory_family"]},
        {"section": "config", "item": "input file", "value": config["input_file"]},
        {"section": "config", "item": "output file", "value": config["output_file"]},
        {
            "section": "method notes",
            "item": "2018 role",
            "value": "2018 is used only for P1 lag growth and is not part of trajectory construction",
        },
        {
            "section": "method notes",
            "item": "main analysis window",
            "value": "P1/P2/P3 trajectories, SGrowth_NR, and full-period outcomes remain based on 2019-2024",
        },
        {
            "section": "method notes",
            "item": "sector profile",
            "value": "sector_en is required and used for sector trajectory profiles",
        },
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
        for sample_name in SCENARIO_ORDER
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
        return pd.DataFrame(columns=["section", "metric", *SCENARIO_ORDER])
    metric_order = [column for column in sample_overview.columns if column != "sample"]
    output = sample_overview.set_index("sample").T.reset_index().rename(columns={"index": "metric"})
    output = output.loc[output["metric"].isin(metric_order), ["metric", *SCENARIO_ORDER]]
    output.insert(0, "section", output["metric"].map(sample_overview_section))
    return output.loc[:, ["section", "metric", *SCENARIO_ORDER]]


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
        **{
            f"header_{role}": workbook.add_format(
                {
                    "bold": True,
                    "bottom": 1,
                    "bg_color": colour,
                    "font_color": "#FFFFFF",
                    "valign": "vcenter",
                    "text_wrap": True,
                }
            )
            for role, colour in TAB_COLOURS.items()
        },
        "percent": workbook.add_format({"num_format": "0.0%"}),
        "integer": workbook.add_format({"num_format": "#,##0"}),
        "decimal": workbook.add_format({"num_format": "0.0000"}),
        "p_value": workbook.add_format(
            {"num_format": '[<0.0001]"<0.0001";0.0000'}
        ),
        "growth_percent": workbook.add_format({"num_format": "0.0%"}),
        "index": workbook.add_format({"num_format": "0.0"}),
        "wrap": workbook.add_format({"text_wrap": True, "valign": "top"}),
        "correlation_section": workbook.add_format(
            {
                "bold": True,
                "bg_color": "#FCE4D6",
                "font_color": "#9C5700",
                "top": 1,
                "bottom": 1,
            }
        ),
    }


def sheet_role(sheet_name: str) -> str:
    prefix = sheet_name[:2]
    if prefix == "00":
        return "navigation"
    if prefix in {"01", "02", "03", "04"}:
        return "diagnostics"
    if prefix.startswith("1"):
        return "trajectory"
    if prefix.startswith("2"):
        return "correlation"
    if prefix.startswith("3"):
        return "scenario"
    return "appendix"


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
        "n_before",
        "n_after",
        "n_changed",
        "n_total",
        "n_missing",
        "estimation_sample_n",
        "descriptive_n",
        "unique_non_missing",
    }
    sample_metric_column = "metric" if "metric" in df.columns else None
    profile_variable_column = "variable" if "variable" in df.columns else None
    for row_idx in range(len(df)):
        excel_row = row_idx + 1
        for col_idx, column_name in enumerate(df.columns):
            value = df.iloc[row_idx, col_idx]
            if pd.isna(value) or not pd.api.types.is_number(value):
                continue
            if sheet_name == "02_SAMPLE_SUMMARY":
                metric = str(df.loc[df.index[row_idx], sample_metric_column]) if sample_metric_column else ""
                cell_format = sample_overview_row_format(metric, formats)
                if cell_format is None:
                    continue
            else:
                lower_name = str(column_name).lower()
                if lower_name == "p_value":
                    cell_format = formats["p_value"]
                elif "share" in lower_name:
                    cell_format = formats["percent"]
                elif lower_name in count_columns or lower_name.endswith("_count"):
                    cell_format = formats["integer"]
                elif sheet_name == "13_TRAJECTORY_PROFILE" and lower_name in {
                    "median_value",
                    "p10_value",
                    "p90_value",
                }:
                    variable = str(df.loc[df.index[row_idx], profile_variable_column]) if profile_variable_column else ""
                    cell_format = formats["percent"] if variable in PROFILE_RATIO_VARIABLES else formats["decimal"]
                elif sheet_name == "14_TRAJECTORY_PROFILE_PIVOT" and str(column_name) in PROFILE_VARIABLES:
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
    role = sheet_role(sheet_name)
    apply_header_format(worksheet, df, formats[f"header_{role}"])
    worksheet.set_tab_color(TAB_COLOURS[role])
    set_table_column_widths(worksheet, df)
    if sheet_name == "00_README_STRUCTURE":
        worksheet.set_column(0, 0, 18)
        worksheet.set_column(1, 1, 16)
        worksheet.set_column(2, 2, 18)
        worksheet.set_column(3, 3, 30)
        worksheet.set_column(4, 4, 55, formats["wrap"])
        worksheet.set_column(5, 5, 85, formats["wrap"])
        worksheet.set_default_row(30)
        for row_idx, record in enumerate(df.to_dict('records'), start=1):
            if record['block'] == 'manual exclusions':
                lines = max(len(textwrap.wrap(str(record['reader_use']), width=50)),
                            len(textwrap.wrap(str(record['description']), width=80)), 1)
                worksheet.set_row(row_idx, max(30, lines * 15 + 6))
    if sheet_name in {"20_CORR_MAIN_BASELINE", "21_CORR_MAIN_WINSOR"}:
        for row_idx, row_type in enumerate(df["row_type"], start=1):
            if row_type == "section":
                worksheet.set_row(row_idx, 22)
        worksheet.conditional_format(
            1,
            0,
            len(df),
            len(df.columns) - 1,
            {
                "type": "formula",
                "criteria": '=$D2="section"',
                "format": formats["correlation_section"],
            },
        )
        worksheet.set_column(3, 3, 12)
        worksheet.set_column(4, 5, 38)
    if sheet_name == "31_SCENARIO_DIAGNOSTICS":
        notes_col = df.columns.get_loc("sample_inclusion_notes")
        scale_col = df.columns.get_loc("descriptive_scale")
        worksheet.set_column(notes_col, notes_col, 65, formats["wrap"])
        worksheet.set_column(scale_col, scale_col, 36, formats["wrap"])
    apply_table_number_formats(worksheet, df, formats, sheet_name)
    freeze_and_hide_gridlines(worksheet, 1, 2 if sheet_name == "02_SAMPLE_SUMMARY" else 0)
    if sheet_name != "00_README_STRUCTURE":
        apply_safe_autofilter(worksheet, df)


def write_workbook(output_path: Path, tables: dict[str, pd.DataFrame]) -> list[str]:
    actual_sheets = list(tables)
    if actual_sheets != OUTPUT_SHEETS:
        raise ValueError(f"Internal sheet order changed. Expected={OUTPUT_SHEETS}, actual={actual_sheets}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        formats = create_formats(writer.book)
        for sheet_name, table in tables.items():
            write_sheet(writer, sheet_name, table, formats)
    validate_written_workbook(output_path)
    return actual_sheets


def validate_written_workbook(output_path: Path) -> None:
    if not output_path.exists():
        raise FileNotFoundError(f"Workbook was not written: {output_path}")

    with pd.ExcelFile(output_path) as workbook:
        if workbook.sheet_names != OUTPUT_SHEETS:
            raise ValueError(
                "Written workbook sheet order does not match the required schema: "
                f"expected={OUTPUT_SHEETS}, actual={workbook.sheet_names}"
            )
        written_tables = {
            sheet_name: pd.read_excel(workbook, sheet_name=sheet_name)
            for sheet_name in OUTPUT_SHEETS
        }
        empty_sheets = [
            sheet_name
            for sheet_name, table in written_tables.items()
            if table.empty
        ]
    if empty_sheets:
        raise ValueError(
            f"Written workbook contains empty required sheets: {empty_sheets}"
        )
    validate_diagnostic_content(
        {
            sheet_name: written_tables[sheet_name]
            for sheet_name in DIAGNOSTIC_SHEET_NAMES
        }
    )


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
        if sheet_name == "02_SAMPLE_SUMMARY" and "metric" in table.columns:
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
    analytical_sheets = ["10_TRAJECTORY_SUMMARY", "11_TRAJECTORY_BY_SCENARIO", "16_PATH_INDEX"]
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
    print(f"median_profile_by_trajectory_written: {'13_TRAJECTORY_PROFILE' in written_sheets}")
    print(f"median_profile_pivot_written: {'14_TRAJECTORY_PROFILE_PIVOT' in written_sheets}")
    profile_sheets_have_median_index = all(
        "median_index_2024" in tables[sheet_name].columns
        for sheet_name in ["13_TRAJECTORY_PROFILE", "14_TRAJECTORY_PROFILE_PIVOT"]
    )
    print(f"median_index_2024_added_to_profile_sheets: {profile_sheets_have_median_index}")
    print("profile_sorting_uses_median_index_2024: True")
    total_growth_rows_added = (
        "11_TRAJECTORY_BY_SCENARIO" in tables
        and "level" in tables["11_TRAJECTORY_BY_SCENARIO"].columns
        and tables["11_TRAJECTORY_BY_SCENARIO"]["level"].astype("string").eq("total").any()
    )
    total_profile_rows_added = (
        "13_TRAJECTORY_PROFILE" in tables
        and "level" in tables["13_TRAJECTORY_PROFILE"].columns
        and tables["13_TRAJECTORY_PROFILE"]["level"].astype("string").eq("total").any()
    )
    total_pivot_rows_added = (
        "14_TRAJECTORY_PROFILE_PIVOT" in tables
        and "level" in tables["14_TRAJECTORY_PROFILE_PIVOT"].columns
        and tables["14_TRAJECTORY_PROFILE_PIVOT"]["level"].astype("string").eq("total").any()
    )
    total_distribution_rows_added = (
        "10_TRAJECTORY_SUMMARY" in tables
        and "level" in tables["10_TRAJECTORY_SUMMARY"].columns
        and tables["10_TRAJECTORY_SUMMARY"]["level"].astype("string").eq("total").any()
    )
    print(f"total_rows_added_to_growth_by_trajectory: {total_growth_rows_added}")
    print(f"total_rows_added_to_median_profile_by_trajectory: {total_profile_rows_added}")
    print(f"total_rows_added_to_median_profile_pivot: {total_pivot_rows_added}")
    print(f"total_rows_added_to_trajectory_distribution: {total_distribution_rows_added}")
    print("sorting_hierarchy_updated_total_group_3step: True")
    pivot_statistics = []
    if "14_TRAJECTORY_PROFILE_PIVOT" in tables and "statistic" in tables["14_TRAJECTORY_PROFILE_PIVOT"].columns:
        pivot_statistics = sorted(tables["14_TRAJECTORY_PROFILE_PIVOT"]["statistic"].dropna().astype(str).unique().tolist())
    print(f"median_profile_pivot_statistics_present: {pivot_statistics}")
    performance_sheet_written = "15_PERFORMANCE_BANDS" in written_sheets
    performance_levels = []
    performance_row_shares_valid = False
    if "15_PERFORMANCE_BANDS" in tables and not tables["15_PERFORMANCE_BANDS"].empty:
        performance_table = tables["15_PERFORMANCE_BANDS"]
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
    print("code_helpers_imported: True")
    print(
        "sector_en present and used as categorical control: "
        f"{'sector_en' in df.columns}"
    )
    winsor_impact = tables["04_WINSOR_IMPACT"]
    print(
        "winsor_impact_single_transformation: "
        f"{set(winsor_impact['model_variant']) == {'winsor'}}"
    )
    print(
        "winsor_impact_positive_n_changed: "
        f"{bool(winsor_impact['n_changed'].gt(0).all())}"
    )
    print(
        "correlation_p_values_written: "
        f"{all('p_value' in tables[sheet].columns for sheet in ['22_CORR_WITH_DV_BASELINE', '23_CORR_WITH_DV_WINSOR', '90_CORR_LONG_ALL'])}"
    )
    print(
        "correlation_matrix_p_values_omitted: "
        f"{all('p_value' not in tables[sheet].columns for sheet in ['20_CORR_MAIN_BASELINE', '21_CORR_MAIN_WINSOR'])}"
    )
    for sheet_name in [
        "20_CORR_MAIN_BASELINE",
        "21_CORR_MAIN_WINSOR",
        "22_CORR_WITH_DV_BASELINE",
        "23_CORR_WITH_DV_WINSOR",
        "31_SCENARIO_DIAGNOSTICS",
    ]:
        scenario_coverage = (
            tables[sheet_name]["scenario"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )
        print(f"{sheet_name}_scenarios: {scenario_coverage}")
    empty_expected_sheets = [
        sheet_name for sheet_name in OUTPUT_SHEETS if tables[sheet_name].empty
    ]
    print(f"empty_expected_sheets: {empty_expected_sheets}")
    print("workbook_regenerated_successfully: True")
    print(f"written_sheet_names: {written_sheets}")
    print(f"output_path: {output_path.resolve()}")


def run_trajectory_analysis(
    config: dict[str, Any] = CONFIG,
    diagnostic_tables: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    config = dict(config)
    columns = get_trajectory_family_config(config["trajectory_family"])
    input_path = Path(config["input_file"])
    output_path = Path(config["output_file"])
    diagnostic_metadata: dict[str, Any] = {}
    if diagnostic_tables is None:
        diagnostic_tables, diagnostic_metadata = build_diagnostic_tables(input_path)
    diagnostic_tables = validate_diagnostic_tables(diagnostic_tables)
    diagnostic_growth_mode = (
        diagnostic_metadata.get("config", {}).get("growth_mode")
        if diagnostic_metadata
        else config["trajectory_family"]
    )
    if diagnostic_growth_mode != config["trajectory_family"]:
        raise ValueError(
            "Trajectory and diagnostic growth families differ: "
            f"trajectory={config['trajectory_family']}, "
            f"diagnostics={diagnostic_growth_mode}"
        )
    scenario_names = list(get_scenario_definitions())
    validate_scenario_alignment(scenario_names, list(get_scenario_definitions()))
    print(f"final_ordered_scenarios: {scenario_names}")

    df, exclusion_audit = apply_manual_exclusions(load_input_data(input_path))
    config["manual_exclusion_audit"] = exclusion_audit
    validate_input(df, columns)
    performance_band_columns = resolve_performance_band_columns(df, columns["trajectory_family"])
    performance_band_columns = {
        "ALL": performance_band_columns["All"],
        "MANUFACTURING": performance_band_columns["All"],
        "RANK2019": performance_band_columns["Rank2019"],
        "RANK2019_MANUFACTURING": performance_band_columns["Rank2019_Manufacturing"],
    }

    samples = build_samples(df, columns)
    profile_variables_included, profile_variables_skipped = available_profile_variables(df)
    if profile_variables_skipped:
        print(f"Profile variables missing and skipped: {profile_variables_skipped}")
    median_profile_long = build_median_profile_by_trajectory(
        samples,
        columns,
        profile_variables_included,
    )
    diagnostics = {
        sheet_name: diagnostic_tables[sheet_name]
        for sheet_name in DIAGNOSTIC_SHEET_NAMES
    }
    tables = {
        "00_README_STRUCTURE": diagnostics["00_README_STRUCTURE"],
        "01_VARIABLES": diagnostics["01_VARIABLES"],
        "02_SAMPLE_SUMMARY": transpose_sample_overview(build_sample_overview(samples, columns)),
        "03_MISSINGNESS": diagnostics["03_MISSINGNESS"],
        "04_WINSOR_IMPACT": diagnostics["04_WINSOR_IMPACT"],
        "10_TRAJECTORY_SUMMARY": build_trajectory_distribution(samples, columns),
        "11_TRAJECTORY_BY_SCENARIO": build_growth_by_trajectory(samples, columns),
        "12_TRAJECTORY_BY_SECTOR": build_sector_owner_profile(samples, columns),
        "13_TRAJECTORY_PROFILE": median_profile_long,
        "14_TRAJECTORY_PROFILE_PIVOT": build_median_profile_pivot(median_profile_long, profile_variables_included),
        "15_PERFORMANCE_BANDS": build_performance_band_trajectory(
            samples,
            columns,
            performance_band_columns,
        ),
        "16_PATH_INDEX": build_path_index(samples, columns),
        "20_CORR_MAIN_BASELINE": diagnostics["20_CORR_MAIN_BASELINE"],
        "21_CORR_MAIN_WINSOR": diagnostics["21_CORR_MAIN_WINSOR"],
        "22_CORR_WITH_DV_BASELINE": diagnostics["22_CORR_WITH_DV_BASELINE"],
        "23_CORR_WITH_DV_WINSOR": diagnostics["23_CORR_WITH_DV_WINSOR"],
        "24_CORR_PREDICTOR_RISK": diagnostics["24_CORR_PREDICTOR_RISK"],
        "25_CORR_STABILITY_SCENARIOS": diagnostics["25_CORR_STABILITY_SCENARIOS"],
        "30_SCENARIO_SUMMARY": diagnostics["30_SCENARIO_SUMMARY"],
        "31_SCENARIO_DIAGNOSTICS": diagnostics["31_SCENARIO_DIAGNOSTICS"],
        "90_CORR_LONG_ALL": diagnostics["90_CORR_LONG_ALL"],
        "91_APPENDIX_FULL_MATRICES": diagnostics["91_APPENDIX_FULL_MATRICES"],
        "92_FIRM_TRAJECTORIES": build_firm_trajectories(df, columns),
        "93_CONFIG_AUDIT": build_config_sheet(config, columns),
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
        "diagnostic_metadata": diagnostic_metadata,
    }


if __name__ == "__main__":
    run_trajectory_analysis()
