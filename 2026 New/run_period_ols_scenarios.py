from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import statsmodels.api as sm


class StandardScaler:
    def fit_transform(self, data):
        frame = pd.DataFrame(data).astype(float)
        means = frame.mean(axis=0)
        stds = frame.std(axis=0, ddof=0).replace(0, 1.0)
        return ((frame - means) / stds).to_numpy()


DEFAULT_REGRESSOR_RULES = {
    "column_pattern": "{base_name}_start_{period}",
    "standardise": True,
}

REGRESSOR_METADATA = {
    "ln_sales": {
        "interpretation": "firm size",
    },
    "profit_margin": {
        "interpretation": "profitability",
    },
    "export_ratio": {
        "interpretation": "internationalisation intensity",
    },
    "asset_turnover": {
        "interpretation": "asset efficiency",
    },
    "capital_ratio": {
        "interpretation": "equity financing strength",
    },
    "sales_per_employee": {
        "interpretation": "labour productivity",
    },
}

CATEGORICAL_METADATA = {
    "sector_en": {
        "reference": "production",
        "display_prefix": "sector: ",
        "interpretation_template": "sector dummy relative to production reference category",
    }
}

OWNER_METADATA = {
    "owner_num": {
        "display_name": "Foreign",
        "interpretation": "foreign ownership dummy; Domestic = 0 reference group",
        "standardise": False,
    }
}

LAG_GROWTH_METADATA = {
    "display_name": "lag_growth_log_ann",
    "interpretation": "prior-period growth persistence",
    "standardise": True,
}

INTERACTION_METADATA = {
    "foreign_x_size_ratio": {
        "variables": ["export_ratio","ln_sales"],
        "display_name": "export_ratio × ln_sales",
        "interpretation": "interaction between export intensity and company size",
        "standardise": True,
        "include": True,
    }
}

# Researcher control panel
#
# To add a standard numeric regressor:
# - add only its name to CONFIG["base_regressors"], for example "roa"
# The default column rule then creates roa_start_P1, roa_start_P2, roa_start_P3 automatically.
#
# Optional numeric labels, interpretations, standardisation rules, or column-pattern
# overrides go in REGRESSOR_METADATA.
#
# Categorical controls are listed by column name in CONFIG["categorical_controls"].
# Their reference categories and labels must be defined in CATEGORICAL_METADATA.
#
# Ownership is controlled by include_owner and owner_column.
# Owner labels and standardisation rules go in OWNER_METADATA.
#
# Lag growth is controlled by include_lag_growth and lag_growth_periods.
# Lag-growth labels and standardisation rules go in LAG_GROWTH_METADATA.
#
# Interactions are optional and live only in INTERACTION_METADATA. Set
# include=True to activate an interaction, include=False to ignore it, or use
# INTERACTION_METADATA = {} for no interactions. Example: Foreign x
# export_ratio uses variables ["owner_num", "export_ratio"] and is generated
# automatically by period. Categorical interactions are not supported yet.
#
# To switch real vs nominal growth:
# - set growth_mode to "real" or "nominal"
# The dependent variables and lag growth controls are generated from this setting.
#
# To change the sample:
# - edit base_sample_filter using pandas query syntax
# The complete-growth requirement is added automatically from growth_mode.
CONFIG = {
    "analysis_name": "period_ols_scenarios",
    "input_file": "Data_period_2019-2024.parquet",
    "output_file": "Results_period_ols_scenarios.xlsx",
    "sample_name": "ScenarioSamples",
    "base_sample_filter": "True",
    "growth_mode": "nominal",  # "real" or "nominal"
    "periods": ["P1", "P2", "P3", "FULL"],
    "base_regressors": [
        "ln_sales",
        "profit_margin",
        "export_ratio",
        "asset_turnover",
        "capital_ratio",
        "sales_per_employee",
    ],
    "include_owner": True,
    "owner_column": "owner_num",
    "include_lag_growth": True,
    "lag_growth_periods": ["P2", "P3"],
    "categorical_controls": [
        "sector_en",
    ],
    "winsorise_dependent": True,
    "winsor_lower": 0.01,
    "winsor_upper": 0.99,
    "standardised_models": True,
    "standardise_dependent": True,
    "covariance_type": "nonrobust",
    "model_variants": [
        {"suffix": "baseline", "model_family": "raw", "winsorised": False, "standardised_model": False},
        {"suffix": "baseline_std", "model_family": "std", "winsorised": False, "standardised_model": True},
        {"suffix": "winsor", "model_family": "raw_winsor", "winsorised": True, "standardised_model": False},
        {"suffix": "winsor_std", "model_family": "std_winsor", "winsorised": True, "standardised_model": True},
    ],
}


SCENARIOS = {
    "ALL": None,
    "RANK2019": "in_rank_2019 == 1",
    "MANUFACTURING": "manufacturing == 1",
    "RANK2019_MANUFACTURING": "in_rank_2019 == 1 & manufacturing == 1",
}

ORIGINAL_RESULTS_FILE = "Results_period_ols.xlsx"
MIN_EXTRA_RESIDUAL_DF = 1

OUTPUT_SHEETS = [
    "README",
    "Compare_Main",
    "Compare_Raw",
    "Descriptive_Stats",
    "Correlation_Long",
    "Model_Summary_Long",
    "AUDIT_AND_TECHNICAL_TABS",
    "Diagnostics_Long",
    "Dropped_Rows_Long",
    "Coefficients_Long",
    "Variable_Labels",
    "Run_Log",
]

COMPARISON_VARIANT_DISPLAY = {
    "baseline": "Baseline",
    "winsor": "Winsor",
    "baseline_std": "StdBaseline",
    "winsor_std": "StdWinsor",
}

COMPARISON_VARIANT_ORDER = ["baseline", "winsor", "baseline_std", "winsor_std"]
COMPARE_MAIN_VARIANTS = ["baseline_std", "winsor_std"]
COMPARE_RAW_VARIANTS = ["baseline", "winsor"]

SUMMARY_ROWS = ["N", "R-squared", "Adjusted R-squared"]

COEFFICIENT_OUTPUT_COLUMNS = [
    "model",
    "period",
    "model_family",
    "growth_mode",
    "dependent_variable",
    "winsorised",
    "standardised_model",
    "sample_filter",
    "raw_variable",
    "display_name",
    "variable_type",
    "coefficient",
    "std error",
    "t-stat",
    "p-value",
    "CI lower",
    "CI upper",
]


def get_growth_prefix(config: dict[str, Any]) -> str:
    if config["growth_mode"] == "real":
        return "r"
    if config["growth_mode"] == "nominal":
        return "n"
    raise ValueError("CONFIG['growth_mode'] must be 'real' or 'nominal'.")


def growth_col(config: dict[str, Any], period: str) -> str:
    if period == "FULL":
        return full_period_growth_col(config)
    return f"{get_growth_prefix(config)}growth_log_ann_{period}"


def full_period_growth_col(config: dict[str, Any]) -> str:
    if config["growth_mode"] == "real":
        return "rgrowth_log_ann_2019_2024"
    if config["growth_mode"] == "nominal":
        return "ngrowth_log_ann_2019_2024"
    raise ValueError("CONFIG['growth_mode'] must be 'real' or 'nominal'.")


def lag_growth_col(config: dict[str, Any], period: str) -> str:
    return f"lag_{get_growth_prefix(config)}growth_log_ann_{period}"


def get_regressor_period_for_model(period: str) -> str:
    return "P1" if period == "FULL" else period


def trajectory_col(config: dict[str, Any]) -> str:
    return f"has_complete_{get_growth_prefix(config)}trajectory"


def get_sample_filter(config: dict[str, Any]) -> str:
    return f"{trajectory_col(config)} == 1 and {config['base_sample_filter']}"


def validate_user_config(config: dict[str, Any]) -> None:
    required_keys = {
        "analysis_name",
        "input_file",
        "output_file",
        "sample_name",
        "base_sample_filter",
        "growth_mode",
        "periods",
        "base_regressors",
        "include_owner",
        "owner_column",
        "include_lag_growth",
        "lag_growth_periods",
        "categorical_controls",
        "winsorise_dependent",
        "winsor_lower",
        "winsor_upper",
        "standardised_models",
        "standardise_dependent",
        "covariance_type",
        "model_variants",
    }
    missing = sorted(required_keys.difference(config))
    if missing:
        raise ValueError(f"CONFIG is missing required keys: {missing}. Add them to the researcher control panel.")

    old_keys = {
        "models",
        "owner_variable",
        "lag_growth",
        "variable_display_map",
        "variable_label_interpretations",
        "variable_label_base_keys",
        "comparison_row_order",
        "interaction_terms",
    }
    present_old_keys = sorted(old_keys.intersection(config))
    if present_old_keys:
        raise ValueError(
            "CONFIG contains old technical keys that are now generated automatically: "
            f"{present_old_keys}. Remove them. Use base_regressors, categorical_controls, owner_column, "
            "and the metadata dictionaries instead."
        )

    if not isinstance(config["base_regressors"], list) or not config["base_regressors"]:
        raise ValueError("CONFIG['base_regressors'] must be a non-empty list of variable-name strings.")
    non_string_regressors = [item for item in config["base_regressors"] if not isinstance(item, str)]
    if non_string_regressors:
        raise ValueError(
            "CONFIG['base_regressors'] must contain only strings. "
            f"Invalid entries: {non_string_regressors}. Put labels or special rules in REGRESSOR_METADATA."
        )

    if not isinstance(config["categorical_controls"], list):
        raise ValueError("CONFIG['categorical_controls'] must be a list of categorical column-name strings.")
    non_string_categoricals = [item for item in config["categorical_controls"] if not isinstance(item, str)]
    if non_string_categoricals:
        raise ValueError(
            "CONFIG['categorical_controls'] must contain only strings. "
            f"Invalid entries: {non_string_categoricals}. Put reference categories and labels in CATEGORICAL_METADATA."
        )

    missing_categorical_metadata = [column for column in config["categorical_controls"] if column not in CATEGORICAL_METADATA]
    if missing_categorical_metadata:
        raise ValueError(
            "Every categorical control needs reference-category metadata. "
            f"Missing CATEGORICAL_METADATA entries for: {missing_categorical_metadata}."
        )

    if config["include_owner"]:
        owner_column = config["owner_column"]
        if not isinstance(owner_column, str) or not owner_column:
            raise ValueError("CONFIG['owner_column'] must be a non-empty string when include_owner is True.")
        if owner_column not in OWNER_METADATA:
            raise ValueError(
                f"CONFIG['owner_column'] is {owner_column!r}, but OWNER_METADATA has no entry for it. "
                "Add OWNER_METADATA for this owner column or change CONFIG['owner_column']."
            )

    validate_interaction_metadata(config)


def validate_interaction_metadata(config: dict[str, Any]) -> None:
    base_regressors = set(config["base_regressors"])
    categorical_controls = set(config["categorical_controls"])
    owner_column = config["owner_column"]

    active_required_keys = {"variables", "display_name", "interpretation", "standardise", "include"}
    allowed_keys = set(active_required_keys)
    for name, interaction in INTERACTION_METADATA.items():
        if not isinstance(interaction, dict):
            raise ValueError(f"INTERACTION_METADATA[{name!r}] must be a dictionary.")
        unknown_keys = sorted(set(interaction).difference(allowed_keys))
        if unknown_keys:
            raise ValueError(
                f"INTERACTION_METADATA[{name!r}] contains unsupported keys {unknown_keys}. "
                f"Allowed keys are {sorted(allowed_keys)}."
            )
        if "include" not in interaction:
            raise ValueError(f"INTERACTION_METADATA[{name!r}] must explicitly set include to True or False.")
        if not isinstance(interaction["include"], bool):
            raise ValueError(f"INTERACTION_METADATA[{name!r}]['include'] must be True or False.")
        if not interaction["include"]:
            continue

        missing_keys = sorted(active_required_keys.difference(interaction))
        if missing_keys:
            raise ValueError(f"Active INTERACTION_METADATA[{name!r}] is missing required keys: {missing_keys}.")

        variables = interaction["variables"]
        if not isinstance(variables, list) or len(variables) != 2 or not all(isinstance(variable, str) for variable in variables):
            raise ValueError(f"INTERACTION_METADATA[{name!r}]['variables'] must contain exactly two variable-name strings.")

        for variable in variables:
            if variable in categorical_controls:
                raise ValueError("Categorical interactions are not supported yet. Please use numeric or dummy variables only.")
            if variable in base_regressors or variable == owner_column:
                continue
            raise ValueError(
                f"Interaction {name!r} uses unknown variable {variable!r}. "
                "Use a CONFIG['base_regressors'] name or CONFIG['owner_column']. "
                "Categorical and lag-growth interactions are not supported yet."
            )


def get_active_interactions() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "variables": metadata["variables"],
            "display_name": metadata["display_name"],
            "interpretation": metadata["interpretation"],
            "standardise": metadata["standardise"],
        }
        for name, metadata in INTERACTION_METADATA.items()
        if metadata.get("include") is True
    ]


def normalise_base_regressors(config: dict[str, Any]) -> list[dict[str, Any]]:
    regressors = []
    for base_name in config["base_regressors"]:
        metadata = REGRESSOR_METADATA.get(base_name, {})
        allowed_keys = {"display_name", "interpretation", "standardise", "column_pattern"}
        unknown_keys = sorted(set(metadata).difference(allowed_keys))
        if unknown_keys:
            raise ValueError(
                f"REGRESSOR_METADATA[{base_name!r}] contains unsupported keys {unknown_keys}. "
                f"Allowed keys are {sorted(allowed_keys)}."
            )

        regressors.append(
            {
                "base_name": base_name,
                "column_pattern": metadata.get("column_pattern", DEFAULT_REGRESSOR_RULES["column_pattern"]),
                "display_name": metadata.get("display_name", base_name),
                "interpretation": metadata.get("interpretation", "no interpretation provided"),
                "standardise": metadata.get("standardise", DEFAULT_REGRESSOR_RULES["standardise"]),
            }
        )
    return regressors


def normalise_categorical_controls(config: dict[str, Any]) -> list[dict[str, Any]]:
    controls = []
    required_keys = {"reference", "display_prefix", "interpretation_template"}
    allowed_keys = set(required_keys)
    for column in config["categorical_controls"]:
        if column not in CATEGORICAL_METADATA:
            raise ValueError(
                f"Categorical control {column!r} is missing from CATEGORICAL_METADATA. "
                "Add reference, display_prefix, and interpretation_template metadata for this categorical variable."
            )
        metadata = CATEGORICAL_METADATA[column]
        missing_keys = sorted(required_keys.difference(metadata))
        if missing_keys:
            raise ValueError(f"CATEGORICAL_METADATA[{column!r}] is missing required keys: {missing_keys}.")
        unknown_keys = sorted(set(metadata).difference(allowed_keys))
        if unknown_keys:
            raise ValueError(
                f"CATEGORICAL_METADATA[{column!r}] contains unsupported keys {unknown_keys}. "
                f"Allowed keys are {sorted(allowed_keys)}."
            )
        controls.append(
            {
                "column": column,
                "reference": metadata["reference"],
                "display_prefix": metadata["display_prefix"],
                "interpretation_template": metadata["interpretation_template"],
            }
        )
    return controls


def normalise_owner_variable(config: dict[str, Any]) -> dict[str, Any]:
    owner_column = config["owner_column"]
    if owner_column not in OWNER_METADATA:
        raise ValueError(
            f"OWNER_METADATA is missing an entry for CONFIG['owner_column'] = {owner_column!r}. "
            "Add display_name, interpretation, and standardise to OWNER_METADATA."
        )

    metadata = OWNER_METADATA[owner_column]
    required_keys = {"display_name", "interpretation", "standardise"}
    missing_keys = sorted(required_keys.difference(metadata))
    if missing_keys:
        raise ValueError(f"OWNER_METADATA[{owner_column!r}] is missing required keys: {missing_keys}.")
    unknown_keys = sorted(set(metadata).difference(required_keys))
    if unknown_keys:
        raise ValueError(
            f"OWNER_METADATA[{owner_column!r}] contains unsupported keys {unknown_keys}. "
            f"Allowed keys are {sorted(required_keys)}."
        )
    return {
        "column": owner_column,
        "display_name": metadata["display_name"],
        "interpretation": metadata["interpretation"],
        "standardise": metadata["standardise"],
    }


def normalise_lag_growth(config: dict[str, Any]) -> dict[str, Any]:
    required_keys = {"display_name", "interpretation", "standardise"}
    missing_keys = sorted(required_keys.difference(LAG_GROWTH_METADATA))
    if missing_keys:
        raise ValueError(f"LAG_GROWTH_METADATA is missing required keys: {missing_keys}.")
    unknown_keys = sorted(set(LAG_GROWTH_METADATA).difference(required_keys))
    if unknown_keys:
        raise ValueError(
            f"LAG_GROWTH_METADATA contains unsupported keys {unknown_keys}. "
            f"Allowed keys are {sorted(required_keys)}."
        )
    return {
        "display_name": LAG_GROWTH_METADATA["display_name"],
        "interpretation": LAG_GROWTH_METADATA["interpretation"],
        "standardise": LAG_GROWTH_METADATA["standardise"],
    }


def normalise_config(config: dict[str, Any]) -> dict[str, Any]:
    normalised = dict(config)
    normalised["base_regressors"] = normalise_base_regressors(config)
    normalised["categorical_controls"] = normalise_categorical_controls(config)
    normalised["owner_variable"] = normalise_owner_variable(config) if config["include_owner"] else {
        "column": config["owner_column"],
        "display_name": config["owner_column"],
        "interpretation": "owner variable excluded from current configuration",
        "standardise": False,
    }
    normalised["lag_growth"] = normalise_lag_growth(config)
    return normalised


def validate_config(config: dict[str, Any]) -> None:
    required_keys = {
        "analysis_name",
        "input_file",
        "output_file",
        "sample_name",
        "base_sample_filter",
        "growth_mode",
        "periods",
        "base_regressors",
        "include_owner",
        "owner_variable",
        "include_lag_growth",
        "lag_growth_periods",
        "lag_growth",
        "categorical_controls",
        "winsorise_dependent",
        "winsor_lower",
        "winsor_upper",
        "standardised_models",
        "standardise_dependent",
        "covariance_type",
        "model_variants",
    }
    missing = sorted(required_keys.difference(config))
    if missing:
        raise ValueError(f"CONFIG is missing required keys: {missing}. Add them to the researcher control panel.")

    removed_user_keys = {
        "models",
        "variable_display_map",
        "variable_label_interpretations",
        "variable_label_base_keys",
        "comparison_row_order",
    }
    present_removed_keys = sorted(removed_user_keys.intersection(config))
    if present_removed_keys:
        raise ValueError(
            "CONFIG contains old technical keys that are now generated automatically: "
            f"{present_removed_keys}. Remove these keys and edit base_regressors instead."
        )

    if config["growth_mode"] not in {"real", "nominal"}:
        raise ValueError("CONFIG['growth_mode'] must be either 'real' or 'nominal'.")

    if not isinstance(config["periods"], list) or not config["periods"]:
        raise ValueError("CONFIG['periods'] must be a non-empty list such as ['P1', 'P2', 'P3'].")
    if len(set(config["periods"])) != len(config["periods"]):
        raise ValueError("CONFIG['periods'] contains duplicates. Each period should appear once.")

    if not isinstance(config["base_sample_filter"], str) or not config["base_sample_filter"].strip():
        raise ValueError("CONFIG['base_sample_filter'] must be a non-empty pandas query string.")

    if not isinstance(config["base_regressors"], list) or not config["base_regressors"]:
        raise ValueError("CONFIG['base_regressors'] must be a non-empty list of variable definitions.")

    required_regressor_keys = {"base_name", "column_pattern", "display_name", "interpretation", "standardise"}
    seen_base_names = set()
    seen_display_names = {}
    for i, regressor in enumerate(config["base_regressors"], start=1):
        missing_regressor_keys = sorted(required_regressor_keys.difference(regressor))
        if missing_regressor_keys:
            raise ValueError(f"base_regressors item {i} is missing keys: {missing_regressor_keys}.")
        if regressor["base_name"] in seen_base_names:
            raise ValueError(f"Duplicate base_regressors base_name {regressor['base_name']!r}.")
        seen_base_names.add(regressor["base_name"])
        display_name = regressor["display_name"]
        if display_name in seen_display_names:
            raise ValueError(
                f"Duplicate base_regressors display_name {display_name!r}. "
                f"It is used by both {seen_display_names[display_name]!r} and {regressor['base_name']!r}. "
                "Change one display_name so comparison tables have unique row labels."
            )
        seen_display_names[display_name] = regressor["base_name"]
        try:
            regressor["column_pattern"].format(base_name=regressor["base_name"], period=config["periods"][0])
        except KeyError as exc:
            raise ValueError(
                "Each base_regressors column_pattern may only use {base_name} and {period}. "
                f"Problem in {regressor['base_name']!r}: missing placeholder {exc}."
            ) from exc

    owner_keys = {"column", "display_name", "interpretation", "standardise"}
    owner_missing = sorted(owner_keys.difference(config["owner_variable"]))
    if owner_missing:
        raise ValueError(f"CONFIG['owner_variable'] is missing keys: {owner_missing}.")
    if config["include_owner"] and config["owner_variable"]["display_name"] in seen_display_names:
        raise ValueError(
            f"owner_variable display_name {config['owner_variable']['display_name']!r} duplicates a base_regressors label. "
            "Change owner_variable['display_name'] or the base regressor display_name."
        )

    lag_growth_keys = {"display_name", "interpretation", "standardise"}
    lag_missing = sorted(lag_growth_keys.difference(config["lag_growth"]))
    if lag_missing:
        raise ValueError(f"CONFIG['lag_growth'] is missing keys: {lag_missing}.")
    if config["include_lag_growth"] and config["lag_growth"]["display_name"] in seen_display_names:
        raise ValueError(
            f"lag_growth display_name {config['lag_growth']['display_name']!r} duplicates a base_regressors label. "
            "Change lag_growth['display_name'] or the base regressor display_name."
        )

    invalid_lag_periods = sorted(set(config["lag_growth_periods"]).difference(config["periods"]))
    if invalid_lag_periods:
        raise ValueError(f"CONFIG['lag_growth_periods'] includes periods not listed in CONFIG['periods']: {invalid_lag_periods}.")
    if "FULL" in config["lag_growth_periods"]:
        raise ValueError(
            "FULL cannot be included in CONFIG['lag_growth_periods'] because the full-period model uses P1 starting covariates and has no prior-period lag."
        )

    required_categorical_keys = {"column", "reference", "display_prefix", "interpretation_template"}
    seen_categorical_columns = set()
    for i, control in enumerate(config["categorical_controls"], start=1):
        missing_control_keys = sorted(required_categorical_keys.difference(control))
        if missing_control_keys:
            raise ValueError(f"categorical_controls item {i} is missing keys: {missing_control_keys}.")
        if control["column"] in seen_categorical_columns:
            raise ValueError(f"Duplicate categorical control column {control['column']!r}.")
        seen_categorical_columns.add(control["column"])

    if not (0 <= config["winsor_lower"] < config["winsor_upper"] <= 1):
        raise ValueError("CONFIG winsor settings must satisfy 0 <= winsor_lower < winsor_upper <= 1.")

    if config["covariance_type"] not in {"nonrobust", "HC1", "HC3"}:
        raise ValueError("CONFIG['covariance_type'] must be one of: 'nonrobust', 'HC1', 'HC3'.")

    validate_model_variants(config)


def validate_model_variants(config: dict[str, Any]) -> None:
    if not isinstance(config["model_variants"], list) or not config["model_variants"]:
        raise ValueError("CONFIG['model_variants'] must be a non-empty list.")

    required_variant_keys = {"suffix", "model_family", "winsorised", "standardised_model"}
    suffixes = []
    for variant in config["model_variants"]:
        missing = sorted(required_variant_keys.difference(variant))
        if missing:
            raise ValueError(f"Each model variant must contain {sorted(required_variant_keys)}. Missing: {missing}.")
        suffixes.append(variant["suffix"])
        if not isinstance(variant["winsorised"], bool) or not isinstance(variant["standardised_model"], bool):
            raise ValueError(f"Model variant {variant['suffix']!r} must use boolean winsorised and standardised_model flags.")

    duplicates = sorted({suffix for suffix in suffixes if suffixes.count(suffix) > 1})
    if duplicates:
        raise ValueError(f"CONFIG['model_variants'] contains duplicate suffixes: {duplicates}.")

    configured_suffixes = set(suffixes)
    required_suffixes = set(COMPARISON_VARIANT_ORDER)
    missing_suffixes = sorted(required_suffixes.difference(configured_suffixes))
    if missing_suffixes:
        raise ValueError(
            "CONFIG['model_variants'] is missing variants required by the fixed output workbook: "
            f"{missing_suffixes}. Add these suffixes or update COMPARISON_VARIANT_ORDER in the script."
        )


def get_model_variants(config: dict[str, Any]) -> list[dict[str, Any]]:
    variants = []
    for variant in config["model_variants"]:
        if variant["standardised_model"] and not config["standardised_models"]:
            continue
        if variant["winsorised"] and not config["winsorise_dependent"]:
            continue
        variants.append(dict(variant))
    return variants


def render_regressor_column(regressor: dict[str, Any], period: str) -> str:
    return regressor["column_pattern"].format(base_name=regressor["base_name"], period=period)


def get_base_regressor_by_name(config: dict[str, Any], base_name: str) -> dict[str, Any] | None:
    for regressor in config["base_regressors"]:
        if regressor["base_name"] == base_name:
            return regressor
    return None


def resolve_interaction_variable(config: dict[str, Any], variable_name: str, period: str) -> str:
    regressor = get_base_regressor_by_name(config, variable_name)
    if regressor is not None:
        return render_regressor_column(regressor, period)
    if variable_name == config["owner_variable"]["column"]:
        return config["owner_variable"]["column"]
    if variable_name in get_categorical_columns(config):
        raise ValueError("Categorical interactions are not supported yet. Please use numeric or dummy variables only.")
    raise ValueError(
        f"Interaction variable {variable_name!r} is not supported. "
        "Use a base regressor name or the owner column."
    )


def build_interaction_column_name(interaction_name: str, period: str) -> str:
    return f"{interaction_name}_{period}"


def get_interaction_column_names(config: dict[str, Any]) -> set[str]:
    return {
        build_interaction_column_name(interaction["name"], period)
        for interaction in get_active_interactions()
        for period in config["periods"]
    }


def get_interaction_source_columns(config: dict[str, Any]) -> set[str]:
    source_columns = set()
    for interaction in get_active_interactions():
        for period in config["periods"]:
            regressor_period = get_regressor_period_for_model(period)
            for variable_name in interaction["variables"]:
                source_columns.add(resolve_interaction_variable(config, variable_name, regressor_period))
    return source_columns


def add_interaction_columns(
    df: pd.DataFrame,
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    output = df.copy()
    active_interactions = get_active_interactions()
    if not active_interactions:
        return output

    for period in models:
        regressor_period = get_regressor_period_for_model(period)
        for interaction in active_interactions:
            source_columns = [
                resolve_interaction_variable(config, variable_name, regressor_period)
                for variable_name in interaction["variables"]
            ]
            interaction_column = build_interaction_column_name(interaction["name"], period)
            left = pd.to_numeric(output[source_columns[0]], errors="coerce")
            right = pd.to_numeric(output[source_columns[1]], errors="coerce")
            output[interaction_column] = left * right
    return output


def build_models(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    models: dict[str, dict[str, Any]] = {}
    lag_periods = set(config["lag_growth_periods"]) if config["include_lag_growth"] else set()
    for period in config["periods"]:
        regressor_period = get_regressor_period_for_model(period)
        regressors = [render_regressor_column(regressor, regressor_period) for regressor in config["base_regressors"]]
        if config["include_owner"]:
            regressors.append(config["owner_variable"]["column"])
        regressors.extend(
            build_interaction_column_name(interaction["name"], period)
            for interaction in get_active_interactions()
        )
        if period in lag_periods:
            regressors.append(lag_growth_col(config, period))

        models[period] = {
            "dependent": growth_col(config, period),
            "regressors": regressors,
        }
    return models


def get_categorical_columns(config: dict[str, Any]) -> list[str]:
    return [control["column"] for control in config["categorical_controls"]]


def get_categorical_control(config: dict[str, Any], column: str) -> dict[str, Any]:
    for control in config["categorical_controls"]:
        if control["column"] == column:
            return control
    raise KeyError(f"Unknown categorical control: {column}")


def load_input_data(config: dict[str, Any]) -> pd.DataFrame:
    input_path = Path(config["input_file"])
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}. Change CONFIG['input_file'].")
    return pd.read_parquet(input_path)


def get_required_columns(config: dict[str, Any], models: dict[str, dict[str, Any]]) -> list[str]:
    required = {trajectory_col(config), *get_categorical_columns(config)}
    interaction_columns = get_interaction_column_names(config)
    for model_spec in models.values():
        required.add(model_spec["dependent"])
        required.update(regressor for regressor in model_spec["regressors"] if regressor not in interaction_columns)
    required.update(get_interaction_source_columns(config))
    return sorted(required)


def validate_input_columns(df: pd.DataFrame, config: dict[str, Any], models: dict[str, dict[str, Any]]) -> None:
    missing = sorted(set(get_required_columns(config, models)).difference(df.columns))
    if missing:
        raise ValueError(
            "Input file is missing columns generated from CONFIG: "
            f"{missing}. Check growth_mode, periods, base_regressors column_pattern, owner_variable, "
            "lag_growth_periods, and interaction_terms source variables."
        )


def apply_sample_filter(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    sample_filter = get_sample_filter(config)
    try:
        filtered = df.query(sample_filter, engine="python").copy()
    except Exception as exc:
        raise ValueError(f"CONFIG sample filter failed: {sample_filter!r}. Edit CONFIG['base_sample_filter'].") from exc
    if filtered.empty:
        raise ValueError(f"Sample filter returned zero rows: {sample_filter!r}. Edit CONFIG['base_sample_filter'].")
    return filtered


def get_categorical_levels(df: pd.DataFrame, config: dict[str, Any]) -> dict[str, list[str]]:
    levels: dict[str, list[str]] = {}
    for control in config["categorical_controls"]:
        column = control["column"]
        observed = sorted(df[column].dropna().astype(str).unique().tolist())
        reference = str(control["reference"])
        if reference not in observed:
            raise ValueError(
                f"Reference category {reference!r} not found for categorical control {column!r} after sample filtering. "
                "Change the control reference or base_sample_filter."
            )
        levels[column] = [reference] + [level for level in observed if level != reference]
    return levels


def make_registry_entry(
    display_name: str,
    interpretation: str,
    standardise: bool,
    variable_type: str,
    source: str,
    order: int,
    period: str | None = None,
) -> dict[str, Any]:
    return {
        "display_name": display_name,
        "interpretation": interpretation,
        "standardise": bool(standardise),
        "variable_type": variable_type,
        "source": source,
        "order": order,
        "period": period,
    }


def build_variable_registry(
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
    categorical_levels: dict[str, list[str]] | None = None,
) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}

    for order, regressor in enumerate(config["base_regressors"], start=10):
        for period in config["periods"]:
            regressor_period = get_regressor_period_for_model(period)
            column = render_regressor_column(regressor, regressor_period)
            registry[column] = make_registry_entry(
                display_name=regressor["display_name"],
                interpretation=regressor["interpretation"],
                standardise=regressor["standardise"],
                variable_type="numeric",
                source=regressor["base_name"],
                order=order,
                period=period,
            )

    if config["include_owner"]:
        owner = config["owner_variable"]
        registry[owner["column"]] = make_registry_entry(
            display_name=owner["display_name"],
            interpretation=owner["interpretation"],
            standardise=owner["standardise"],
            variable_type="dummy",
            source="owner",
            order=1000,
        )

    for order, interaction in enumerate(get_active_interactions(), start=1500):
        for period in config["periods"]:
            column = build_interaction_column_name(interaction["name"], period)
            registry[column] = make_registry_entry(
                display_name=interaction["display_name"],
                interpretation=interaction["interpretation"],
                standardise=interaction["standardise"],
                variable_type="interaction",
                source="interaction",
                order=order,
                period=period,
            )

    if config["include_lag_growth"]:
        for period in config["lag_growth_periods"]:
            if period in models:
                column = lag_growth_col(config, period)
                registry[column] = make_registry_entry(
                    display_name=config["lag_growth"]["display_name"],
                    interpretation=config["lag_growth"]["interpretation"],
                    standardise=config["lag_growth"]["standardise"],
                    variable_type="lag",
                    source="lag_growth",
                    order=3000,
                    period=period,
                )

    if categorical_levels is not None:
        for control_order, control in enumerate(config["categorical_controls"], start=2000):
            column = control["column"]
            for level in categorical_levels[column]:
                raw_name = f"{column}_{level}"
                is_reference = str(level) == str(control["reference"])
                registry[raw_name] = make_registry_entry(
                    display_name=f"{control['display_prefix']}{level}" if not is_reference else f"{control['display_prefix']}reference: {level}",
                    interpretation=(
                        f"omitted reference category for {column}"
                        if is_reference
                        else control["interpretation_template"]
                    ),
                    standardise=False,
                    variable_type="categorical_reference" if is_reference else "categorical",
                    source=column,
                    order=control_order,
                )

    registry["const"] = make_registry_entry(
        display_name="const",
        interpretation="intercept",
        standardise=False,
        variable_type="constant",
        source="constant",
        order=4000,
    )
    return registry


def validate_registry_against_models(registry: dict[str, dict[str, Any]], models: dict[str, dict[str, Any]]) -> None:
    missing = []
    for period, model_spec in models.items():
        for column in model_spec["regressors"]:
            if column not in registry:
                missing.append((period, column))
    if missing:
        raise ValueError(f"Internal registry is missing generated model columns: {missing}.")


def validate_model_columns_after_filter(
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
) -> None:
    missing_by_period = {}
    for period, model_spec in models.items():
        model_columns = [model_spec["dependent"], *model_spec["regressors"], *get_categorical_columns(config)]
        missing = sorted(set(model_columns).difference(filtered_df.columns))
        if missing:
            missing_by_period[period] = missing
    if missing_by_period:
        raise ValueError(
            "Generated model columns are missing after sample filtering. "
            f"Missing by period: {missing_by_period}. Edit CONFIG period/regressor/growth settings."
        )


def winsorize_series(series: pd.Series, lower_q: float, upper_q: float) -> tuple[pd.Series, float, float, int]:
    non_missing = series.dropna()
    if non_missing.empty:
        raise ValueError("Cannot winsorise because the dependent variable has no non-missing observations.")
    lower = float(non_missing.quantile(lower_q))
    upper = float(non_missing.quantile(upper_q))
    clipped = series.clip(lower=lower, upper=upper)
    affected = int(((series < lower) | (series > upper)).fillna(False).sum())
    return clipped, lower, upper, affected


def significance_stars(p_value: float) -> str:
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def get_standardisable_regressors(regressors: list[str], registry: dict[str, dict[str, Any]]) -> list[str]:
    return [
        column
        for column in regressors
        if registry.get(column, {}).get("standardise", False)
        and registry.get(column, {}).get("variable_type") in {"numeric", "lag", "dummy", "interaction"}
    ]


def build_design_matrix(
    estimation_df: pd.DataFrame,
    regressors: list[str],
    categorical_levels: dict[str, list[str]],
    registry: dict[str, dict[str, Any]],
    config: dict[str, Any],
    standardised_model: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    categorical_columns = get_categorical_columns(config)
    numeric_regressors = [column for column in regressors if column not in categorical_columns]
    x_raw_numeric = estimation_df[numeric_regressors].apply(pd.to_numeric, errors="coerce").astype(float)
    x_model_numeric = x_raw_numeric.copy()

    included_in_standardisation: list[str] = []
    excluded_from_standardisation: list[str] = []
    if standardised_model:
        standardisable = get_standardisable_regressors(numeric_regressors, registry)
        included_in_standardisation = list(standardisable)
        excluded_from_standardisation = [column for column in numeric_regressors if column not in standardisable]
        if standardisable:
            scaler = StandardScaler()
            x_model_numeric.loc[:, standardisable] = scaler.fit_transform(x_model_numeric[standardisable])
    else:
        excluded_from_standardisation = list(numeric_regressors)

    x_raw = x_raw_numeric.copy()
    x_model = x_model_numeric.copy()

    for column in categorical_columns:
        categories = categorical_levels[column]
        control = get_categorical_control(config, column)
        categorical_series = pd.Series(
            pd.Categorical(estimation_df[column].astype(str), categories=categories),
            index=estimation_df.index,
            name=column,
        )
        dummies = pd.get_dummies(categorical_series, prefix=column, drop_first=False, dtype=float)
        expected_columns = [f"{column}_{level}" for level in categories]
        dummies = dummies.reindex(columns=expected_columns, fill_value=0.0)
        reference_column = f"{column}_{control['reference']}"
        dummies = dummies.drop(columns=[reference_column], errors="ignore")
        x_raw = pd.concat([x_raw, dummies], axis=1)
        x_model = pd.concat([x_model, dummies], axis=1)
        excluded_from_standardisation.extend(dummies.columns.tolist())

    x_raw = sm.add_constant(x_raw, has_constant="add")
    x_model = sm.add_constant(x_model, has_constant="add")
    excluded_from_standardisation.append("const")
    return x_model, x_raw, sorted(set(included_in_standardisation)), sorted(set(excluded_from_standardisation))


def get_model_name(period: str, variant: dict[str, Any]) -> str:
    return f"{period}_{variant['suffix']}"


def build_model_registry(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    registry = {}
    for period in config["periods"]:
        for variant in get_model_variants(config):
            model_name = get_model_name(period, variant)
            registry[model_name] = {
                "period": period,
                "suffix": variant["suffix"],
                "model_family": variant["model_family"],
                "winsorised": variant["winsorised"],
                "standardised_model": variant["standardised_model"],
            }
    return registry


def fit_ols(y_model: pd.Series, x_model: pd.DataFrame, config: dict[str, Any]):
    model = sm.OLS(y_model, x_model)
    if config["covariance_type"] == "nonrobust":
        return model.fit()
    return model.fit(cov_type=config["covariance_type"])


def run_model_variant(
    period: str,
    model_spec: dict[str, Any],
    variant: dict[str, Any],
    filtered_df: pd.DataFrame,
    categorical_levels: dict[str, list[str]],
    variable_registry: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    dependent = model_spec["dependent"]
    regressors = [*model_spec["regressors"], *get_categorical_columns(config)]
    model_name = get_model_name(period, variant)

    working = filtered_df.loc[:, [dependent, *regressors]].copy()
    numeric_regressors = [column for column in regressors if column not in get_categorical_columns(config)]
    working[dependent] = pd.to_numeric(working[dependent], errors="coerce")
    working[numeric_regressors] = working[numeric_regressors].apply(pd.to_numeric, errors="coerce")
    estimation_df = working.dropna().copy()
    rows_dropped = len(filtered_df) - len(estimation_df)
    if estimation_df.empty:
        raise ValueError(
            f"Model {model_name} has zero observations after dropping missing dependent/regressor values. "
            "Check generated regressors and sample filter."
        )

    y_effective = estimation_df[dependent].astype(float).copy()
    winsor_lower = None
    winsor_upper = None
    winsor_affected = 0
    dependent_used = dependent

    if variant["winsorised"]:
        y_effective, winsor_lower, winsor_upper, winsor_affected = winsorize_series(
            y_effective,
            config["winsor_lower"],
            config["winsor_upper"],
        )
        dependent_used = f"{dependent}_w"

    x_model, x_raw, standardised_variables, non_standardised_variables = build_design_matrix(
        estimation_df=estimation_df,
        regressors=regressors,
        categorical_levels=categorical_levels,
        registry=variable_registry,
        config=config,
        standardised_model=bool(variant["standardised_model"]),
    )

    y_model = y_effective.copy()
    dependent_standardised = False
    if variant["standardised_model"] and config["standardise_dependent"]:
        y_scaler = StandardScaler()
        y_model = pd.Series(y_scaler.fit_transform(y_model.to_frame()).ravel(), index=y_model.index, dtype=float)
        dependent_standardised = True

    fitted = fit_ols(y_model, x_model, config)

    return {
        "model": model_name,
        "period": period,
        "model_family": variant["model_family"],
        "growth_mode": config["growth_mode"],
        "covariance_type": config["covariance_type"],
        "dependent_variable": dependent_used,
        "generated_dependent_variable": dependent,
        "winsorised": "Yes" if variant["winsorised"] else "No",
        "standardised_model": "Yes" if variant["standardised_model"] else "No",
        "dependent_standardised": "Yes" if dependent_standardised else "No",
        "sample_filter": get_sample_filter(config),
        "observations": int(fitted.nobs),
        "rows_dropped_due_to_missing": rows_dropped,
        "includes_categorical_controls": "Yes" if config["categorical_controls"] else "No",
        "includes_lag_variable": "Yes" if any(variable_registry.get(r, {}).get("variable_type") == "lag" for r in model_spec["regressors"]) else "No",
        "winsorisation_lower_bound": winsor_lower,
        "winsorisation_upper_bound": winsor_upper,
        "observations_affected_by_winsorisation": winsor_affected,
        "generated_regressors": list(model_spec["regressors"]),
        "regressors_used": list(x_raw.columns.drop("const")),
        "standardised_variables": standardised_variables,
        "non_standardised_variables": non_standardised_variables,
        "result": fitted,
    }


def extract_model_summary(model_result: dict[str, Any]) -> dict[str, Any]:
    fitted = model_result["result"]
    return {
        "model": model_result["model"],
        "period": model_result["period"],
        "model_family": model_result["model_family"],
        "growth_mode": model_result["growth_mode"],
        "dependent_variable": model_result["dependent_variable"],
        "covariance_type": model_result["covariance_type"],
        "winsorised": model_result["winsorised"],
        "standardised_model": model_result["standardised_model"],
        "dependent_standardised": model_result["dependent_standardised"],
        "sample_filter": model_result["sample_filter"],
        "observations": model_result["observations"],
        "rows_dropped_due_to_missing": model_result["rows_dropped_due_to_missing"],
        "includes_categorical_controls": model_result["includes_categorical_controls"],
        "includes_lag_variable": model_result["includes_lag_variable"],
        "winsor_lower": model_result["winsorisation_lower_bound"],
        "winsor_upper": model_result["winsorisation_upper_bound"],
        "R_squared": fitted.rsquared,
        "adjusted_R_squared": fitted.rsquared_adj,
        "F_statistic": fitted.fvalue,
        "Prob_F_statistic": fitted.f_pvalue,
    }


def registry_lookup(variable: str, variable_registry: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if variable.endswith("_w") and variable[:-2] in variable_registry:
        base = variable_registry[variable[:-2]].copy()
        base["display_name"] = f"{base['display_name']} (winsorised)"
        return base
    if variable in variable_registry:
        return variable_registry[variable]
    return make_registry_entry(
        display_name=variable,
        interpretation="generated variable not found in registry",
        standardise=False,
        variable_type="unknown",
        source="unknown",
        order=9999,
    )


def add_dependent_variables_to_registry(registry: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    output = dict(registry)
    dependent_specs = {
        "rgrowth_log_ann_P1": ("real annualised log growth P1", "real annualised log sales growth for 2019 -> 2020"),
        "rgrowth_log_ann_P2": ("real annualised log growth P2", "real annualised log sales growth for 2020 -> 2022"),
        "rgrowth_log_ann_P3": ("real annualised log growth P3", "real annualised log sales growth for 2022 -> 2024"),
        "rgrowth_log_ann_2019_2024": ("real annualised log growth FULL", "real annualised log sales growth for 2019 -> 2024"),
        "ngrowth_log_ann_P1": ("nominal annualised log growth P1", "nominal annualised log sales growth for 2019 -> 2020"),
        "ngrowth_log_ann_P2": ("nominal annualised log growth P2", "nominal annualised log sales growth for 2020 -> 2022"),
        "ngrowth_log_ann_P3": ("nominal annualised log growth P3", "nominal annualised log sales growth for 2022 -> 2024"),
        "ngrowth_log_ann_2019_2024": ("nominal annualised log growth FULL", "nominal annualised log sales growth for 2019 -> 2024"),
    }
    for raw_name, (display_name, interpretation) in dependent_specs.items():
        output[raw_name] = make_registry_entry(
            display_name=display_name,
            interpretation=interpretation,
            standardise=True,
            variable_type="dependent",
            source="dependent",
            order=5,
        )
    return output


def variable_label(variable: str, variable_registry: dict[str, dict[str, Any]]) -> str:
    return registry_lookup(variable, variable_registry)["display_name"]


def label_list(raw_variables: str | float | None, variable_registry: dict[str, dict[str, Any]]) -> str:
    if raw_variables is None or pd.isna(raw_variables):
        return ""
    variables = [part.strip() for part in str(raw_variables).split(",") if part.strip()]
    return ", ".join(variable_label(variable, variable_registry) for variable in variables)


def extract_coefficients(
    model_result: dict[str, Any],
    variable_registry: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    fitted = model_result["result"]
    conf_int = fitted.conf_int()
    rows = []

    for variable in fitted.params.index:
        variable_meta = registry_lookup(variable, variable_registry)
        rows.append(
            {
                "model": model_result["model"],
                "period": model_result["period"],
                "model_family": model_result["model_family"],
                "growth_mode": model_result["growth_mode"],
                "dependent_variable": model_result["dependent_variable"],
                "winsorised": model_result["winsorised"],
                "standardised_model": model_result["standardised_model"],
                "sample_filter": model_result["sample_filter"],
                "raw_variable": variable,
                "variable": variable,
                "display_name": variable_meta["display_name"],
                "variable_type": variable_meta["variable_type"],
                "coefficient": fitted.params[variable],
                "std error": fitted.bse[variable],
                "t-stat": fitted.tvalues[variable],
                "p-value": fitted.pvalues[variable],
                "CI lower": conf_int.loc[variable, 0],
                "CI upper": conf_int.loc[variable, 1],
            }
        )

    return pd.DataFrame(rows)


def format_value_cell(value: float, p_value: float) -> str:
    if pd.isna(value) or pd.isna(p_value):
        return ""
    return f"{value:.3f}{significance_stars(p_value)}\n({p_value:.3f})"


def get_comparison_column_label(model_name: str) -> str:
    period, suffix = model_name.split("_", 1)
    display_suffix = COMPARISON_VARIANT_DISPLAY.get(suffix, suffix)
    return f"{period}_{display_suffix}"


def build_comparison_row_order(
    config: dict[str, Any],
    coefficients_df: pd.DataFrame,
    variable_registry: dict[str, dict[str, Any]],
) -> list[str]:
    ordered = [regressor["display_name"] for regressor in config["base_regressors"]]

    if config["include_owner"]:
        ordered.append(config["owner_variable"]["display_name"])

    ordered.extend(
        interaction["display_name"]
        for interaction in get_active_interactions()
    )

    categorical_rows = (
        coefficients_df.loc[coefficients_df["variable_type"] == "categorical", ["raw_variable", "display_name"]]
        .drop_duplicates()
        .assign(order=lambda frame: frame["raw_variable"].map(lambda raw: variable_registry[raw]["order"]))
        .sort_values(["order", "display_name"])
    )
    ordered.extend(categorical_rows["display_name"].tolist())

    if config["include_lag_growth"]:
        ordered.append(config["lag_growth"]["display_name"])

    ordered.extend(["const", *SUMMARY_ROWS])
    return list(dict.fromkeys(ordered))


def validate_comparison_display_names(coefficients_df: pd.DataFrame) -> None:
    duplicates = coefficients_df.loc[
        coefficients_df.duplicated(subset=["model", "display_name"], keep=False),
        ["model", "display_name", "raw_variable", "variable_type"],
    ].sort_values(["model", "display_name", "raw_variable"])

    if duplicates.empty:
        return

    details = []
    for (model, display_name), group in duplicates.groupby(["model", "display_name"], sort=False):
        raw_variables = group["raw_variable"].astype(str).tolist()
        details.append(f"{model}: display_name {display_name!r} is used by raw variables {raw_variables}")

    raise ValueError(
        "Comparison tables require one unique display_name per model, but duplicate labels were generated. "
        "Edit CONFIG so each base_regressors display_name and categorical display label is unique. "
        "Duplicate labels found: "
        + " | ".join(details)
    )


def get_inactive_variant_reason(suffix: str, config: dict[str, Any]) -> str:
    variant_lookup = {variant["suffix"]: variant for variant in config["model_variants"]}
    variant = variant_lookup.get(suffix)
    if variant is None:
        return f"variant {suffix!r} is not listed in CONFIG['model_variants']"
    if variant["standardised_model"] and not config["standardised_models"]:
        return f"variant {suffix!r} requires CONFIG['standardised_models'] = True"
    if variant["winsorised"] and not config["winsorise_dependent"]:
        return f"variant {suffix!r} requires CONFIG['winsorise_dependent'] = True"
    return f"variant {suffix!r} is inactive for an unknown reason"


def validate_comparison_variants_active(variant_suffixes: list[str], config: dict[str, Any]) -> None:
    active_suffixes = {variant["suffix"] for variant in get_model_variants(config)}
    inactive_suffixes = [suffix for suffix in variant_suffixes if suffix not in active_suffixes]
    if inactive_suffixes:
        reasons = [get_inactive_variant_reason(suffix, config) for suffix in inactive_suffixes]
        raise ValueError(
            "Cannot build requested comparison sheet because one or more model variants are inactive: "
            + "; ".join(reasons)
        )


def build_comparison_sheet(
    coefficients_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    config: dict[str, Any],
    row_order: list[str],
    variant_suffixes: list[str],
) -> pd.DataFrame:
    validate_comparison_variants_active(variant_suffixes, config)
    model_names = [
        f"{period}_{suffix}"
        for period in config["periods"]
        for suffix in variant_suffixes
    ]
    missing_models = [model_name for model_name in model_names if model_name not in set(summary_df["model"])]
    if missing_models:
        raise ValueError(f"Cannot build comparison sheet because these expected models were not estimated: {missing_models}")

    subset = coefficients_df.loc[coefficients_df["model"].isin(model_names)].copy()
    subset["formatted"] = subset.apply(lambda row: format_value_cell(row["coefficient"], row["p-value"]), axis=1)
    duplicate_cells = subset.duplicated(subset=["model", "display_name"], keep=False)
    if duplicate_cells.any():
        duplicated = subset.loc[duplicate_cells, ["model", "display_name", "raw_variable"]]
        raise ValueError(
            "Cannot build comparison table because multiple raw variables map to the same display_name "
            f"within a model: {duplicated.to_dict(orient='records')}"
        )
    table = subset.pivot(index="display_name", columns="model", values="formatted")
    table = table.reindex(index=row_order, columns=model_names).fillna("")
    table.columns = [get_comparison_column_label(model_name) for model_name in model_names]

    summary_indexed = summary_df.set_index("model")
    table = table.astype(object)
    table.loc["N"] = summary_indexed.loc[model_names, "observations"].astype(int).astype(str).tolist()
    table.loc["R-squared"] = summary_indexed.loc[model_names, "R_squared"].map(lambda x: f"{x:.3f}").tolist()
    table.loc["Adjusted R-squared"] = summary_indexed.loc[model_names, "adjusted_R_squared"].map(lambda x: f"{x:.3f}").tolist()

    return table.reset_index().rename(columns={"index": "display_name"})


def find_company_name_column(df: pd.DataFrame) -> str | None:
    for column in ["company_name", "name", "company", "firma", "nazwa"]:
        if column in df.columns:
            return column
    return None


def get_model_missing_frame(
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    model_spec: dict[str, Any],
) -> pd.DataFrame:
    dependent = model_spec["dependent"]
    regressors = [*model_spec["regressors"], *get_categorical_columns(config)]
    working = filtered_df.loc[:, [dependent, *regressors]].copy()
    numeric_regressors = [column for column in regressors if column not in get_categorical_columns(config)]
    working[dependent] = pd.to_numeric(working[dependent], errors="coerce")
    working[numeric_regressors] = working[numeric_regressors].apply(pd.to_numeric, errors="coerce")
    return working.isna()


def get_missing_reason(dependent_missing: bool, control_missing: bool) -> str:
    if dependent_missing and control_missing:
        return "missing dependent and regressor/control"
    if dependent_missing:
        return "missing dependent variable"
    return "missing regressor/control"


def build_dropped_rows_table(
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    output_columns = [
        "model",
        "period",
        "model_family",
        "winsorised",
        "standardised_model",
        "nip",
        "company",
        "dependent_variable",
        "missing_columns",
        "missing_values_count",
        "row_index",
        "reason",
    ]
    rows = []
    company_column = find_company_name_column(filtered_df)

    for period, model_spec in models.items():
        dependent = model_spec["dependent"]
        regressors = [*model_spec["regressors"], *get_categorical_columns(config)]
        model_columns = [dependent, *regressors]
        missing_frame = get_model_missing_frame(filtered_df, config, model_spec)
        dropped_mask = missing_frame.any(axis=1)
        if not dropped_mask.any():
            continue

        for variant in get_model_variants(config):
            model_name = get_model_name(period, variant)
            for row_index, missing_values in missing_frame.loc[dropped_mask].iterrows():
                missing_columns = [column for column in model_columns if bool(missing_values[column])]
                dependent_missing = dependent in missing_columns
                control_missing = any(column != dependent for column in missing_columns)
                rows.append(
                    {
                        "model": model_name,
                        "period": period,
                        "model_family": variant["model_family"],
                        "winsorised": "Yes" if variant["winsorised"] else "No",
                        "standardised_model": "Yes" if variant["standardised_model"] else "No",
                        "nip": filtered_df.at[row_index, "nip"] if "nip" in filtered_df.columns else None,
                        "company": filtered_df.at[row_index, company_column] if company_column else None,
                        "dependent_variable": dependent,
                        "missing_columns": ", ".join(missing_columns),
                        "missing_values_count": len(missing_columns),
                        "row_index": row_index,
                        "reason": get_missing_reason(dependent_missing, control_missing),
                    }
                )

    if not rows:
        return pd.DataFrame(columns=output_columns)
    return pd.DataFrame(rows, columns=output_columns)


def summarize_top_missing_columns(dropped_rows_df: pd.DataFrame, model_name: str) -> str:
    if dropped_rows_df.empty:
        return "No dropped rows"
    subset = dropped_rows_df.loc[dropped_rows_df["model"] == model_name, "missing_columns"]
    if subset.empty:
        return "No dropped rows"

    counts: dict[str, int] = {}
    for value in subset.dropna().astype(str):
        for column in [part.strip() for part in value.split(",") if part.strip()]:
            counts[column] = counts.get(column, 0) + 1
    if not counts:
        return "No dropped rows"
    return ", ".join(f"{column}: {count}" for column, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def get_generated_interaction_columns_by_period(config: dict[str, Any]) -> str:
    active_interactions = get_active_interactions()
    if not active_interactions:
        return "No interaction terms configured"
    return "; ".join(
        f"{period}: "
        + ", ".join(build_interaction_column_name(interaction["name"], period) for interaction in active_interactions)
        for period in config["periods"]
    )


def build_diagnostics_table(
    model_results: list[dict[str, Any]],
    filtered_df: pd.DataFrame,
    input_df: pd.DataFrame,
    config: dict[str, Any],
    dropped_rows_df: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    generated_dependents = {period: growth_col(config, period) for period in config["periods"]}
    format_list = lambda values: ", ".join(values) if values else "No variables"
    for model_result in model_results:
        rows.append(
            {
                "analysis_name": config["analysis_name"],
                "sample_name": config["sample_name"],
                "growth_mode": config["growth_mode"],
                "base_sample_filter": config["base_sample_filter"],
                "sample_filter": model_result["sample_filter"],
                "input_row_count": len(input_df),
                "row_count_after_filter": len(filtered_df),
                "unique_firms_after_filter": filtered_df["nip"].nunique() if "nip" in filtered_df else None,
                "model": model_result["model"],
                "period": model_result["period"],
                "model_family": model_result["model_family"],
                "covariance_type": model_result["covariance_type"],
                "generated_dependent_variables_by_period": "; ".join(f"{p}: {v}" for p, v in generated_dependents.items()),
                "interaction_terms_configured": format_list([interaction["name"] for interaction in get_active_interactions()]).replace("No variables", "No interaction terms configured"),
                "generated_interaction_columns_by_period": get_generated_interaction_columns_by_period(config),
                "dependent_variable": model_result["dependent_variable"],
                "generated_regressors_for_period": ", ".join(model_result["generated_regressors"]),
                "x_variables_used": ", ".join(model_result["regressors_used"]),
                "variables_included_in_standardisation": format_list(model_result["standardised_variables"]),
                "variables_excluded_from_standardisation": format_list(model_result["non_standardised_variables"]),
                "winsorisation_lower_bound": model_result["winsorisation_lower_bound"],
                "winsorisation_upper_bound": model_result["winsorisation_upper_bound"],
                "observations_affected_by_winsorisation": model_result["observations_affected_by_winsorisation"],
                "rows_dropped_due_to_missing": model_result["rows_dropped_due_to_missing"],
                "dropped_unique_firms": (
                    dropped_rows_df.loc[dropped_rows_df["model"] == model_result["model"], "nip"].nunique()
                    if "nip" in dropped_rows_df.columns and not dropped_rows_df.empty
                    else 0
                ),
                "top_missing_columns": summarize_top_missing_columns(dropped_rows_df, model_result["model"]),
            }
        )
    return pd.DataFrame(rows)


def build_variable_labels_table(variable_registry: dict[str, dict[str, Any]], coefficients_df: pd.DataFrame) -> pd.DataFrame:
    observed_variables = set(coefficients_df["raw_variable"].dropna().astype(str).tolist())
    rows = []
    for raw_name, meta in variable_registry.items():
        if meta["variable_type"] == "categorical_reference" or raw_name in observed_variables or meta["source"] in {"owner", "lag_growth", "constant"}:
            rows.append(
                {
                    "raw_name": raw_name,
                    "display_name": meta["display_name"],
                    "variable_type": meta["variable_type"],
                    "standardise": meta["standardise"],
                    "interpretation": meta["interpretation"],
                }
            )

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["raw_name"], keep="first")
        .sort_values(["variable_type", "display_name", "raw_name"])
        .reset_index(drop=True)
    )


def write_workbook(
    summary_df: pd.DataFrame,
    compare_main_df: pd.DataFrame,
    compare_raw_df: pd.DataFrame,
    coefficients_all_df: pd.DataFrame,
    diagnostics_df: pd.DataFrame,
    dropped_rows_df: pd.DataFrame,
    variable_labels_df: pd.DataFrame,
    config: dict[str, Any],
) -> list[str]:
    output_path = Path(config["output_file"])
    workbook_tables: dict[str, pd.DataFrame] = {
        "Model_Summary": summary_df,
        "Compare_Main": compare_main_df,
        "Compare_Raw": compare_raw_df,
        "Coefficients_All": coefficients_all_df,
        "Diagnostics": diagnostics_df,
        "Dropped_Rows": dropped_rows_df,
        "Variable_Labels": variable_labels_df,
    }

    actual_sheet_names = list(workbook_tables.keys())
    if actual_sheet_names != OUTPUT_SHEETS:
        raise ValueError(f"Internal workbook sheet order changed. Expected={OUTPUT_SHEETS}, actual={actual_sheet_names}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sheet_name, table in workbook_tables.items():
            table.to_excel(writer, sheet_name=sheet_name, index=False)
    return actual_sheet_names


def compute_r_squared_matches(summary_df: pd.DataFrame, model_registry: dict[str, dict[str, Any]]) -> dict[str, bool]:
    matches: dict[str, bool] = {}
    registry_df = pd.DataFrame(
        [
            {
                "model": model_name,
                "period": meta["period"],
                "winsorised": meta["winsorised"],
                "standardised_model": meta["standardised_model"],
            }
            for model_name, meta in model_registry.items()
        ]
    )
    summary_lookup = summary_df.set_index("model")

    for (_period, winsorised), group in registry_df.groupby(["period", "winsorised"], dropna=False):
        raw_models = group.loc[group["standardised_model"] == False, "model"].tolist()
        std_models = group.loc[group["standardised_model"] == True, "model"].tolist()
        if len(raw_models) == 1 and len(std_models) == 1:
            raw_model = raw_models[0]
            std_model = std_models[0]
            matches[raw_model] = abs(float(summary_lookup.loc[raw_model, "R_squared"]) - float(summary_lookup.loc[std_model, "R_squared"])) < 1e-12
    return matches


def print_validation(
    input_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    models: dict[str, dict[str, Any]],
    model_results: list[dict[str, Any]],
    summary_df: pd.DataFrame,
    model_registry: dict[str, dict[str, Any]],
    variable_registry: dict[str, dict[str, Any]],
    written_sheets: list[str],
    config: dict[str, Any],
) -> None:
    print("CONFIG-driven period OLS completed")
    print(f"analysis_name: {config['analysis_name']}")
    print(f"sample_name: {config['sample_name']}")
    print(f"growth_mode: {config['growth_mode']}")
    print(f"base_sample_filter: {config['base_sample_filter']}")
    print(f"sample_filter: {get_sample_filter(config)}")
    print(f"covariance_type: {config['covariance_type']}")
    print(f"input_row_count: {len(input_df):,}")
    print(f"output_model_count: {len(model_results):,}")
    print(f"number_of_observations_after_filter: {len(filtered_df):,}")
    if "nip" in filtered_df:
        print(f"number_of_firms_after_filter: {filtered_df['nip'].nunique():,}")
        print(f"duplicate_firm_check_after_filter: {filtered_df['nip'].duplicated().sum():,}")
    print("generated_dependent_variables_per_period:")
    for period, model_spec in models.items():
        print(f"{period}: {model_spec['dependent']}")
    print("generated_regressors_per_period:")
    for period, model_spec in models.items():
        print(f"{period}: {model_spec['regressors']}")
    print("standardisation_registry:")
    included = sorted([name for name, meta in variable_registry.items() if meta["standardise"]])
    excluded = sorted([name for name, meta in variable_registry.items() if not meta["standardise"]])
    print(f"included: {included}")
    print(f"excluded: {excluded}")
    print("winsorised_observations_count_per_model:")
    for model_result in model_results:
        if model_result["winsorised"] == "Yes":
            print(f"{model_result['model']}: {model_result['observations_affected_by_winsorisation']}")
    print("rows_dropped_due_to_missing_per_model:")
    for model_result in model_results:
        print(f"{model_result['model']}: {model_result['rows_dropped_due_to_missing']}")
    print("missing_counts_for_generated_columns_after_filter:")
    required_columns = get_required_columns(config, models)
    missing_counts = filtered_df[required_columns].isna().sum().to_dict()
    for column, count in missing_counts.items():
        print(f"{column}: {int(count)}")
    print("summary_statistics_for_key_outcomes:")
    dependent_columns = [model_spec["dependent"] for model_spec in models.values()]
    print(filtered_df[dependent_columns].describe().to_string())
    print(f"final_coefficient_column_list: {COEFFICIENT_OUTPUT_COLUMNS}")
    print(f"final_coefficient_column_count: {len(COEFFICIENT_OUTPUT_COLUMNS)}")
    print(f"R_squared_raw_equals_std: {compute_r_squared_matches(summary_df, model_registry)}")
    print(f"output_sheets_written: {written_sheets}")
    print("registry_generates_labels_and_comparison_rows: True")


def run_period_ols(config: dict[str, Any] = CONFIG) -> dict[str, Any]:
    config = dict(config)
    validate_user_config(config)
    config = normalise_config(config)
    validate_config(config)

    models = build_models(config)
    pre_filter_registry = build_variable_registry(config, models)
    validate_registry_against_models(pre_filter_registry, models)

    input_df = load_input_data(config)
    validate_input_columns(input_df, config, models)
    filtered_df = apply_sample_filter(input_df, config)
    filtered_df = add_interaction_columns(filtered_df, config, models)
    validate_model_columns_after_filter(filtered_df, config, models)
    categorical_levels = get_categorical_levels(filtered_df, config)

    variable_registry = build_variable_registry(config, models, categorical_levels)
    validate_registry_against_models(variable_registry, models)
    model_registry = build_model_registry(config)

    model_results = []
    for period, model_spec in models.items():
        for variant in get_model_variants(config):
            model_results.append(
                run_model_variant(
                    period=period,
                    model_spec=model_spec,
                    variant=variant,
                    filtered_df=filtered_df,
                    categorical_levels=categorical_levels,
                    variable_registry=variable_registry,
                    config=config,
                )
            )

    summary_df = pd.DataFrame([extract_model_summary(model_result) for model_result in model_results])
    coefficients_internal_df = pd.concat(
        [extract_coefficients(model_result, variable_registry) for model_result in model_results],
        ignore_index=True,
    )
    validate_comparison_display_names(coefficients_internal_df)
    coefficients_all_df = coefficients_internal_df[COEFFICIENT_OUTPUT_COLUMNS]

    row_order = build_comparison_row_order(config, coefficients_internal_df, variable_registry)
    compare_main_df = build_comparison_sheet(
        coefficients_df=coefficients_internal_df,
        summary_df=summary_df,
        config=config,
        row_order=row_order,
        variant_suffixes=COMPARE_MAIN_VARIANTS,
    )
    compare_raw_df = build_comparison_sheet(
        coefficients_df=coefficients_internal_df,
        summary_df=summary_df,
        config=config,
        row_order=row_order,
        variant_suffixes=COMPARE_RAW_VARIANTS,
    )
    dropped_rows_df = build_dropped_rows_table(filtered_df, config, models)
    diagnostics_df = build_diagnostics_table(model_results, filtered_df, input_df, config, dropped_rows_df)
    variable_labels_df = build_variable_labels_table(variable_registry, coefficients_internal_df)

    written_sheets = write_workbook(
        summary_df=summary_df,
        compare_main_df=compare_main_df,
        compare_raw_df=compare_raw_df,
        coefficients_all_df=coefficients_all_df,
        diagnostics_df=diagnostics_df,
        dropped_rows_df=dropped_rows_df,
        variable_labels_df=variable_labels_df,
        config=config,
    )
    print_validation(
        input_df=input_df,
        filtered_df=filtered_df,
        models=models,
        model_results=model_results,
        summary_df=summary_df,
        model_registry=model_registry,
        variable_registry=variable_registry,
        written_sheets=written_sheets,
        config=config,
    )
    return {
        "summary_df": summary_df,
        "coefficients_df": coefficients_all_df,
        "compare_main_df": compare_main_df,
        "compare_raw_df": compare_raw_df,
        "diagnostics_df": diagnostics_df,
        "dropped_rows_df": dropped_rows_df,
        "variable_labels_df": variable_labels_df,
        "written_sheets": written_sheets,
        "models": models,
        "variable_registry": variable_registry,
        "config_used": {**config, "sample_filter": get_sample_filter(config)},
    }


def make_run_log_row(
    scenario_name: str,
    filter_query: str | None,
    starting_rows: int,
    rows_after_scenario_filter: int | None,
    event: str,
    warning: str | None = None,
    model: str | None = None,
    period: str | None = None,
    model_family: str | None = None,
    observations_used: int | None = None,
    rows_dropped_due_to_missing: int | None = None,
) -> dict[str, Any]:
    return {
        "scenario": scenario_name,
        "filter_query": filter_query if filter_query is not None else "None",
        "starting_number_of_rows": starting_rows,
        "rows_after_scenario_filter": rows_after_scenario_filter,
        "event": event,
        "model": model,
        "period": period,
        "model_family": model_family,
        "observations_used": observations_used,
        "rows_dropped_due_to_missing": rows_dropped_due_to_missing,
        "warning": warning,
    }


def apply_scenario_filter(
    df: pd.DataFrame,
    scenario_name: str,
    filter_query: str | None,
) -> pd.DataFrame:
    if filter_query is None:
        return df.copy()
    try:
        return df.query(filter_query, engine="python").copy()
    except Exception as exc:
        raise ValueError(
            f"Scenario {scenario_name!r} filter failed: {filter_query!r}. "
            "Check that every referenced column exists and that the query is valid."
        ) from exc


def check_model_has_enough_observations(
    period: str,
    model_spec: dict[str, Any],
    variant: dict[str, Any],
    filtered_df: pd.DataFrame,
    categorical_levels: dict[str, list[str]],
    variable_registry: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> tuple[bool, int, int, int, str | None]:
    dependent = model_spec["dependent"]
    regressors = [*model_spec["regressors"], *get_categorical_columns(config)]
    working = filtered_df.loc[:, [dependent, *regressors]].copy()
    numeric_regressors = [column for column in regressors if column not in get_categorical_columns(config)]
    working[dependent] = pd.to_numeric(working[dependent], errors="coerce")
    working[numeric_regressors] = working[numeric_regressors].apply(pd.to_numeric, errors="coerce")
    estimation_df = working.dropna().copy()
    rows_dropped = len(filtered_df) - len(estimation_df)
    model_name = get_model_name(period, variant)

    if estimation_df.empty:
        return False, 0, 0, rows_dropped, (
            f"Model {model_name} skipped: zero observations after dropping missing dependent/regressor values."
        )

    x_model, _x_raw, _standardised_variables, _non_standardised_variables = build_design_matrix(
        estimation_df=estimation_df,
        regressors=regressors,
        categorical_levels=categorical_levels,
        registry=variable_registry,
        config=config,
        standardised_model=bool(variant["standardised_model"]),
    )
    observations = len(estimation_df)
    parameters = x_model.shape[1]
    minimum_required = parameters + MIN_EXTRA_RESIDUAL_DF
    if observations < minimum_required:
        return False, observations, parameters, rows_dropped, (
            f"Model {model_name} skipped: too few observations after missing-value filtering "
            f"({observations} observations for {parameters} parameters; minimum required is {minimum_required})."
        )
    return True, observations, parameters, rows_dropped, None


def add_scenario_column(frame: pd.DataFrame, scenario_name: str) -> pd.DataFrame:
    output = frame.copy()
    if "scenario" in output.columns:
        output["scenario"] = scenario_name
    else:
        output.insert(0, "scenario", scenario_name)
    return output


def build_compare_sheet_for_scenario(
    coefficients_internal_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    scenario_name: str,
    config: dict[str, Any],
    variable_registry: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    coefficients_subset = coefficients_internal_df.loc[coefficients_internal_df["scenario"] == scenario_name].copy()
    summary_subset = summary_df.loc[summary_df["scenario"] == scenario_name].copy()
    if coefficients_subset.empty or summary_subset.empty:
        return pd.DataFrame()
    row_order = build_comparison_row_order(config, coefficients_subset, variable_registry)
    coefficients_without_scenario = coefficients_subset.drop(columns=["scenario"])
    summary_without_scenario = summary_subset.drop(columns=["scenario"])
    return build_comparison_sheet(
        coefficients_df=coefficients_without_scenario,
        summary_df=summary_without_scenario,
        config=config,
        row_order=row_order,
        variant_suffixes=COMPARE_MAIN_VARIANTS,
    )


def run_models_for_scenario(
    input_df: pd.DataFrame,
    scenario_name: str,
    filter_query: str | None,
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
    pre_filter_registry: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    starting_rows = len(input_df)
    run_log_rows = []
    scenario_config = dict(config)
    scenario_config["sample_name"] = scenario_name
    scenario_config["base_sample_filter"] = "True"

    try:
        scenario_df = apply_scenario_filter(input_df, scenario_name, filter_query)
    except ValueError as exc:
        run_log_rows.append(
            make_run_log_row(
                scenario_name=scenario_name,
                filter_query=filter_query,
                starting_rows=starting_rows,
                rows_after_scenario_filter=None,
                event="scenario_failed",
                warning=str(exc),
            )
        )
        return {
            "summary_df": pd.DataFrame(),
            "coefficients_internal_df": pd.DataFrame(),
            "coefficients_long_df": pd.DataFrame(),
            "diagnostics_df": pd.DataFrame(),
            "compare_df": pd.DataFrame(),
            "dropped_rows_df": pd.DataFrame(),
            "filtered_df": pd.DataFrame(),
            "variable_registry": {},
            "models": models,
            "run_log_rows": run_log_rows,
            "models_estimated": 0,
            "models_skipped": len(models) * len(get_model_variants(config)),
            "warnings": [str(exc)],
        }

    rows_after_scenario_filter = len(scenario_df)
    if scenario_df.empty:
        warning = f"Scenario {scenario_name!r} returned zero rows before trajectory filtering."
        run_log_rows.append(
            make_run_log_row(
                scenario_name,
                filter_query,
                starting_rows,
                rows_after_scenario_filter,
                "scenario_failed",
                warning,
            )
        )
        return {
            "summary_df": pd.DataFrame(),
            "coefficients_internal_df": pd.DataFrame(),
            "coefficients_long_df": pd.DataFrame(),
            "diagnostics_df": pd.DataFrame(),
            "compare_df": pd.DataFrame(),
            "dropped_rows_df": pd.DataFrame(),
            "filtered_df": pd.DataFrame(),
            "variable_registry": {},
            "models": models,
            "run_log_rows": run_log_rows,
            "models_estimated": 0,
            "models_skipped": len(models) * len(get_model_variants(config)),
            "warnings": [warning],
        }

    warnings = []
    try:
        validate_input_columns(scenario_df, scenario_config, models)
        filtered_df = apply_sample_filter(scenario_df, scenario_config)
        filtered_df = add_interaction_columns(filtered_df, scenario_config, models)
        validate_model_columns_after_filter(filtered_df, scenario_config, models)
        categorical_levels = get_categorical_levels(filtered_df, scenario_config)
        variable_registry = add_dependent_variables_to_registry(build_variable_registry(scenario_config, models, categorical_levels))
        validate_registry_against_models(variable_registry, models)
    except Exception as exc:
        warning = f"Scenario {scenario_name!r} could not run: {exc}"
        run_log_rows.append(
            make_run_log_row(
                scenario_name,
                filter_query,
                starting_rows,
                rows_after_scenario_filter,
                "scenario_failed",
                warning,
            )
        )
        return {
            "summary_df": pd.DataFrame(),
            "coefficients_internal_df": pd.DataFrame(),
            "coefficients_long_df": pd.DataFrame(),
            "diagnostics_df": pd.DataFrame(),
            "compare_df": pd.DataFrame(),
            "dropped_rows_df": pd.DataFrame(),
            "filtered_df": pd.DataFrame(),
            "variable_registry": {},
            "models": models,
            "run_log_rows": run_log_rows,
            "models_estimated": 0,
            "models_skipped": len(models) * len(get_model_variants(config)),
            "warnings": [warning],
        }

    model_results = []
    models_skipped = 0
    for period, model_spec in models.items():
        for variant in get_model_variants(scenario_config):
            model_name = get_model_name(period, variant)
            can_run, observations, _parameters, rows_dropped, warning = check_model_has_enough_observations(
                period=period,
                model_spec=model_spec,
                variant=variant,
                filtered_df=filtered_df,
                categorical_levels=categorical_levels,
                variable_registry=variable_registry,
                config=scenario_config,
            )
            if not can_run:
                models_skipped += 1
                warnings.append(warning)
                run_log_rows.append(
                    make_run_log_row(
                        scenario_name,
                        filter_query,
                        starting_rows,
                        rows_after_scenario_filter,
                        "model_skipped",
                        warning,
                        model=model_name,
                        period=period,
                        model_family=variant["model_family"],
                        observations_used=observations,
                        rows_dropped_due_to_missing=rows_dropped,
                    )
                )
                continue

            try:
                model_result = run_model_variant(
                    period=period,
                    model_spec=model_spec,
                    variant=variant,
                    filtered_df=filtered_df,
                    categorical_levels=categorical_levels,
                    variable_registry=variable_registry,
                    config=scenario_config,
                )
                model_results.append(model_result)
                run_log_rows.append(
                    make_run_log_row(
                        scenario_name,
                        filter_query,
                        starting_rows,
                        rows_after_scenario_filter,
                        "model_estimated",
                        model=model_name,
                        period=period,
                        model_family=variant["model_family"],
                        observations_used=model_result["observations"],
                        rows_dropped_due_to_missing=model_result["rows_dropped_due_to_missing"],
                    )
                )
            except Exception as exc:
                models_skipped += 1
                warning = f"Model {model_name} skipped after estimation error: {exc}"
                warnings.append(warning)
                run_log_rows.append(
                    make_run_log_row(
                        scenario_name,
                        filter_query,
                        starting_rows,
                        rows_after_scenario_filter,
                        "model_skipped",
                        warning,
                        model=model_name,
                        period=period,
                        model_family=variant["model_family"],
                        observations_used=observations,
                        rows_dropped_due_to_missing=rows_dropped,
                    )
                )

    if not model_results:
        return {
            "summary_df": pd.DataFrame(),
            "coefficients_internal_df": pd.DataFrame(),
            "coefficients_long_df": pd.DataFrame(),
            "diagnostics_df": pd.DataFrame(),
            "compare_df": pd.DataFrame(),
            "dropped_rows_df": pd.DataFrame(),
            "filtered_df": filtered_df,
            "variable_registry": variable_registry,
            "models": models,
            "run_log_rows": run_log_rows,
            "models_estimated": 0,
            "models_skipped": models_skipped,
            "warnings": warnings,
        }

    summary_df = add_scenario_column(
        pd.DataFrame([extract_model_summary(model_result) for model_result in model_results]),
        scenario_name,
    )
    coefficients_internal_df = add_scenario_column(
        pd.concat(
            [extract_coefficients(model_result, variable_registry) for model_result in model_results],
            ignore_index=True,
        ),
        scenario_name,
    )
    validate_comparison_display_names(coefficients_internal_df.drop(columns=["scenario"]))
    coefficients_long_df = coefficients_internal_df[["scenario", *COEFFICIENT_OUTPUT_COLUMNS]]
    dropped_rows_df = build_dropped_rows_table(filtered_df, scenario_config, models)
    diagnostics_df = add_scenario_column(
        build_diagnostics_table(model_results, filtered_df, scenario_df, scenario_config, dropped_rows_df),
        scenario_name,
    )

    try:
        compare_df = build_compare_sheet_for_scenario(
            coefficients_internal_df=coefficients_internal_df,
            summary_df=summary_df,
            scenario_name=scenario_name,
            config=scenario_config,
            variable_registry=variable_registry,
        )
    except Exception as exc:
        warning = f"Scenario {scenario_name!r} comparison sheet could not be built: {exc}"
        warnings.append(warning)
        run_log_rows.append(
            make_run_log_row(
                scenario_name,
                filter_query,
                starting_rows,
                rows_after_scenario_filter,
                "compare_sheet_warning",
                warning,
            )
        )
        compare_df = pd.DataFrame()

    return {
        "summary_df": summary_df,
        "coefficients_internal_df": coefficients_internal_df,
        "coefficients_long_df": coefficients_long_df,
        "diagnostics_df": diagnostics_df,
        "compare_df": compare_df,
        "dropped_rows_df": add_scenario_column(dropped_rows_df, scenario_name),
        "filtered_df": filtered_df,
        "variable_registry": variable_registry,
        "models": models,
        "run_log_rows": run_log_rows,
        "models_estimated": len(model_results),
        "models_skipped": models_skipped,
        "warnings": warnings,
    }


def empty_long_frames() -> dict[str, pd.DataFrame]:
    return {
        "summary": pd.DataFrame(columns=["scenario"]),
        "coefficients": pd.DataFrame(columns=["scenario", *COEFFICIENT_OUTPUT_COLUMNS]),
        "diagnostics": pd.DataFrame(columns=["scenario"]),
    }


def build_readme_sheet(config: dict[str, Any]) -> pd.DataFrame:
    scenario_text = "; ".join(f"{name}: {query if query is not None else 'all firms'}" for name, query in SCENARIOS.items())
    variant_text = "; ".join(
        f"{variant['suffix']} ({variant['model_family']}, winsorised={variant['winsorised']}, standardised={variant['standardised_model']})"
        for variant in get_model_variants(config)
    )
    rows = [
        ("Analysis name", config["analysis_name"]),
        ("Input file", config["input_file"]),
        ("Output file", config["output_file"]),
        ("Growth mode", config["growth_mode"]),
        ("Model periods", ", ".join(config["periods"])),
        ("Scenario definitions", scenario_text),
        ("Model variants", variant_text),
        ("Dependent variable logic", "P1/P2/P3 use annualised log growth by period; FULL uses 2019-2024 annualised log growth."),
        ("Winsorisation", f"{config['winsorise_dependent']} with bounds {config['winsor_lower']} and {config['winsor_upper']} for winsor variants."),
        ("Standardisation", f"standardised_models={config['standardised_models']}; standardise_dependent={config['standardise_dependent']}."),
        ("Significance stars", "*** p<0.01; ** p<0.05; * p<0.10."),
        ("Workbook structure", "Working tabs come first. Tabs after AUDIT_AND_TECHNICAL_TABS are technical diagnostics and reproducibility audit trails."),
    ]
    return pd.DataFrame(rows, columns=["item", "description"])


def build_separator_sheet() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "message": [
                "The tabs after this point contain technical diagnostics, dropped-row audit trails, full coefficient outputs, variable dictionaries, and run logs for reproducibility."
            ]
        }
    )


def add_variable_labels_to_frame(
    frame: pd.DataFrame,
    variable_registry: dict[str, dict[str, Any]],
    raw_column: str,
    label_column: str,
) -> pd.DataFrame:
    output = frame.copy()
    if raw_column in output.columns:
        output[label_column] = output[raw_column].map(lambda value: variable_label(str(value), variable_registry) if pd.notna(value) else "")
    return output


def add_summary_labels(summary_df: pd.DataFrame, variable_registry: dict[str, dict[str, Any]]) -> pd.DataFrame:
    output = add_variable_labels_to_frame(summary_df, variable_registry, "dependent_variable", "dependent_variable_label")
    preferred = [
        "scenario",
        "period",
        "model",
        "model_family",
        "growth_mode",
        "dependent_variable",
        "dependent_variable_label",
        "winsorised",
        "standardised_model",
        "includes_lag_variable",
        "sample_filter",
        "observations",
        "R_squared",
        "adjusted_R_squared",
    ]
    for column in preferred:
        if column not in output.columns:
            output[column] = pd.NA
    return output[[*preferred, *[column for column in output.columns if column not in preferred]]]


def add_diagnostics_labels(diagnostics_df: pd.DataFrame, variable_registry: dict[str, dict[str, Any]]) -> pd.DataFrame:
    output = add_variable_labels_to_frame(diagnostics_df, variable_registry, "dependent_variable", "dependent_variable_label")
    if "generated_regressors_for_period" in output.columns:
        output["generated_regressor_labels_for_period"] = output["generated_regressors_for_period"].map(lambda value: label_list(value, variable_registry))
    if "x_variables_used" in output.columns:
        output["x_variable_labels_used"] = output["x_variables_used"].map(lambda value: label_list(value, variable_registry))
    return output


def add_dropped_row_labels(dropped_rows_df: pd.DataFrame, variable_registry: dict[str, dict[str, Any]]) -> pd.DataFrame:
    output = add_variable_labels_to_frame(dropped_rows_df, variable_registry, "dependent_variable", "dependent_variable_label")
    if "missing_columns" in output.columns:
        output["missing_column_labels"] = output["missing_columns"].map(lambda value: label_list(value, variable_registry))
    preferred = [
        "scenario",
        "model",
        "period",
        "model_family",
        "winsorised",
        "standardised_model",
        "nip",
        "company",
        "dependent_variable",
        "dependent_variable_label",
        "missing_columns",
        "missing_column_labels",
        "missing_values_count",
        "row_index",
        "reason",
    ]
    for column in preferred:
        if column not in output.columns:
            output[column] = pd.NA
    return output[[*preferred, *[column for column in output.columns if column not in preferred]]]


def build_stacked_compare_sheet(
    coefficients_long_df: pd.DataFrame,
    summary_long_df: pd.DataFrame,
    scenario_results: dict[str, dict[str, Any]],
    config: dict[str, Any],
    variant_suffixes: list[str],
) -> pd.DataFrame:
    frames = []
    model_names = [f"{period}_{suffix}" for period in config["periods"] for suffix in variant_suffixes]
    columns = ["scenario", "display_name", *[get_comparison_column_label(model_name) for model_name in model_names]]

    for scenario_name in SCENARIOS:
        coefficients_subset = coefficients_long_df.loc[coefficients_long_df["scenario"] == scenario_name].copy()
        summary_subset = summary_long_df.loc[summary_long_df["scenario"] == scenario_name].copy()
        variable_registry = scenario_results.get(scenario_name, {}).get("variable_registry", {})
        if coefficients_subset.empty or summary_subset.empty:
            frames.append(pd.DataFrame([{"scenario": scenario_name, "display_name": "No models estimated"}], columns=columns))
            continue
        coefficients_subset["formatted"] = coefficients_subset.apply(lambda row: format_value_cell(row["coefficient"], row["p-value"]), axis=1)

        row_order = build_comparison_row_order(config, coefficients_subset, variable_registry)
        subset = coefficients_subset.loc[coefficients_subset["model"].isin(model_names)].copy()
        table = subset.pivot(index="display_name", columns="model", values="formatted")
        table = table.reindex(index=row_order, columns=model_names).fillna("")
        table.columns = [get_comparison_column_label(model_name) for model_name in model_names]

        summary_indexed = summary_subset.set_index("model")
        table = table.astype(object)
        for summary_row, source_column, formatter in [
            ("N", "observations", lambda value: str(int(value))),
            ("R-squared", "R_squared", lambda value: f"{value:.3f}"),
            ("Adjusted R-squared", "adjusted_R_squared", lambda value: f"{value:.3f}"),
        ]:
            table.loc[summary_row] = [
                formatter(summary_indexed.loc[model_name, source_column]) if model_name in summary_indexed.index else ""
                for model_name in model_names
            ]

        scenario_table = table.reset_index().rename(columns={"index": "display_name"})
        scenario_table.insert(0, "scenario", scenario_name)
        frames.append(scenario_table.reindex(columns=columns))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


def get_estimation_sample(
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    model_spec: dict[str, Any],
) -> pd.DataFrame:
    dependent = model_spec["dependent"]
    regressors = [*model_spec["regressors"], *get_categorical_columns(config)]
    working = filtered_df.loc[:, [dependent, *regressors]].copy()
    numeric_regressors = [column for column in regressors if column not in get_categorical_columns(config)]
    working[dependent] = pd.to_numeric(working[dependent], errors="coerce")
    working[numeric_regressors] = working[numeric_regressors].apply(pd.to_numeric, errors="coerce")
    return working.dropna().copy()


def numeric_variables_for_descriptives(model_spec: dict[str, Any], variable_registry: dict[str, dict[str, Any]]) -> list[str]:
    variables = [model_spec["dependent"], *model_spec["regressors"]]
    excluded_types = {"categorical", "categorical_reference", "constant"}
    return [
        variable
        for variable in variables
        if registry_lookup(variable, variable_registry)["variable_type"] not in excluded_types
    ]


def numeric_variables_for_correlations(model_spec: dict[str, Any], variable_registry: dict[str, dict[str, Any]]) -> list[str]:
    variables = numeric_variables_for_descriptives(model_spec, variable_registry)
    return [
        variable
        for variable in variables
        if registry_lookup(variable, variable_registry)["variable_type"] != "interaction"
    ]


def build_descriptive_stats(
    scenario_results: dict[str, dict[str, Any]],
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    rows = []
    for scenario_name, result in scenario_results.items():
        filtered_df = result.get("filtered_df", pd.DataFrame())
        variable_registry = result.get("variable_registry", {})
        if filtered_df.empty:
            continue
        scenario_config = dict(config)
        scenario_config["sample_name"] = scenario_name
        scenario_config["base_sample_filter"] = "True"
        for period, model_spec in models.items():
            estimation_df = get_estimation_sample(filtered_df, scenario_config, model_spec)
            for variant in get_model_variants(scenario_config):
                model_name = get_model_name(period, variant)
                for raw_variable in numeric_variables_for_descriptives(model_spec, variable_registry):
                    series = pd.to_numeric(estimation_df[raw_variable], errors="coerce")
                    rows.append(
                        {
                            "scenario": scenario_name,
                            "period": period,
                            "model": model_name,
                            "sample_type": "estimation_sample",
                            "raw_variable": raw_variable,
                            "variable_label": variable_label(raw_variable, variable_registry),
                            "variable_type": registry_lookup(raw_variable, variable_registry)["variable_type"],
                            "N": int(series.notna().sum()),
                            "missing": int(series.isna().sum()),
                            "mean": series.mean(skipna=True),
                            "sd": series.std(skipna=True),
                            "min": series.min(skipna=True),
                            "p25": series.quantile(0.25),
                            "median": series.median(skipna=True),
                            "p75": series.quantile(0.75),
                            "max": series.max(skipna=True),
                        }
                    )
    return pd.DataFrame(rows)


def build_correlation_long(
    scenario_results: dict[str, dict[str, Any]],
    config: dict[str, Any],
    models: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    rows = []
    for scenario_name, result in scenario_results.items():
        filtered_df = result.get("filtered_df", pd.DataFrame())
        variable_registry = result.get("variable_registry", {})
        if filtered_df.empty:
            continue
        scenario_config = dict(config)
        scenario_config["sample_name"] = scenario_name
        scenario_config["base_sample_filter"] = "True"
        for period, model_spec in models.items():
            estimation_df = get_estimation_sample(filtered_df, scenario_config, model_spec)
            variables = numeric_variables_for_correlations(model_spec, variable_registry)
            for variant in get_model_variants(scenario_config):
                model_name = get_model_name(period, variant)
                for i, variable_1 in enumerate(variables):
                    for variable_2 in variables[i + 1:]:
                        pair = estimation_df[[variable_1, variable_2]].apply(pd.to_numeric, errors="coerce").dropna()
                        rows.append(
                            {
                                "scenario": scenario_name,
                                "period": period,
                                "model": model_name,
                                "sample_type": "estimation_sample",
                                "variable_1": variable_1,
                                "variable_1_label": variable_label(variable_1, variable_registry),
                                "variable_2": variable_2,
                                "variable_2_label": variable_label(variable_2, variable_registry),
                                "correlation": pair[variable_1].corr(pair[variable_2]) if len(pair) >= 2 else pd.NA,
                                "N": len(pair),
                            }
                        )
    return pd.DataFrame(rows)


def build_variable_labels_table_for_scenarios(
    scenario_results: dict[str, dict[str, Any]],
    coefficients_long_df: pd.DataFrame,
) -> pd.DataFrame:
    registries = [result.get("variable_registry", {}) for result in scenario_results.values()]
    combined_registry: dict[str, dict[str, Any]] = {}
    for registry in registries:
        combined_registry.update(registry)
    combined_registry = add_dependent_variables_to_registry(combined_registry)

    observed_variables = set(coefficients_long_df["raw_variable"].dropna().astype(str).tolist()) if not coefficients_long_df.empty else set()
    for result in scenario_results.values():
        for model_spec in result.get("models", {}).values():
            observed_variables.add(model_spec["dependent"])
            observed_variables.update(model_spec["regressors"])

    rows = []
    for raw_name, meta in combined_registry.items():
        if raw_name in observed_variables or meta["variable_type"] in {"dependent", "categorical_reference"} or meta["source"] in {"owner", "lag_growth", "constant"}:
            rows.append(
                {
                    "raw_name": raw_name,
                    "display_name": meta["display_name"],
                    "variable_type": meta["variable_type"],
                    "standardise": meta["standardise"],
                    "interpretation": meta["interpretation"],
                }
            )
    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["raw_name"], keep="first")
        .sort_values(["variable_type", "display_name", "raw_name"])
        .reset_index(drop=True)
    )


def format_workbook(writer: pd.ExcelWriter, workbook_tables: dict[str, pd.DataFrame]) -> None:
    workbook = writer.book
    header_format = workbook.add_format({"bold": True, "bg_color": "#1F4E78", "font_color": "white", "text_wrap": True})
    tech_header_format = workbook.add_format({"bold": True, "bg_color": "#C65911", "font_color": "white", "text_wrap": True})
    separator_header_format = workbook.add_format({"bold": True, "bg_color": "#808080", "font_color": "white", "text_wrap": True})
    wrap_format = workbook.add_format({"text_wrap": True, "valign": "top"})
    working_tabs = {"README", "Compare_Main", "Compare_Raw", "Descriptive_Stats", "Correlation_Long", "Model_Summary_Long"}
    tech_tabs = {"Diagnostics_Long", "Dropped_Rows_Long", "Coefficients_Long", "Variable_Labels", "Run_Log"}
    long_text_columns = {
        "description",
        "message",
        "sample_filter",
        "generated_dependent_variables_by_period",
        "generated_regressors_for_period",
        "generated_regressor_labels_for_period",
        "x_variables_used",
        "x_variable_labels_used",
        "missing_columns",
        "missing_column_labels",
        "interpretation",
        "warning",
        "top_missing_columns",
    }

    for sheet_name, frame in workbook_tables.items():
        worksheet = writer.sheets[sheet_name]
        if sheet_name in working_tabs:
            worksheet.set_tab_color("#5B9BD5")
            header = header_format
        elif sheet_name == "AUDIT_AND_TECHNICAL_TABS":
            worksheet.set_tab_color("#A6A6A6")
            header = separator_header_format
        else:
            worksheet.set_tab_color("#ED7D31")
            header = tech_header_format

        if sheet_name not in {"README", "AUDIT_AND_TECHNICAL_TABS"} and not frame.empty:
            worksheet.autofilter(0, 0, max(len(frame), 1), max(len(frame.columns) - 1, 0))
            if sheet_name in {"Compare_Main", "Compare_Raw"}:
                worksheet.freeze_panes(1, 2)
            else:
                worksheet.freeze_panes(1, 0)

        for col_idx, column in enumerate(frame.columns):
            worksheet.write(0, col_idx, column, header)
            values = frame[column] if not frame.empty else pd.Series([column])
            max_len = max([len(str(column)), *values.head(200).map(lambda value: len(str(value))).tolist()])
            width = min(max(max_len + 2, 12), 45)
            if column in long_text_columns:
                width = min(max(width, 28), 60)
                worksheet.set_column(col_idx, col_idx, width, wrap_format)
            else:
                worksheet.set_column(col_idx, col_idx, width)


def write_scenario_workbook(
    readme_df: pd.DataFrame,
    compare_main_df: pd.DataFrame,
    compare_raw_df: pd.DataFrame,
    descriptive_stats_df: pd.DataFrame,
    correlation_long_df: pd.DataFrame,
    summary_long_df: pd.DataFrame,
    coefficients_long_df: pd.DataFrame,
    diagnostics_long_df: pd.DataFrame,
    separator_df: pd.DataFrame,
    dropped_rows_long_df: pd.DataFrame,
    variable_labels_df: pd.DataFrame,
    run_log_df: pd.DataFrame,
    config: dict[str, Any],
) -> list[str]:
    workbook_tables: dict[str, pd.DataFrame] = {
        "README": readme_df,
        "Compare_Main": compare_main_df,
        "Compare_Raw": compare_raw_df,
        "Descriptive_Stats": descriptive_stats_df,
        "Correlation_Long": correlation_long_df,
        "Model_Summary_Long": summary_long_df,
        "AUDIT_AND_TECHNICAL_TABS": separator_df,
        "Diagnostics_Long": diagnostics_long_df,
        "Dropped_Rows_Long": dropped_rows_long_df,
        "Coefficients_Long": coefficients_long_df,
        "Variable_Labels": variable_labels_df,
        "Run_Log": run_log_df,
    }
    actual_sheet_names = list(workbook_tables)
    if actual_sheet_names != OUTPUT_SHEETS:
        raise ValueError(f"Internal workbook sheet order changed. Expected={OUTPUT_SHEETS}, actual={actual_sheet_names}")

    output_path = Path(config["output_file"])
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sheet_name, table in workbook_tables.items():
            table.to_excel(writer, sheet_name=sheet_name, index=False)
        format_workbook(writer, workbook_tables)
    return actual_sheet_names


def compare_all_with_original_output(summary_long_df: pd.DataFrame, coefficients_long_df: pd.DataFrame) -> dict[str, Any]:
    original_path = Path(ORIGINAL_RESULTS_FILE)
    if not original_path.exists():
        return {
            "checked": False,
            "matches": False,
            "warning": f"Original output file not found: {ORIGINAL_RESULTS_FILE}",
        }

    try:
        original_summary = pd.read_excel(original_path, sheet_name="Model_Summary")
        original_coefficients = pd.read_excel(original_path, sheet_name="Coefficients_All")
    except Exception as exc:
        return {
            "checked": False,
            "matches": False,
            "warning": f"Could not read original output workbook {ORIGINAL_RESULTS_FILE}: {exc}",
        }

    all_summary = summary_long_df.loc[summary_long_df["scenario"] == "ALL"].drop(columns=["scenario"], errors="ignore")
    all_coefficients = coefficients_long_df.loc[coefficients_long_df["scenario"] == "ALL"].drop(columns=["scenario"], errors="ignore")

    summary_matches = original_summary.reset_index(drop=True).equals(all_summary.reset_index(drop=True))
    coefficients_matches = original_coefficients.reset_index(drop=True).equals(all_coefficients.reset_index(drop=True))
    return {
        "checked": True,
        "matches": bool(summary_matches and coefficients_matches),
        "summary_matches": bool(summary_matches),
        "coefficients_matches": bool(coefficients_matches),
        "original_summary_shape": original_summary.shape,
        "all_summary_shape": all_summary.shape,
        "original_coefficients_shape": original_coefficients.shape,
        "all_coefficients_shape": all_coefficients.shape,
    }


def print_scenario_validation(
    input_df: pd.DataFrame,
    summary_long_df: pd.DataFrame,
    coefficients_long_df: pd.DataFrame,
    diagnostics_long_df: pd.DataFrame,
    dropped_rows_long_df: pd.DataFrame,
    variable_labels_df: pd.DataFrame,
    descriptive_stats_df: pd.DataFrame,
    correlation_long_df: pd.DataFrame,
    compare_main_df: pd.DataFrame,
    compare_raw_df: pd.DataFrame,
    run_log_df: pd.DataFrame,
    written_sheets: list[str],
    models_estimated: int,
    models_skipped: int,
    warnings: list[str],
    all_original_comparison: dict[str, Any],
    config: dict[str, Any],
) -> None:
    print("Scenario period OLS completed")
    print(f"analysis_name: {config['analysis_name']}")
    print(f"input_file: {config['input_file']}")
    print(f"output_file: {config['output_file']}")
    print(f"input_row_count: {len(input_df):,}")
    print(f"scenario_count: {len(SCENARIOS):,}")
    print(f"models_estimated: {models_estimated:,}")
    print(f"models_skipped: {models_skipped:,}")
    print(f"Model_Summary_Long_has_scenario_column: {'scenario' in summary_long_df.columns}")
    print(f"Coefficients_Long_has_scenario_column: {'scenario' in coefficients_long_df.columns}")
    print(f"Diagnostics_Long_has_scenario_column: {'scenario' in diagnostics_long_df.columns}")
    print(f"output_sheets_written: {written_sheets}")
    print(f"Dropped_Rows_Long_exists: {'Dropped_Rows_Long' in written_sheets and not dropped_rows_long_df.empty}")
    print(f"Variable_Labels_exists: {'Variable_Labels' in written_sheets and not variable_labels_df.empty}")
    print(f"descriptive_stats_row_count: {len(descriptive_stats_df):,}")
    print(f"correlation_row_count: {len(correlation_long_df):,}")
    print(f"compare_main_scenarios_included: {sorted(compare_main_df['scenario'].dropna().unique().tolist()) if 'scenario' in compare_main_df else []}")
    print(f"compare_raw_scenarios_included: {sorted(compare_raw_df['scenario'].dropna().unique().tolist()) if 'scenario' in compare_raw_df else []}")
    print("workbook_formatting_applied: True")
    print("models_estimated_by_scenario:")
    if not summary_long_df.empty:
        print(summary_long_df.groupby("scenario")["model"].nunique().to_string())
    else:
        print("No models estimated")
    print("models_skipped_by_scenario:")
    skipped = run_log_df.loc[run_log_df["event"] == "model_skipped"].groupby("scenario")["model"].count()
    print(skipped.to_string() if not skipped.empty else "No models skipped")
    print(f"ALL_matches_original_output: {all_original_comparison}")
    print("warnings:")
    if warnings:
        for warning in warnings:
            print(f"- {warning}")
    else:
        print("No warnings")


def run_period_ols_scenarios(config: dict[str, Any] = CONFIG) -> dict[str, Any]:
    config = dict(config)
    validate_user_config(config)
    config = normalise_config(config)
    validate_config(config)

    models = build_models(config)
    pre_filter_registry = build_variable_registry(config, models)
    validate_registry_against_models(pre_filter_registry, models)

    input_df = load_input_data(config)
    validate_input_columns(input_df, config, models)

    scenario_results = {}
    all_warnings = []
    all_run_log_rows = []
    for scenario_name, filter_query in SCENARIOS.items():
        result = run_models_for_scenario(
            input_df=input_df,
            scenario_name=scenario_name,
            filter_query=filter_query,
            config=config,
            models=models,
            pre_filter_registry=pre_filter_registry,
        )
        scenario_results[scenario_name] = result
        all_warnings.extend(result["warnings"])
        all_run_log_rows.extend(result["run_log_rows"])

    frames = empty_long_frames()
    summary_frames = [result["summary_df"] for result in scenario_results.values() if not result["summary_df"].empty]
    coefficient_frames = [result["coefficients_long_df"] for result in scenario_results.values() if not result["coefficients_long_df"].empty]
    diagnostics_frames = [result["diagnostics_df"] for result in scenario_results.values() if not result["diagnostics_df"].empty]
    dropped_row_frames = [result["dropped_rows_df"] for result in scenario_results.values() if not result["dropped_rows_df"].empty]

    summary_long_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else frames["summary"]
    coefficients_long_df = pd.concat(coefficient_frames, ignore_index=True) if coefficient_frames else frames["coefficients"]
    diagnostics_long_df = pd.concat(diagnostics_frames, ignore_index=True) if diagnostics_frames else frames["diagnostics"]
    dropped_rows_long_df = pd.concat(dropped_row_frames, ignore_index=True) if dropped_row_frames else pd.DataFrame(columns=["scenario"])
    run_log_df = pd.DataFrame(all_run_log_rows)
    combined_registry = add_dependent_variables_to_registry(
        {
            raw_name: meta
            for result in scenario_results.values()
            for raw_name, meta in result.get("variable_registry", {}).items()
        }
    )
    summary_long_df = add_summary_labels(summary_long_df, combined_registry)
    diagnostics_long_df = add_diagnostics_labels(diagnostics_long_df, combined_registry)
    dropped_rows_long_df = add_dropped_row_labels(dropped_rows_long_df, combined_registry)
    compare_main_df = build_stacked_compare_sheet(
        coefficients_long_df=coefficients_long_df,
        summary_long_df=summary_long_df,
        scenario_results=scenario_results,
        config=config,
        variant_suffixes=COMPARE_MAIN_VARIANTS,
    )
    compare_raw_df = build_stacked_compare_sheet(
        coefficients_long_df=coefficients_long_df,
        summary_long_df=summary_long_df,
        scenario_results=scenario_results,
        config=config,
        variant_suffixes=COMPARE_RAW_VARIANTS,
    )
    descriptive_stats_df = build_descriptive_stats(scenario_results, config, models)
    correlation_long_df = build_correlation_long(scenario_results, config, models)
    variable_labels_df = build_variable_labels_table_for_scenarios(scenario_results, coefficients_long_df)

    written_sheets = write_scenario_workbook(
        readme_df=build_readme_sheet(config),
        compare_main_df=compare_main_df,
        compare_raw_df=compare_raw_df,
        descriptive_stats_df=descriptive_stats_df,
        correlation_long_df=correlation_long_df,
        summary_long_df=summary_long_df,
        coefficients_long_df=coefficients_long_df,
        diagnostics_long_df=diagnostics_long_df,
        separator_df=build_separator_sheet(),
        dropped_rows_long_df=dropped_rows_long_df,
        variable_labels_df=variable_labels_df,
        run_log_df=run_log_df,
        config=config,
    )

    models_estimated = sum(result["models_estimated"] for result in scenario_results.values())
    models_skipped = sum(result["models_skipped"] for result in scenario_results.values())
    all_original_comparison = compare_all_with_original_output(summary_long_df, coefficients_long_df)
    print_scenario_validation(
        input_df=input_df,
        summary_long_df=summary_long_df,
        coefficients_long_df=coefficients_long_df,
        diagnostics_long_df=diagnostics_long_df,
        dropped_rows_long_df=dropped_rows_long_df,
        variable_labels_df=variable_labels_df,
        descriptive_stats_df=descriptive_stats_df,
        correlation_long_df=correlation_long_df,
        compare_main_df=compare_main_df,
        compare_raw_df=compare_raw_df,
        run_log_df=run_log_df,
        written_sheets=written_sheets,
        models_estimated=models_estimated,
        models_skipped=models_skipped,
        warnings=all_warnings,
        all_original_comparison=all_original_comparison,
        config=config,
    )
    return {
        "summary_long_df": summary_long_df,
        "coefficients_long_df": coefficients_long_df,
        "diagnostics_long_df": diagnostics_long_df,
        "dropped_rows_long_df": dropped_rows_long_df,
        "variable_labels_df": variable_labels_df,
        "descriptive_stats_df": descriptive_stats_df,
        "correlation_long_df": correlation_long_df,
        "compare_main_df": compare_main_df,
        "compare_raw_df": compare_raw_df,
        "run_log_df": run_log_df,
        "written_sheets": written_sheets,
        "models_estimated": models_estimated,
        "models_skipped": models_skipped,
        "warnings": all_warnings,
        "all_original_comparison": all_original_comparison,
        "config_used": config,
    }


if __name__ == "__main__":
    run_period_ols_scenarios()


# Technical note
#
# CONFIG is now a lightweight researcher cockpit. Most standard numeric
# regressors are added with one line in CONFIG["base_regressors"], while
# metadata dictionaries provide optional labels, interpretations, and special
# rules.
#
# The internal model engine still works with fully expanded dictionaries. The
# normalise_* functions convert the lightweight CONFIG into that internal shape
# before model construction, validation, registry building, and workbook export.
#
# To add a new standard numeric variable, add its base name to
# CONFIG["base_regressors"]. If a better label, interpretation, standardisation
# setting, or column pattern is needed, add an entry to REGRESSOR_METADATA.
#
# To add a new categorical control, add its column name to
# CONFIG["categorical_controls"] and add the required reference, display_prefix,
# and interpretation_template fields to CATEGORICAL_METADATA. The script never
# guesses categorical reference categories.
#
# Standardised beta outputs were removed to reduce duplicated interpretation.
# Compare_Main is the main interpretation table and uses standardised models.
# Compare_Raw is kept as a robustness and technical table. Raw coefficients are
# useful for checking units and robustness, while standardised coefficients are
# easier for comparing effect sizes across regressors.
# Coefficients_All remains the detailed technical coefficient output.
# Dropped_Rows lists the actual companies excluded from each model because of
# missing dependent, regressor, or categorical-control values, supporting sample
# transparency and bias checks.
#
# Interaction capability is controlled entirely by INTERACTION_METADATA. When
# INTERACTION_METADATA is empty or all interaction include flags are False, the
# model results contain no interactions. Interactions are useful for testing
# whether one predictor modifies the effect of another, but main effects should
# be interpreted with extra care once interactions are included. Each
# interaction is entered only once in INTERACTION_METADATA.
