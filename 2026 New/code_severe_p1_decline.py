"""Focused severe-P1-decline supplement; canonical data and main models stay intact."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import textwrap
import warnings

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.special import expit
from scipy.stats import chi2, norm, t

import code_ols_scenarios as ols
from code_config import build_sample_mask, period_dependent_metadata, apply_manual_exclusions, manual_exclusion_readme_rows


CONFIG = {
    "input_file": ols.CONFIG["input_file"],
    # Exact filename specified in the user's supplementary-analysis request.
    "output_file": "Results_severe_P1_decline_analysis.xlsx",
    "samples": ["Rank2019", "Rank2019_Manufacturing"],
    "thresholds": [15, 20, 25],
    "primary_threshold": 20,
    "logit_method": "firth",
    "interaction_regressor": "profit_margin",
    "max_iter": 500,
    "tolerance": 1e-8,
}
SHEETS = [
    "00_README", "01_GROUP_PROFILE", "02_LOGIT_BOTTOMP1",
    "03_LOGIT_MARGINAL_EFFECTS", "04_P2_GROUP_MODEL", "05_P3_GROUP_MODEL",
    "06_SELECTED_INTERACTIONS", "07_THRESHOLD_ROBUSTNESS",
]
METHOD_SOURCE = "https://search.r-project.org/CRAN/refmans/logistf/html/logistf.html"
LR_SOURCE = "https://raw.githubusercontent.com/cran/logistf/master/R/logistftest.R"


def bottom_indicator(growth: pd.Series, threshold: int) -> pd.Series:
    """Strict inequality; missing/nonfinite P1 growth has unknown membership."""
    numeric = pd.to_numeric(growth, errors="coerce").replace([np.inf, -np.inf], np.nan)
    result = pd.Series(pd.NA, index=growth.index, dtype="Int64")
    result.loc[numeric.notna()] = numeric.loc[numeric.notna()].lt(-threshold / 100).astype(int)
    return result


@dataclass
class LogisticFit:
    beta: np.ndarray
    covariance: np.ndarray
    loglik: float
    penalized_loglik: float
    iterations: int
    score_max: float


def firth_state(x: np.ndarray, y: np.ndarray, beta: np.ndarray):
    """Jeffreys penalty and adjusted score, calculated in the full design space."""
    eta = x @ beta
    probability = expit(eta)
    weight = probability * (1 - probability)
    information = x.T @ (weight[:, None] * x)
    sign, logdet = np.linalg.slogdet(information)
    if sign <= 0 or not np.isfinite(logdet):
        raise ValueError("Firth information matrix is singular.")
    covariance = np.linalg.inv(information)
    leverage = weight * np.einsum("ij,jk,ik->i", x, covariance, x)
    score = x.T @ (y - probability + leverage * (0.5 - probability))
    loglik = float(np.sum(y * eta - np.logaddexp(0, eta)))
    return loglik + 0.5 * logdet, score, information, covariance, loglik


def fit_firth(x: np.ndarray, y: np.ndarray, active: list[int] | None = None,
              max_iter: int = 500, tolerance: float = 1e-8) -> LogisticFit:
    """Adjusted-score Fisher scoring with monotone penalised-likelihood steps.

    Restricted tests retain the full design's penalty; inactive slopes are zero.
    This follows the null restriction used by logistftest, not separate penalties
    on differently sized design matrices. Coefficient inference uses Wald CIs.
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if np.linalg.matrix_rank(x) < x.shape[1]:
        raise ValueError("Logistic design is rank deficient; controls were not silently removed.")
    active = list(range(x.shape[1])) if active is None else list(active)
    beta = np.zeros(x.shape[1])
    for iteration in range(1, max_iter + 1):
        objective, score, information, covariance, loglik = firth_state(x, y, beta)
        if np.max(np.abs(score[active])) < tolerance:
            return LogisticFit(beta, covariance, loglik, objective, iteration,
                               float(np.max(np.abs(score[active]))))
        step = np.linalg.solve(information[np.ix_(active, active)], score[active])
        # Cap the change in the linear predictor before backtracking.
        eta_change = np.max(np.abs(x[:, active] @ step))
        step *= min(1.0, 2.0 / max(eta_change, 1e-15))
        accepted = False
        for halves in range(35):
            trial = beta.copy()
            trial[active] += step / 2 ** halves
            trial_objective = firth_state(x, y, trial)[0]
            if trial_objective >= objective - 1e-10:
                beta = trial
                accepted = True
                break
        if not accepted:
            raise ValueError("Firth step-halving failed; no result reported as converged.")
    raise ValueError("Firth adjusted score did not converge within max_iter.")


@dataclass
class Prepared:
    sample: str
    period: str
    estimation: pd.DataFrame
    x: pd.DataFrame
    y: pd.Series
    registry: dict
    scales: dict
    lower: float
    upper: float
    affected: int
    dropped: int


def prepare_model(frame, sample, period, shared, models, levels, registry) -> Prepared:
    spec = models[period]
    estimation = ols.get_estimation_sample(frame, shared, spec)
    if estimation.empty:
        raise ValueError(f"Empty estimation sample: {sample}, {period}")
    x, raw, included, _ = ols.build_design_matrix(
        estimation, spec["regressors"], levels, registry, shared, True,
    )
    scales = {
        name: {"mean": float(raw[name].mean()),
               "sd": float(raw[name].std(ddof=0)) or 1.0,
               "standardised": name in included}
        for name in spec["regressors"]
    }
    y = estimation[spec["dependent"]].astype(float)
    lower = upper = np.nan
    affected = 0
    # The binary logit response is never winsorised or standardised.
    if period in {"P2", "P3"}:
        if shared["winsorise_dependent"]:
            y, lower, upper, affected = ols.winsorize_series(y, shared["winsor_lower"], shared["winsor_upper"])
        if shared["standardise_dependent"]:
            sd = y.std(ddof=0)
            y = (y - y.mean()) / (sd if pd.notna(sd) and sd else 1.0)
    return Prepared(sample, period, estimation, x.astype(float), y, registry, scales,
                    lower, upper, affected, len(frame) - len(estimation))


def effect_statistics(estimate: float, variance: float, df: float | None = None) -> dict:
    if variance < -1e-10 or not np.isfinite(variance):
        raise ValueError("Invalid effect variance.")
    se = float(np.sqrt(max(variance, 0)))
    statistic = estimate / se if se else np.nan
    distribution = norm if df is None else t(df)
    critical = distribution.ppf(0.975)
    return {"estimate": estimate, "std_error": se, "statistic": statistic,
            "p_value": float(2 * distribution.sf(abs(statistic))),
            "CI_lower": estimate - critical * se, "CI_upper": estimate + critical * se}


def continuous_ame(x, beta, covariance, derivative) -> tuple[dict, np.ndarray]:
    """Total derivative, including the chain rule for existing interactions."""
    probability = expit(x @ beta)
    weight = probability * (1 - probability)
    slope = derivative @ beta
    estimate = float(np.mean(weight * slope))
    gradient = np.mean(
        weight[:, None] * derivative
        + (weight * (1 - 2 * probability) * slope)[:, None] * x,
        axis=0,
    )
    return effect_statistics(estimate, float(gradient @ covariance @ gradient)), gradient


def discrete_ame(x0, x1, beta, covariance) -> tuple[dict, np.ndarray]:
    p0, p1 = expit(x0 @ beta), expit(x1 @ beta)
    estimate = float(np.mean(p1 - p0))
    gradient = np.mean((p1 * (1 - p1))[:, None] * x1 - (p0 * (1 - p0))[:, None] * x0, axis=0)
    return effect_statistics(estimate, float(gradient @ covariance @ gradient)), gradient


def marginal_effects(prepared: Prepared, fitted: LogisticFit, shared) -> pd.DataFrame:
    x = prepared.x.to_numpy()
    columns = prepared.x.columns.tolist()
    records = []
    principal = [r["column_pattern"].format(base_name=r["base_name"], period="P1")
                 for r in shared["base_regressors"]]
    if shared["include_lag_growth"] and "P1" in shared["lag_growth_periods"]:
        principal.append(ols.lag_growth_col(shared, "P1"))
    for column in principal:
        derivative = np.zeros_like(x)
        derivative[:, columns.index(column)] = 1
        own_scale = prepared.scales[column]
        delta_raw = own_scale["sd"] if own_scale["standardised"] else 1.0
        for interaction in ols.get_active_interactions():
            sources = [ols.resolve_interaction_variable(shared, name, "P1") for name in interaction["variables"]]
            if column not in sources:
                continue
            product_name = ols.build_interaction_column_name(interaction["name"], "P1")
            product_scale = prepared.scales[product_name]
            divisor = product_scale["sd"] if product_scale["standardised"] else 1.0
            # Product-rule sum also handles a repeated source such as X * X.
            for position, source in enumerate(sources):
                if source == column:
                    other = sources[1 - position]
                    derivative[:, columns.index(product_name)] += prepared.estimation[other].to_numpy(dtype=float) * delta_raw / divisor
        statistics, _ = continuous_ame(x, fitted.beta, fitted.covariance, derivative)
        records.append({"sample": prepared.sample, "variable": column,
                        "label": prepared.registry[column]["display_name"],
                        "change": "Local derivative per 1 SD (not a finite shift)" if own_scale["standardised"] else "Local derivative per raw unit",
                        "effect_type": "Total derivative, interactions included", **statistics})
    if shared["include_owner"]:
        column = shared["owner_variable"]["column"]
        x0, x1 = x.copy(), x.copy()
        x0[:, columns.index(column)] = 0
        x1[:, columns.index(column)] = 1
        for interaction in ols.get_active_interactions():
            sources = [ols.resolve_interaction_variable(shared, name, "P1") for name in interaction["variables"]]
            if column in sources:
                product = ols.build_interaction_column_name(interaction["name"], "P1")
                other = sources[1 - sources.index(column)]
                scale = prepared.scales[product]
                x0[:, columns.index(product)] = (0 - scale["mean"]) / scale["sd"] if scale["standardised"] else 0
                raw1 = prepared.estimation[other].to_numpy(dtype=float)
                x1[:, columns.index(product)] = (raw1 - scale["mean"]) / scale["sd"] if scale["standardised"] else raw1
        statistics, _ = discrete_ame(x0, x1, fitted.beta, fitted.covariance)
        records.append({"sample": prepared.sample, "variable": column,
                        "label": shared["owner_variable"]["display_name"],
                        "change": "Domestic (0) to Foreign (1)", "effect_type": "Average discrete probability difference", **statistics})
    for control in shared["categorical_controls"]:
        dummy_columns = [name for name in columns if name.startswith(control["column"] + "_")]
        for column in dummy_columns:
            x0, x1 = x.copy(), x.copy()
            for dummy in dummy_columns:
                x0[:, columns.index(dummy)] = 0
                x1[:, columns.index(dummy)] = 0
            x1[:, columns.index(column)] = 1
            statistics, _ = discrete_ame(x0, x1, fitted.beta, fitted.covariance)
            records.append({"sample": prepared.sample, "variable": column,
                            "label": prepared.registry[column]["display_name"],
                            "change": f"{control['reference']} to {column[len(control['column']) + 1:]}",
                            "effect_type": "Average discrete probability difference", **statistics})
    table = pd.DataFrame(records).rename(columns={"estimate": "AME", "statistic": "z_stat"})
    table["AME_percentage_points"] = table["AME"] * 100
    return table


def fit_logistic(prepared, frame, threshold, shared, config):
    name = f"BottomP1_{threshold}"
    y = frame.loc[prepared.x.index, name].to_numpy(dtype=float)
    x = prepared.x.to_numpy()
    if np.unique(y).size != 2:
        raise ValueError(f"Both logit outcome classes required: {prepared.sample}, {name}")
    if config["logit_method"] == "firth":
        fitted = fit_firth(x, y, max_iter=config["max_iter"], tolerance=config["tolerance"])
        null = fit_firth(x, y, active=[prepared.x.columns.get_loc("const")],
                         max_iter=config["max_iter"], tolerance=config["tolerance"])
        lr = 2 * (fitted.penalized_loglik - null.penalized_loglik)
        method = "Firth bias-reduced logistic; Wald coefficient inference"
        lr_method = "Penalised LR: all slopes = 0, full-design Jeffreys penalty"
    elif config["logit_method"] == "ordinary":
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = sm.Logit(y, prepared.x).fit(disp=False, maxiter=config["max_iter"])
        if not result.mle_retvals.get("converged") or caught:
            raise ValueError(f"Ordinary logit was not reliable: {[str(w.message) for w in caught]}")
        fitted = LogisticFit(result.params.to_numpy(), result.cov_params().to_numpy(),
                             float(result.llf), float(result.llf), int(result.mle_retvals.get("iterations", 0)), np.nan)
        lr = float(result.llr)
        method, lr_method = "Ordinary maximum-likelihood logistic", "Ordinary LR"
    else:
        raise ValueError("logit_method must be firth or ordinary.")
    mean = y.mean()
    ll_null_ml = float(np.sum(y * np.log(mean) + (1 - y) * np.log1p(-mean)))
    coefficients = []
    for position, variable in enumerate(prepared.x.columns):
        statistics = effect_statistics(float(fitted.beta[position]), float(fitted.covariance[position, position]))
        with np.errstate(over="ignore"):
            odds = np.exp([statistics["estimate"], statistics["CI_lower"], statistics["CI_upper"]])
        coefficients.append({"sample": prepared.sample, "variable": variable,
                             "label": prepared.registry[variable]["display_name"],
                             "scale": "Within-model-sample z-score" if variable in prepared.scales and prepared.scales[variable]["standardised"] else "Intercept or unstandardised dummy",
                             **statistics, "odds_ratio": odds[0], "OR_CI_lower": odds[1], "OR_CI_upper": odds[2]})
    coefficient_table = pd.DataFrame(coefficients).rename(columns={"estimate": "coefficient", "statistic": "z_stat"})
    pure_sectors = []
    for control in shared["categorical_controls"]:
        outcomes = pd.Series(y, index=prepared.estimation.index).groupby(prepared.estimation[control["column"]]).nunique()
        pure_sectors.extend(outcomes[outcomes.lt(2)].index.astype(str).tolist())
    summary = {
        "sample": prepared.sample, "threshold": threshold, "N": len(y), "bottom_N": int(y.sum()),
        "bottom_share": float(y.mean()), "rows_dropped": prepared.dropped,
        "pseudo_R2": 1 - fitted.loglik / ll_null_ml, "LR_stat": lr,
        "LR_df": x.shape[1] - 1, "LR_p_value": float(chi2.sf(lr, x.shape[1] - 1)),
        "AIC_plug_in": -2 * fitted.loglik + 2 * x.shape[1],
        "BIC_plug_in": -2 * fitted.loglik + np.log(len(y)) * x.shape[1],
        "iterations": fitted.iterations, "adjusted_score_max": fitted.score_max,
        "method": method, "LR_method": lr_method,
        "single_class_sectors": "; ".join(pure_sectors) or "None",
    }
    return {"summary": summary, "coefficients": coefficient_table,
            "marginal_effects": marginal_effects(prepared, fitted, shared), "fit": fitted}


def fit_growth(prepared, frame, threshold, shared, profitability, interaction=False):
    name = f"BottomP1_{threshold}"
    x = prepared.x.copy()
    x[name] = frame.loc[x.index, name].astype(float)
    interaction_name = f"profitability_z_x_{name}"
    if interaction:
        x[interaction_name] = x[profitability] * x[name]
    fitted = ols.fit_ols(prepared.y, x, shared)
    if fitted.df_resid <= 0:
        raise ValueError("Insufficient residual degrees of freedom.")
    coefficients = []
    for variable in x.columns:
        label = name if variable == name else (
            f"Standardised profitability × {name}" if variable == interaction_name
            else prepared.registry[variable]["display_name"]
        )
        coefficients.append({"sample": prepared.sample, "period": prepared.period,
                             "threshold": threshold, "variable": variable, "label": label,
                             "scale": "z-score × fixed 0/1 indicator; product not restandardised" if variable == interaction_name else
                                      "Fixed 0/1 indicator" if variable == name else
                                      "Within-model-sample z-score" if variable in prepared.scales and prepared.scales[variable]["standardised"] else
                                      "Intercept or unstandardised dummy",
                             **effect_statistics(float(fitted.params[variable]), float(fitted.cov_params().loc[variable, variable]), fitted.df_resid)})
    coefficient_table = pd.DataFrame(coefficients).rename(columns={"estimate": "coefficient", "statistic": "t_stat"})
    summary = {
        "sample": prepared.sample, "period": prepared.period, "threshold": threshold,
        "N": int(fitted.nobs), "bottom_N": int(x[name].sum()),
        "rows_dropped": prepared.dropped, "R2": float(fitted.rsquared),
        "adjusted_R2": float(fitted.rsquared_adj), "F_stat": float(fitted.fvalue),
        "F_p_value": float(fitted.f_pvalue), "covariance": shared["covariance_type"],
        "winsor_lower_bound": prepared.lower, "winsor_upper_bound": prepared.upper,
        "winsor_affected_N": prepared.affected,
        "outcome": period_dependent_metadata(shared["growth_mode"], prepared.period)["column"],
        "treatment": "winsor_std", "model": "Profitability interaction" if interaction else "Group indicator",
    }
    slopes = []
    if interaction:
        for group, variables in [
            ("Other firms", [profitability]),
            (name, [profitability, interaction_name]),
            ("Difference between slopes", [interaction_name]),
        ]:
            contrast = pd.Series(0.0, index=x.columns)
            contrast.loc[variables] = 1
            estimate = float(contrast @ fitted.params)
            variance = float(contrast @ fitted.cov_params() @ contrast)
            slopes.append({"sample": prepared.sample, "period": prepared.period,
                           "threshold": threshold, "group": group,
                           **effect_statistics(estimate, variance, fitted.df_resid)})
    return {"summary": summary, "coefficients": coefficient_table,
            "slopes": pd.DataFrame(slopes).rename(columns={"statistic": "t_stat"}), "fit": fitted}


def build_profiles(frames, shared, models, primary):
    counts, continuous, categorical = [], [], []
    prefix = ols.get_growth_prefix(shared)
    descriptions = {f"{prefix}growth_{period}": (f"{shared['growth_mode'].title()} simple sales growth {period}", "rate")
                    for period in ["P1", "P2", "P3"]}
    descriptions.update({models[p]["dependent"]: (period_dependent_metadata(shared["growth_mode"], p)["label"], "log points/year") for p in ["P1", "P2", "P3"]})
    for regressor in shared["base_regressors"]:
        name = ols.render_regressor_column(regressor, "P1")
        unit = "rate" if regressor["base_name"] in {"profit_margin", "export_ratio", "capital_ratio"} else "raw units"
        descriptions[name] = (regressor["display_name"] + " (2019)", unit)
    if shared["include_lag_growth"] and "P1" in shared["lag_growth_periods"]:
        descriptions[ols.lag_growth_col(shared, "P1")] = ("Pre-P1 annualised log growth, 2018-2019", "log points/year")
    for sample, frame in frames.items():
        groups = {1: frame[frame[f"BottomP1_{primary}"].eq(1)], 0: frame[frame[f"BottomP1_{primary}"].eq(0)]}
        for indicator, group in groups.items():
            counts.append({"sample": sample, "group": f"BottomP1_{primary} = {indicator}",
                           "firms": len(group), "sample_share": len(group) / len(frame)})
        for variable, (label, unit) in descriptions.items():
            values = [pd.to_numeric(groups[i][variable], errors="coerce").dropna() for i in [1, 0]]
            a, b = values
            continuous.append({"sample": sample, "variable": variable, "label": label, "unit": unit,
                               "bottom_N": len(a), "other_N": len(b),
                               "bottom_mean": a.mean(), "other_mean": b.mean(), "mean_difference": a.mean() - b.mean(),
                               "bottom_median": a.median(), "other_median": b.median(),
                               "bottom_sd": a.std(ddof=1), "other_sd": b.std(ddof=1)})
        controls = list(dict.fromkeys([shared["owner_column"], *ols.get_categorical_columns(shared), "manufacturing"]))
        for variable in controls:
            for category in sorted(frame[variable].dropna().unique(), key=str):
                ns = [int(groups[i][variable].eq(category).sum()) for i in [1, 0]]
                categorical.append({"sample": sample, "control": variable, "category": str(category),
                                    "bottom_count": ns[0], "other_count": ns[1],
                                    "bottom_share": ns[0] / len(groups[1]), "other_share": ns[1] / len(groups[0])})
            missing = [int(groups[i][variable].isna().sum()) for i in [1, 0]]
            if any(missing):
                categorical.append({"sample": sample, "control": variable, "category": "Missing",
                                    "bottom_count": missing[0], "other_count": missing[1],
                                    "bottom_share": missing[0] / len(groups[1]), "other_share": missing[1] / len(groups[0])})
    return pd.DataFrame(counts), pd.DataFrame(continuous), pd.DataFrame(categorical)


def readme(config, shared, status):
    mode = shared["growth_mode"]
    rows = [
        ("Purpose", "Supplementary P1 vulnerability → P2 recovery → P3 subsequent development analysis. Associations are descriptive, not causal."),
        ("Input file", config["input_file"]), ("Output file", config["output_file"]),
        *manual_exclusion_readme_rows(config.get("manual_exclusion_audit", [])),
        ("Samples", "RANK2019 (primary) and RANK2019_MANUFACTURING (secondary). Internal shared masks are Rank2019 and Rank2019_Manufacturing. Manufacturing is nested within ranking; these are not independent replications."),
        ("Price basis", f"{mode.title()} sales growth. All model outcomes and lags follow code_config.PERIOD_MODEL_SETTINGS."),
        ("Primary definition", f"BottomP1_20 = 1 only when {ols.get_growth_prefix(shared)}growth_P1 < -0.20; equality is outside the severe group. Membership is fixed by P1."),
        ("Robustness", "Repeat the logit, P2/P3 group models, and profitability interactions separately at -15%, -20%, -25%. Never include multiple threshold indicators in one model."),
        ("Sample and missingness", "Apply the same ranking/manufacturing and complete-trajectory masks as main OLS, then the same period-specific complete-case exclusions. Missing P1 growth has unknown membership, never zero."),
        ("Group profile", "Raw simple P1/P2/P3 growth and 2019 starting covariates; N, mean, median, sample SD. Profile shares use the selected complete-trajectory sample, not the smaller regression samples."),
        ("Logit outcome", "BottomP1_20 (0/1), neither standardised nor winsorised. P1=2019-2020 simple growth determines membership; log-growth columns are not compared with percentage thresholds."),
        ("Logit predictors", ", ".join(models_for_readme(shared)["P1"]["regressors"]) + "; sector controls."),
        ("Timing", "P1 start covariates are from 2019; P1 lag is 2018-2019. No P1 realised outcome, P2/P3 predictor or trajectory label enters the logit. Ownership/sector use the established stable descriptors, treated as predetermined."),
        ("Logit estimator", "Firth bias-reduced logistic regression for both selected samples and all thresholds; full firm samples and sector controls retained. Manufacturing health/pharma has zero severe cases at all thresholds." if config["logit_method"] == "firth" else "Ordinary maximum-likelihood logit; non-estimable manufacturing fits are explicitly flagged."),
        ("Logit inference", "Wald coefficient z tests and 95% intervals using inverse expected Fisher information; odds-ratio intervals exponentiate coefficient intervals. They are not profile-penalised intervals."),
        ("Model statistics", "McFadden pseudo-R² uses unpenalised log-likelihood at the fitted coefficients versus intercept-only ML. LR is penalised for Firth and constrains all slopes to zero using the same full-design Jeffreys penalty."),
        ("AIC/BIC", "AIC_plug_in and BIC_plug_in evaluate the unpenalised likelihood at bias-reduced estimates. They are descriptive, not conventional MLE model-selection criteria. Do not compare them across thresholds with different responses."),
        ("Average marginal effects", "Preferred probability-scale interpretation. Continuous AMEs are average total derivatives per one SD, including the chain rule for export_ratio × ln_sales. Binary ownership and sector effects are average discrete probability differences; delta-method Wald intervals."),
        ("P2 outcome", period_dependent_metadata(mode, "P2")["label"] + "; " + period_dependent_metadata(mode, "P2")["formula"]),
        ("P3 outcome", period_dependent_metadata(mode, "P3")["label"] + "; " + period_dependent_metadata(mode, "P3")["formula"]),
        ("Growth preprocessing", f"Use principal OLS winsor_std: clip only the outcome at {shared['winsor_lower']:.0%}/{shared['winsor_upper']:.0%}, then standardise it and metadata-designated continuous predictors within the same period estimation sample (ddof=0). Group indicators remain 0/1."),
        ("Growth controls", "Same period starting covariates, ownership, export_ratio × ln_sales, sector controls and lag growth as main OLS. P2's group coefficient is conditional on P1 growth through its lag; it is an incremental threshold association."),
        ("Selected interaction", "Only standardised profit_margin × BottomP1, estimated separately for P2 and P3. Do not restandardise this product. The ordinary-firm slope is β1, the severe-group slope β1+β3, and the slope difference β3. The group main effect is evaluated at mean profitability."),
        ("Interaction audit", "Both profitability main effect and interaction use exactly the same z-score; combined inference uses Var(β1)+Var(β3)+2Cov(β1,β3). No scaling mismatch was found."),
        ("Influence warning", "The manual-exclusion audit above records the status of earlier dominant firms Kania and Globus. Recomputed VIF, subgroup-variation and influence diagnostics describe the current samples. Remaining influential observations can still affect inference; these exclusions alone do not establish robustness."),
        ("Raw-growth mean warning", "Descriptive growth is raw and can be affected by extreme observations. Compare means with the appended medians, 5th/95th percentiles, maxima and mean-without-largest diagnostics."),
        ("P2 lag sensitivity", "An additional -20% diagnostic removes the continuous P1-growth lag on exactly the same sample and outcome scaling. It does not replace the primary model. Both versions remain conditional associations, not causal recovery effects."),
        ("Estimator comparison", "Ordinary MLE works without separation for Rank2019 at all thresholds. Firth was deliberately used in both samples for consistency and is retained. The principal ranking MLE comparison is diagnostic only; manufacturing requires separation handling."),
        ("Firth implementation", "Project-local NumPy/SciPy adjusted-score solver, not execution of the R logistf package. Coefficients independently reproduced by BFGS; covariance is inverse original expected Fisher information. Wald/delta intervals are approximate, particularly for sparse or separated cells."),
        ("FULL", "No FULL model: group definition mechanically contains part of FULL growth."),
        ("Quantile distinction", "Fixed realised-P1 firm groups followed over time; no conditional-quantile models are estimated."),
        ("Interpretation cautions", "Complete-trajectory selection excludes firms without later outcomes. Subsequent covariates and lags may mediate earlier decline; group comparisons and interactions are conditional associations, not causal recovery effects. Firth reduces first-order coefficient bias; it does not guarantee every fitted probability moves toward one half."),
        ("Model status", status),
        ("Method source", METHOD_SOURCE), ("Penalised LR reference", LR_SOURCE),
        ("Reproduce", "python3 code_severe_p1_decline.py; shared settings remain in code_config.py. Canonical datasets and other workbooks are not modified."),
    ]
    return pd.DataFrame(rows, columns=["item", "description"])


def models_for_readme(shared):
    return ols.build_models(shared)


def concat(frames):
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def write_workbook(path, sections):
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        book = writer.book
        colours = {"00_README": "#1F4E78", "01_GROUP_PROFILE": "#548235",
                   "02_LOGIT_BOTTOMP1": "#1F4E78", "03_LOGIT_MARGINAL_EFFECTS": "#1F4E78",
                   "04_P2_GROUP_MODEL": "#1F4E78", "05_P3_GROUP_MODEL": "#1F4E78",
                   "06_SELECTED_INTERACTIONS": "#7030A0", "07_THRESHOLD_ROBUSTNESS": "#C65911"}
        body = book.add_format({"font_name": "Arial", "font_size": 10, "valign": "vcenter"})
        decimal = book.add_format({"font_name": "Arial", "font_size": 10, "num_format": "0.0000", "valign": "vcenter"})
        integer = book.add_format({"font_name": "Arial", "font_size": 10, "num_format": "#,##0", "valign": "vcenter"})
        percent = book.add_format({"font_name": "Arial", "font_size": 10, "num_format": "0.00%", "valign": "vcenter"})
        pformat = book.add_format({"font_name": "Arial", "font_size": 10, "num_format": '[<0.0001]"<0.0001";0.0000', "valign": "vcenter"})
        wrap = book.add_format({"font_name": "Arial", "font_size": 10, "text_wrap": True, "valign": "top"})
        table_index = 0
        for sheet_name in SHEETS:
            worksheet = book.add_worksheet(sheet_name)
            writer.sheets[sheet_name] = worksheet
            worksheet.hide_gridlines(2)
            worksheet.set_tab_color(colours[sheet_name])
            worksheet.set_default_row(18)
            worksheet.set_zoom(90)
            sheet_widths = {}
            worksheet.set_column(0, 0, 30, body)
            worksheet.set_column(1, 24, 18, decimal)
            header = book.add_format({"font_name": "Arial", "font_size": 10, "bold": True,
                                      "bg_color": colours[sheet_name], "font_color": "white",
                                      "text_wrap": True, "valign": "vcenter", "align": "center"})
            title = book.add_format({"font_name": "Arial", "font_size": 13, "bold": True})
            section_format = book.add_format({"font_name": "Arial", "font_size": 11, "bold": True})
            worksheet.write(1, 0, sheet_name[3:].replace("_", " ").title(), title)
            worksheet.write(2, 0, f"{ols.CONFIG['growth_mode'].title()} growth | RANK2019 and RANK2019_MANUFACTURING | Primary threshold -20%", body)
            row = 4
            for label, frame in sections[sheet_name]:
                if frame.empty:
                    continue
                frame = frame.copy()
                if "sample" in frame:
                    frame["sample"] = frame["sample"].replace({"Rank2019": "RANK2019", "Rank2019_Manufacturing": "RANK2019_MANUFACTURING"})
                worksheet.write(row, 0, label, section_format)
                startrow = row + 1
                frame.to_excel(writer, sheet_name=sheet_name, startrow=startrow, index=False)
                table_index += 1
                worksheet.add_table(startrow, 0, startrow + len(frame), len(frame.columns) - 1,
                                    {"name": f"SevereP1Table{table_index}", "style": "Table Style Light 9",
                                     "columns": [{"header": str(column), "header_format": header} for column in frame.columns]})
                worksheet.set_row(startrow, 34)
                row_heights = {}
                for col_idx, column in enumerate(frame.columns):
                    numeric = pd.api.types.is_numeric_dtype(frame[column])
                    number_format = integer if column.endswith("_N") or column in {"N", "N_total", "firms", "bottom_count", "other_count", "threshold", "rows_dropped", "iterations", "LR_df"} else (
                        percent if column.endswith("share") else pformat if column == "p_value" or column.endswith("p_value") else decimal)
                    if numeric:
                        width = min(max(len(column) + 2, 16), 24)
                    else:
                        width = 85 if column == "description" else 44 if column in {"label", "change", "effect_type", "method", "LR_method", "single_class_sectors"} else 36 if column in {"variable", "control", "outcome", "category"} else 30
                    sheet_widths[col_idx] = max(width, sheet_widths.get(col_idx, 0))
                    worksheet.set_column(col_idx, col_idx, sheet_widths[col_idx], body)
                    for local_idx, value in enumerate(frame[column]):
                        cell_row = startrow + 1 + local_idx
                        if pd.isna(value):
                            worksheet.write_blank(cell_row, col_idx, None, number_format if numeric else wrap)
                        elif numeric:
                            if not np.isfinite(float(value)):
                                raise ValueError(f"Nonfinite workbook result: {sheet_name}, {column}")
                            worksheet.write_number(cell_row, col_idx, float(value), number_format)
                        else:
                            worksheet.write_string(cell_row, col_idx, str(value), wrap)
                    if not numeric:
                        for local_idx, value in enumerate(frame[column]):
                            lines = len(textwrap.wrap(str(value), width=width - 4)) or 1
                            row_heights[local_idx] = max(row_heights.get(local_idx, 18), lines * 14 + 4)
                for local_idx, height in row_heights.items():
                    worksheet.set_row(startrow + 1 + local_idx, height)
                if "unit" in frame.columns:
                    # The mixed-unit descriptive table uses explicit per-row formats.
                    values = [c for c in frame if c.endswith(("mean", "median", "sd")) or c in {"mean_difference", "p05", "p95", "maximum", "mean_without_largest"}]
                    for local_idx, record in frame.iterrows():
                        if record["unit"] == "rate":
                            for column in values:
                                value = record[column]
                                if pd.notna(value):
                                    worksheet.write_number(startrow + 1 + local_idx, frame.columns.get_loc(column), float(value), percent)
                row = startrow + len(frame) + 3
            worksheet.freeze_panes(6, 1)
    with pd.ExcelFile(path) as saved:
        if saved.sheet_names != SHEETS:
            raise ValueError("Workbook sheet order differs from the specification.")


def run_analysis(config=None):
    config = {**CONFIG, **(config or {})}
    shared = ols.normalise_config(ols.CONFIG)
    if not shared["standardised_models"] or not shared["standardise_dependent"] or not shared["winsorise_dependent"]:
        raise ValueError("This supplement requires the current principal winsor_std OLS settings.")
    source = Path(config["input_file"])
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    data = pd.read_parquet(source)
    if data.nip.isna().any() or data.nip.duplicated().any():
        raise ValueError("Period data must have non-missing, unique nip identifiers.")
    data, config["manual_exclusion_audit"] = apply_manual_exclusions(data)
    prefix = ols.get_growth_prefix(shared)
    p1_growth = f"{prefix}growth_P1"
    models = ols.build_models(shared)
    if config["interaction_regressor"] != "profit_margin":
        raise ValueError("Only the explicitly requested profitability interaction is supported.")
    data = ols.add_interaction_columns(data, shared, models)
    for threshold in config["thresholds"]:
        data[f"BottomP1_{threshold}"] = bottom_indicator(data[p1_growth], threshold)
    frames, prepared, logits, growths, interactions, failures = {}, {}, {}, {}, {}, []
    for sample in config["samples"]:
        frame = data.loc[build_sample_mask(data, sample, ols.trajectory_col(shared))].copy()
        if frame[[f"BottomP1_{t}" for t in config["thresholds"]]].isna().any().any():
            raise ValueError("Complete-trajectory sample unexpectedly has missing P1 membership.")
        frames[sample] = frame
        levels = ols.get_categorical_levels(frame, shared)
        registry = ols.build_variable_registry(shared, models, levels)
        for period in ["P1", "P2", "P3"]:
            prepared[sample, period] = prepare_model(frame, sample, period, shared, models, levels, registry)
        for threshold in config["thresholds"]:
            try:
                logits[sample, threshold] = fit_logistic(prepared[sample, "P1"], frame, threshold, shared, config)
            except Exception as exc:
                failures.append({"sample": sample, "threshold": threshold, "model": "Logit", "status": str(exc)})
                if config["logit_method"] == "firth":
                    raise
            for period in ["P2", "P3"]:
                profitability = ols.render_regressor_column(next(r for r in shared["base_regressors"] if r["base_name"] == config["interaction_regressor"]), period)
                growths[sample, threshold, period] = fit_growth(prepared[sample, period], frame, threshold, shared, profitability)
                interactions[sample, threshold, period] = fit_growth(prepared[sample, period], frame, threshold, shared, profitability, True)
    primary = config["primary_threshold"]
    profile = build_profiles(frames, shared, models, primary)
    primary_logits = [result for (sample, threshold), result in logits.items() if threshold == primary]
    primary_interactions = [result for (sample, threshold, period), result in interactions.items() if threshold == primary]
    threshold_counts, logit_robust, group_robust, interaction_robust = [], [], [], []
    for sample, frame in frames.items():
        for threshold in config["thresholds"]:
            name = f"BottomP1_{threshold}"
            n_bottom = int(frame[name].sum())
            threshold_counts.append({"sample": sample, "indicator": name, "threshold": threshold,
                                     "sample_N": len(frame), "bottom_N": n_bottom, "bottom_share": n_bottom / len(frame)})
            if (sample, threshold) in logits:
                result = logits[sample, threshold]
                selected = result["marginal_effects"][result["marginal_effects"].variable.isin(["ln_sales_start_P1", "profit_margin_start_P1", "capital_ratio_start_P1", shared["owner_column"]])]
                for _, record in selected.iterrows():
                    coefficient = result["coefficients"].set_index("variable").loc[record["variable"]]
                    logit_robust.append({"sample": sample, "threshold": threshold, "N": result["summary"]["N"],
                                         "variable": record["variable"], "AME": record["AME"],
                                         "p_value": record["p_value"], "CI_lower": record["CI_lower"], "CI_upper": record["CI_upper"],
                                         "odds_ratio_component": coefficient["odds_ratio"]})
            for period in ["P2", "P3"]:
                result = growths[sample, threshold, period]
                coefficient = result["coefficients"].set_index("variable").loc[name].to_dict()
                group_robust.append({"N": result["summary"]["N"], **coefficient})
                result = interactions[sample, threshold, period]
                for _, slope in result["slopes"].iterrows():
                    if slope["group"] in {name, "Difference between slopes"}:
                        interaction_robust.append({"N": result["summary"]["N"], **slope.to_dict()})
    status = f"{len(logits)} logistic, {len(growths)} group OLS, {len(interactions)} interaction OLS primary/threshold models completed. {len(failures)} explicitly flagged failures. Audit diagnostics additionally compare one ranking MLE fit and two P2 fits without lag growth."
    sections = {
        "00_README": [("Specification and interpretation", readme(config, shared, status))],
        "01_GROUP_PROFILE": [("Group counts and sample shares", profile[0]), ("Continuous variables: severe group minus other firms", profile[1]), ("Control composition: within-group shares", profile[2])],
        "02_LOGIT_BOTTOMP1": [("Primary -20% model statistics", pd.DataFrame([r["summary"] for r in primary_logits])), ("Coefficients and odds ratios: approximate Wald inference", concat([r["coefficients"] for r in primary_logits]))],
        "03_LOGIT_MARGINAL_EFFECTS": [("Primary -20% average marginal effects: probability units", concat([r["marginal_effects"] for r in primary_logits]))],
        "06_SELECTED_INTERACTIONS": [("Primary profitability-interaction model statistics", pd.DataFrame([r["summary"] for r in primary_interactions])), ("Profitability slopes and differences", concat([r["slopes"] for r in primary_interactions])), ("Full interaction-model coefficients", concat([r["coefficients"] for r in primary_interactions]))],
        "07_THRESHOLD_ROBUSTNESS": [("Membership counts: strict -15%, -20%, -25% thresholds", pd.DataFrame(threshold_counts)), ("Selected logistic marginal effects", pd.DataFrame(logit_robust)), ("P2 and P3 fixed-group coefficients", pd.DataFrame(group_robust)), ("Profitability slope difference and severe-group slope", pd.DataFrame(interaction_robust))],
    }
    for period in ["P2", "P3"]:
        results = [r for (sample, threshold, p), r in growths.items() if threshold == primary and p == period]
        sheet = "04_P2_GROUP_MODEL" if period == "P2" else "05_P3_GROUP_MODEL"
        sections[sheet] = [("Primary -20% growth-model statistics", pd.DataFrame([r["summary"] for r in results])), ("Standardised coefficients; BottomP1 remains 0/1", concat([r["coefficients"] for r in results]))]
    if failures:
        sections["02_LOGIT_BOTTOMP1"].append(("Explicitly non-estimable models", pd.DataFrame(failures)))
        sections["07_THRESHOLD_ROBUSTNESS"].append(("Explicitly non-estimable models", pd.DataFrame(failures)))
    # Requested audit diagnostics extend existing tabs; primary fits are retained.
    from code_audit_severe_p1_decline import audit_models
    model_result = {"prepared": prepared, "frames": frames, "logits": logits,
                    "growths": growths, "interactions": interactions,
                    "config": config, "shared": shared}
    audit = audit_models(model_result)
    sections["01_GROUP_PROFILE"].append(("Raw-growth tail audit: means unchanged", audit["raw_growth_extremes"]))
    sections["02_LOGIT_BOTTOMP1"].append(("Separation audit for all threshold models", audit["separation"]))
    core_comparison = audit["rank_MLE_Firth"].loc[audit["rank_MLE_Firth"].variable.isin(
        [*models["P1"]["regressors"]])]
    sections["02_LOGIT_BOTTOMP1"].append(("Ranking principal MLE versus Firth: diagnostic only", core_comparison))
    sections["02_LOGIT_BOTTOMP1"].append(("Manufacturing sector counts before complete cases", audit["sector_counts"].query("sample == 'Rank2019_Manufacturing'")))
    sections["04_P2_GROUP_MODEL"].append(("Sensitivity: remove continuous P1-growth lag, retain same sample", audit["P2_lag_sensitivity"]))
    concise_influence = audit["influence"][["sample", "period", "threshold", "VIF_profitability",
                                           "VIF_profitability_interaction", "profit_SD_z_other",
                                           "profit_SD_z_bottom", "outlier_nip", "outlier_profit_SS_share",
                                           "outlier_leverage", "outlier_Cooks_D"]]
    sections["06_SELECTED_INTERACTIONS"].append(("Multicollinearity and influence: do not infer robust slopes from significance alone", concise_influence))
    write_workbook(Path(config["output_file"]), sections)
    if hashlib.sha256(source.read_bytes()).hexdigest() != source_hash:
        raise ValueError("Canonical input was unexpectedly changed.")
    print("Severe P1 supplementary analysis complete")
    print(f"input_row_count: {len(data)}; unique_firms: {data.nip.nunique()}; duplicate_nip: {data.nip.duplicated().sum()}")
    print(f"growth_mode: {shared['growth_mode']}; logit_method: {config['logit_method']}")
    print(status)
    print(pd.DataFrame(threshold_counts).to_string(index=False))
    print("Model-specific estimation sample sizes:")
    for key, result in prepared.items():
        print(f"{key}: N={len(result.estimation)}; rows_dropped={result.dropped}")
    print("Missing counts in main outcome/covariate blocks:")
    for sample, frame in frames.items():
        columns = list(dict.fromkeys([p1_growth, *[models[p]['dependent'] for p in ['P1', 'P2', 'P3']], *[name for p in ['P1', 'P2', 'P3'] for name in models[p]['regressors']]]))
        print(sample, frame[columns].isna().sum().to_dict())
    print("Primary logit adjusted-score convergence:")
    print(pd.DataFrame([r['summary'] for r in primary_logits])[["sample", "N", "bottom_N", "iterations", "adjusted_score_max", "single_class_sectors"]].to_string(index=False))
    print("Primary group effects:")
    print(pd.DataFrame(group_robust).query("threshold == 20")[["sample", "period", "N", "coefficient", "p_value"]].to_string(index=False))
    print("Required sheets:", SHEETS)
    print("All threshold indicators built in analysis memory only; canonical input unchanged.")
    return {"sections": sections, "prepared": prepared, "frames": frames, "logits": logits,
            "growths": growths, "interactions": interactions, "config": config, "shared": shared,
            "audit": audit}


if __name__ == "__main__":
    run_analysis()
