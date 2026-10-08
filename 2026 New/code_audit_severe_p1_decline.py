"""Numerical audit diagnostics; does not alter the supplementary primary models."""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import linprog

import code_ols_scenarios as ols


def coefficient(fit, name):
    interval = fit.conf_int().loc[name]
    return {"coefficient": float(fit.params[name]), "SE": float(fit.bse[name]),
            "p_value": float(fit.pvalues[name]), "CI_lower": float(interval.iloc[0]),
            "CI_upper": float(interval.iloc[1])}


def audit_models(result):
    """Evaluate all original models; add requested diagnostics without replacing them."""
    from code_severe_p1_decline import fit_logistic

    shared = result["shared"]
    models = ols.build_models(shared)
    counts, sizes, scaling, sectors, references = [], [], [], [], []
    separation, units, vifs, variation, influence, extreme = [], [], [], [], [], []
    group_robust, interaction_robust, lag_diagnostic = [], [], []
    mle_compare = []
    for sample, frame in result["frames"].items():
        for threshold in [15, 20, 25]:
            name = f"BottomP1_{threshold}"
            counts.append({"sample": sample, "threshold": -threshold / 100,
                           "total_N": len(frame), "bottom_N": int(frame[name].sum()),
                           "bottom_share": float(frame[name].mean())})
        for sector, part in frame.groupby("sector_en", observed=True):
            sectors.append({"sample": sample, "sector": sector, "N_total": len(part),
                            **{f"Bottom{q}_N": int(part[f"BottomP1_{q}"].sum()) for q in [15, 20, 25]}})
        for period in ["P1", "P2", "P3"]:
            prepared = result["prepared"][sample, period]
            estimation = prepared.estimation
            sizes.append({"sample": sample, "period": period, "N_entering": len(frame),
                          "complete_case_N": len(estimation), "dropped": prepared.dropped,
                          **{f"Bottom{q}_N": int(frame.loc[estimation.index, f"BottomP1_{q}"].sum()) for q in [15, 20, 25]}})
            for variable, scale in prepared.scales.items():
                scaling.append({"sample": sample, "period": period, "raw_variable": variable,
                                "model_column": variable, "winsorised_variable": "None: predictor not winsorised",
                                "standardised_symbol": f"z({variable})" if scale["standardised"] else variable,
                                "mean_raw": scale["mean"], "SD_raw_ddof0": estimation[variable].std(ddof=0),
                                "SD_divisor": scale["sd"] if scale["standardised"] else 1.0,
                                "N_scaling": len(estimation),
                                "model_mean": prepared.x[variable].mean(),
                                "model_SD_ddof0": prepared.x[variable].std(ddof=0)})
            if period != "P1":
                raw_y = estimation[models[period]["dependent"]].astype(float)
                clipped_y = raw_y.clip(prepared.lower, prepared.upper)
                scaling.append({"sample": sample, "period": period,
                                "raw_variable": models[period]["dependent"],
                                "model_column": "dependent growth response",
                                "winsorised_variable": f"winsor({models[period]['dependent']}, .01, .99)",
                                "standardised_symbol": f"z(winsor({models[period]['dependent']}))",
                                "mean_raw": clipped_y.mean(), "SD_raw_ddof0": clipped_y.std(ddof=0),
                                "SD_divisor": clipped_y.std(ddof=0), "N_scaling": len(estimation),
                                "model_mean": prepared.y.mean(), "model_SD_ddof0": prepared.y.std(ddof=0)})
            observed = sorted(frame.sector_en.dropna().astype(str).unique())
            present = sorted(estimation.sector_en.astype(str).unique())
            references.append({"sample": sample, "period": period, "reference": "production",
                               "included_sectors": "; ".join(s for s in observed if s != "production"),
                               "sectors_absent_from_estimation": "; ".join(sorted(set(observed) - set(present))) or "None"})
        prepared = result["prepared"][sample, "P1"]
        matrix = prepared.x.to_numpy()
        for threshold in [15, 20, 25]:
            y = frame.loc[prepared.x.index, f"BottomP1_{threshold}"].to_numpy(dtype=float)
            signed = (2 * y - 1)[:, None] * matrix
            complete = linprog(np.zeros(matrix.shape[1]), A_ub=-signed, b_ub=-np.ones(len(y)),
                               bounds=[(None, None)] * matrix.shape[1], method="highs")
            weak = linprog(-signed.sum(axis=0), A_ub=-signed, b_ub=np.zeros(len(y)),
                           bounds=[(-1, 1)] * matrix.shape[1], method="highs")
            if not weak.success:
                raise ValueError("Separation LP did not solve; separation status is unverified.")
            status = "Complete" if complete.success else "Quasi-complete" if -weak.fun > 1e-6 else "None"
            groups = pd.Series(y, index=prepared.x.index).groupby(prepared.estimation.sector_en)
            pure = [f"{sector} (N={len(values)}, severe={int(values.sum())})"
                    for sector, values in groups if values.nunique() == 1]
            separation.append({"sample": sample, "threshold": threshold, "N": len(y),
                               "bottom_N": int(y.sum()), "separation": status,
                               "single_class_sectors": "; ".join(pure) or "None",
                               "estimator_used": "Firth (primary model retained)"})
            if sample == "Rank2019" and threshold == 20:
                if status != "None":
                    raise ValueError("Ranking MLE comparison unexpectedly has separation.")
                mle = fit_logistic(prepared, frame, 20, shared, {**result["config"], "logit_method": "ordinary"})
                firth = result["logits"][sample, 20]
                all_coefficients = mle["coefficients"].merge(firth["coefficients"], on="variable", suffixes=("_MLE", "_Firth"))
                all_margins = mle["marginal_effects"].merge(firth["marginal_effects"], on="variable", suffixes=("_MLE", "_Firth"))
                comparison = all_coefficients[["variable", "coefficient_MLE", "std_error_MLE", "p_value_MLE", "odds_ratio_MLE",
                                               "coefficient_Firth", "std_error_Firth", "p_value_Firth", "odds_ratio_Firth"]].copy()
                comparison = comparison.merge(all_margins[["variable", "AME_MLE", "p_value_MLE", "AME_Firth", "p_value_Firth"]]
                                              .rename(columns={"p_value_MLE": "AME_p_value_MLE", "p_value_Firth": "AME_p_value_Firth"}),
                                              on="variable", how="left")
                comparison.insert(0, "sample", sample)
                mle_compare.append(comparison)
        for period in ["P2", "P3"]:
            prepared = result["prepared"][sample, period]
            profit = f"profit_margin_start_{period}"
            for threshold in [15, 20, 25]:
                name = f"BottomP1_{threshold}"
                product = f"profitability_z_x_{name}"
                group = result["growths"][sample, threshold, period]
                augmented = result["interactions"].get((sample, threshold, period))
                fit = augmented["fit"] if augmented is not None else group["fit"]
                design = pd.DataFrame(fit.model.exog, index=prepared.x.index, columns=fit.model.exog_names)
                np.testing.assert_allclose(design[profit], prepared.x[profit], atol=1e-12)
                if augmented is not None:
                    np.testing.assert_allclose(design[product], prepared.x[profit] * frame.loc[design.index, name].astype(float), atol=1e-12)
                    covariance = fit.cov_params()
                    combined = float(fit.params[profit] + fit.params[product])
                    combined_variance = float(covariance.loc[profit, profit] + covariance.loc[product, product] + 2 * covariance.loc[profit, product])
                    combined_row = augmented["slopes"].set_index("group").loc[name]
                    np.testing.assert_allclose([combined, np.sqrt(combined_variance)], combined_row[["estimate", "std_error"]].to_numpy(dtype=float), atol=1e-10)
                    units.append({"sample": sample, "period": period, "threshold": threshold,
                                  "main_effect_column": profit, "main_effect_values": "Within-model-sample z-score",
                                  "interaction_source_column": profit, "interaction_column": product,
                                  "identical_units": "Yes", "combined_effect_valid": "Yes",
                                  "var_profitability": covariance.loc[profit, profit],
                                  "var_interaction": covariance.loc[product, product],
                                  "covariance_main_interaction": covariance.loc[profit, product],
                                  "combined_coefficient": combined, "combined_variance": combined_variance,
                                  "combined_SE": np.sqrt(combined_variance), "combined_p_value": combined_row.p_value})
                model_vifs = {}
                for column in design.columns:
                    if column == "const":
                        continue
                    target = design[column].to_numpy()
                    other = design.drop(columns=column).to_numpy()
                    residual = target - other @ np.linalg.lstsq(other, target, rcond=None)[0]
                    denominator = np.sum((target - target.mean()) ** 2)
                    vif = denominator / (residual @ residual)
                    model_vifs[column] = vif
                    vifs.append({"sample": sample, "period": period, "threshold": threshold,
                                 "variable": column, "VIF": vif})
                diagnosed = fit.get_influence()
                z = prepared.x[profit]
                share = (z - z.mean()) ** 2 / np.sum((z - z.mean()) ** 2)
                most_influential = share.idxmax()
                row = frame.loc[most_influential]
                group_sds = []
                for indicator in [0, 1]:
                    values = z.loc[frame.loc[z.index, name].eq(indicator)]
                    group_sds.append(values.std(ddof=0))
                    variation.append({"sample": sample, "period": period, "threshold": threshold,
                                      "BottomP1": indicator, "N": len(values), "profit_mean_z": values.mean(),
                                      "profit_SD_z": values.std(ddof=0), "profit_min_z": values.min(), "profit_max_z": values.max()})
                influence.append({"sample": sample, "period": period, "threshold": threshold,
                                  "N": len(design), "VIF_profitability": model_vifs[profit],
                                  "VIF_profitability_interaction": model_vifs.get(product, np.nan),
                                  "VIF_group": model_vifs[name], "condition_number": np.linalg.cond(design),
                                  "matrix_rank": np.linalg.matrix_rank(design), "parameters": len(design.columns),
                                  "profit_SD_z_other": group_sds[0], "profit_SD_z_bottom": group_sds[1],
                                  "outlier_nip": str(row.nip), "outlier_company": row.company,
                                  "outlier_profit_raw": prepared.estimation.loc[most_influential, profit],
                                  "outlier_profit_SS_share": float(share.loc[most_influential]),
                                  "outlier_leverage": float(diagnosed.hat_matrix_diag[design.index.get_loc(most_influential)]),
                                  "outlier_Cooks_D": float(diagnosed.cooks_distance[0][design.index.get_loc(most_influential)])})
                group_robust.append({"sample": sample, "threshold": threshold, "period": period,
                                     "N": group["summary"]["N"], "bottom_N": group["summary"]["bottom_N"],
                                     **coefficient(group["fit"], name)})
                if augmented is not None:
                    interaction_robust.append({"sample": sample, "threshold": threshold, "period": period,
                                               "N": len(design), **coefficient(fit, product),
                                               "bottom_profit_slope": combined, "bottom_profit_p_value": combined_row.p_value})
                if period == "P2" and threshold == 20:
                    primary = group["fit"]
                    design_group = pd.DataFrame(primary.model.exog, index=prepared.x.index, columns=primary.model.exog_names)
                    lag = ols.lag_growth_col(shared, "P2")
                    no_lag = ols.fit_ols(prepared.y, design_group.drop(columns=lag), shared)
                    for variant, fitted in [("With continuous P1-growth lag (primary)", primary),
                                            ("Without continuous P1-growth lag (diagnostic)", no_lag)]:
                        lag_diagnostic.append({"sample": sample, "variant": variant, "N": int(fitted.nobs),
                                               "bottom_N": int(design_group[name].sum()), **coefficient(fitted, name),
                                               "R2": fitted.rsquared, "outcome_preprocessing": "Same sample, winsor bounds and z-score as primary", "covariance": shared["covariance_type"]})
        for group in [0, 1]:
            subset = frame.loc[frame.BottomP1_20.eq(group)]
            for period in ["P1", "P2", "P3"]:
                values = subset[f"{ols.get_growth_prefix(shared)}growth_{period}"].dropna()
                extreme.append({"sample": sample, "group": f"BottomP1_20 = {group}", "period": period,
                                "N": len(values), "unit": "rate", "mean": values.mean(), "median": values.median(),
                                "p05": values.quantile(.05), "p95": values.quantile(.95), "maximum": values.max(),
                                "max_contribution_to_mean": values.max() / len(values),
                                "mean_without_largest": values.drop(values.idxmax()).mean()})
    return {"counts": pd.DataFrame(counts), "sample_sizes": pd.DataFrame(sizes),
            "standardisation": pd.DataFrame(scaling), "sector_counts": pd.DataFrame(sectors),
            "sector_references": pd.DataFrame(references), "separation": pd.DataFrame(separation),
            "rank_MLE_Firth": pd.concat(mle_compare, ignore_index=True),
            "interaction_units": pd.DataFrame(units), "VIF": pd.DataFrame(vifs),
            "group_profit_variation": pd.DataFrame(variation), "influence": pd.DataFrame(influence),
            "group_threshold_effects": pd.DataFrame(group_robust),
            "interaction_threshold_effects": pd.DataFrame(interaction_robust),
            "P2_lag_sensitivity": pd.DataFrame(lag_diagnostic), "raw_growth_extremes": pd.DataFrame(extreme)}


if __name__ == "__main__":
    # Keep original workbook untouched during an audit-only run.
    from pathlib import Path
    import tempfile
    import code_severe_p1_decline as analysis
    with tempfile.TemporaryDirectory(prefix="severe_p1_audit_") as directory:
        result = analysis.run_analysis({"output_file": str(Path(directory) / "audit_reproduction.xlsx")})
        for name, table in result["audit"].items():
            print(f"\n{name}\n{table.to_string(index=False)}")
