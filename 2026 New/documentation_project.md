# Project files and workflow

## Naming convention

Use lowercase category prefixes so alphabetical sorting groups files consistently:

| Category | Convention | Current example |
| --- | --- | --- |
| Scripts and shared modules | `code_<purpose>.py` | `code_quantile.py` |
| Datasets | `data_<structure>_<years>.<format>` | `data_period_2018-2024.parquet` |
| Results | `results_<analysis>.xlsx` | `results_quantile.xlsx` |
| Documentation | `documentation_<topic>.md` | `documentation_quantile.md` |
| Inspection notebooks | `notebook_<purpose>.ipynb` | `notebook_check_period.ipynb` |
| Dated historical results | `archive/results_<analysis>_<YYYY-MM-DD>.xlsx` | `archive/results_ols_scenarios_2026-09-29.xlsx` |

Keep `AGENTS.md` unchanged as a special instruction filename. Year ranges belong on datasets; `period` is retained where it describes the dataset structure, but omitted from results. Existing undated snapshots retain an undated name rather than receiving an invented date. Local Python caches, macOS metadata, and notebook checkpoints are ignored by Git.

## Active workflow

Run commands from this directory:

1. `python3 code_build_core.py` builds `data_core_2018-2024.parquet` and `.xlsx` from `data_panel_2018-2024.parquet`.
2. `python3 code_build_period.py` builds `data_period_2018-2024.parquet` and `.xlsx` from the annual core dataset.
3. `python3 code_ols_scenarios.py` writes additive `results_ols_scenarios.xlsx` and extended centred-interaction `results_ols_interactions.xlsx`, including a matched-model comparison in the latter. Both retain all 64 scenario/period/variant models. `python code_check_ols_reporting.py` verifies the split and reproducibility.
4. `python3 code_quantile.py` writes `results_quantile.xlsx` with the shared OLS `winsor_std` model specification.
5. `python3 code_diagnostics_trajectories.py` writes `results_diagnostics_trajectories.xlsx`.

The separate severe-P1 supplement runs with `python3 code_severe_p1_decline.py`; `python3 code_check_severe_p1_decline.py` validates and reproduces it. Its exact user-specified output is `Results_severe_P1_decline_analysis.xlsx`, an explicit exception to the usual lowercase filenames. It examines Rank2019 and Rank2019_Manufacturing at fixed -15%, -20%, and -25% P1 thresholds. See `documentation_severe_p1_decline.md` for the method, separation handling, interpretation, and eight-sheet structure. It does not alter the main analyses or canonical datasets.

`python3 code_audit_severe_p1_decline.py` reproduces the supplement's sample/scaling, separation, interaction-unit, VIF/influence, raw-growth-tail, MLE-comparison, and matched-sample P2 no-lag diagnostics without overwriting its workbook. Findings and interpretation cautions are recorded in `documentation_severe_p1_decline_audit.md`; the primary supplementary estimates and existing main results were preserved during the audit.

Shared model settings and metadata live in `code_config.py`; shared helpers live in `code_helpers.py`. Documentation of formulas, samples, preprocessing, and output tabs remains in the corresponding `documentation_*.md` files.

The independent data-quality audit runs with `python code_audit_data_quality.py`; `python code_check_data_quality_audit.py` runs its acceptance tests. It reads existing canonical Parquet files without rebuilding them or fitting regressions, retains the full population including manual analytical exclusions, and respects the approved 2018 sales-only structure. It writes `results_data_quality_audit.xlsx`, supporting `results_data_quality_audit_metadata.json`, and an append-preserved `data_quality_audit_decisions.csv`. Review decisions never modify datasets or regression samples. See `documentation_data_quality_audit.md`; the original specification remains `documentation_data_quality_audit_specification.md`. `code_write_data_quality_audit.py` supplies large-table native XLSX export; `code_write_data_quality_audit.mjs` provides optional bounded visual previews through the bundled artifact-tool runtime.

Mean-centred interaction construction is shared across all analytical runners. `python3 code_check_interaction_centring.py` runs generic automated tests and compares all 64 OLS models before/after, exporting `results_interaction_centring.xlsx` with actual means/SDs, correlation and predictor-VIF diagnostics. See `documentation_interaction_centring.md` for equivalence, coefficient interpretation and the QuantReg precision caveat. Reader annotations in trailing OLS comparison columns are preserved by scenario/row label during rebuilds.

Manual analytical exclusions are maintained once in `code_config.MANUAL_EXCLUSIONS`, including the six requested exact names, verified NIPs and reason codes. `MANUAL_EXCLUSION_REASONS` defines the reason descriptions; unknown codes fail validation. OLS, quantile, severe-P1 and diagnostics apply exclusions before sample selection and all model transformations. Every results README records each firm's reason code/description and removed/already-absent status. Canonical datasets retain their original rows and performance thresholds. Run `python3 code_check_manual_exclusions.py` to check cross-pipeline alignment; see `documentation_manual_exclusions.md` for the current exclusion audit and rerun results.

The current datasets cover 2018–2024, with 2018 supporting the P1 lag. Main outcomes remain 2019–2024. Obsolete dataset copies ending in `2019-2024` were removed from the working directory; their historical versions remain recoverable from Git history.

## Notebook scope

- `notebook_check_core.ipynb` inspects the current annual core dataset.
- `notebook_check_period.ipynb` inspects the current period dataset, using its real growth columns for existing growth checks.
- `notebook_check_ols.ipynb` inspects the current scenario OLS workbook and its actual sheet names.
- `notebook_analysis.ipynb` retains its real-growth trajectory and ownership summaries, now pointing at the current period dataset and real-layer column names.

Notebooks are inspection tools, not the source of transformations or model estimation. Stale saved outputs and execution counts were cleared during migration so they do not misrepresent the updated sources.

## Migration on 6 October 2026

| Previous name | Current name |
| --- | --- |
| `analysis_config.py` | `code_config.py` |
| `analysis_helpers.py` | `code_helpers.py` |
| `build_core_panel.py` | `code_build_core.py` |
| `build_period_dataset.py` | `code_build_period.py` |
| `run_period_ols_scenarios.py` | `code_ols_scenarios.py` |
| `run_quantile_regression.py` | `code_quantile.py` |
| `run_trajectory_analysis.py` | `code_diagnostics_trajectories.py` |
| `Results_period_ols_scenarios.xlsx` | `results_ols_scenarios.xlsx` |
| `Results_period_quantile.xlsx` | `results_quantile.xlsx` |
| `Results_variable_diagnostics_and_trajectories.xlsx` | `results_diagnostics_trajectories.xlsx` |
| `Data_<structure>_<years>.*` | `data_<structure>_<years>.*` |
| `Data_core_2018-2024_documentation.md` | `documentation_core_2018-2024.md` |
| `Data_period_2018-2024_documentation.md` | `documentation_period_2018-2024.md` |
| `Results_period_ols_documentation.md` | `documentation_ols_scenarios.md` |
| `Results_period_quantile_documentation.md` | `documentation_quantile.md` |
| `Results_variable_diagnostics_and_trajectories_documentation.md` | `documentation_diagnostics_trajectories.md` |
| `check_core_panel.ipynb` | `notebook_check_core.ipynb` |
| `check_period_dataset.ipynb` | `notebook_check_period.ipynb` |
| `check_period_ols.ipynb` | `notebook_check_ols.ipynb` |
| `Analysis.ipynb` | `notebook_analysis.ipynb` |

During migration, historical workbooks previously in `Results/` moved to `archive/`. Dot-separated dates became ISO dates at the end of each filename; historical contents were preserved during that rename. The dated summary document is `archive/summary_2026-09-29.docx`. The original single-sample OLS comparison reference resolves to `archive/results_ols_2026-05-06.xlsx`.

## Unused-file cleanup on 6 October 2026

After the naming migration, five obsolete dataset copies were removed: the Parquet and Excel core and period datasets for 2019–2024, and the 2019–2024 enriched panel input. Their contents remain recoverable from earlier Git commits. Seven archived results and summary files were initially removed, then restored in full at the user's request.

All eight historical results and summary files are retained in `archive/`. They are required historical records, not disposable unused files. `archive/results_ols_2026-05-06.xlsx` additionally supports the OLS historical output-comparison check; it is a validation reference, not the current model input. Active datasets, exports, scripts, documentation, inspection notebooks, and results remain in place. Local obsolete bytecode caches were cleared.

File and import changes do not rename dataset columns, change formulas, change regression specifications, or alter stored dataset values. Active results workbooks are regenerated so their embedded paths and module references use the current filenames.

## Migration validation

- All seven renamed modules imported successfully.
- Both dataset builders reproduced their existing canonical datasets exactly; validation summaries included row counts, schema, missingness, and outcome statistics.
- All four notebooks executed successfully against the active files.
- All 64 OLS models and all 48 quantile models were estimated, with no skipped models; the quantile run recorded no convergence warnings.
- Numerical regression outputs, estimation sample sizes, and diagnostics results were compared with the pre-migration workbooks and remained unchanged.
- All renamed datasets and historical archive files retained their original byte content.
- Active workbook descriptions were refreshed to use the new filenames and module names.
