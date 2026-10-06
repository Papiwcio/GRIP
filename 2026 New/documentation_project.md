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
3. `python3 code_ols_scenarios.py` writes `results_ols_scenarios.xlsx`.
4. `python3 code_quantile.py` writes `results_quantile.xlsx` with the shared OLS `winsor_std` model specification.
5. `python3 code_diagnostics_trajectories.py` writes `results_diagnostics_trajectories.xlsx`.

Shared model settings and metadata live in `code_config.py`; shared helpers live in `code_helpers.py`. Documentation of formulas, samples, preprocessing, and output tabs remains in the corresponding `documentation_*.md` files.

The current datasets cover 2018–2024, with 2018 supporting the P1 lag. Main outcomes remain 2019–2024. Files ending in `2019-2024` are retained as historical dataset versions. Their data is preserved and they are not overwritten by the active builds.

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

Historical workbooks previously in `Results/` moved to `archive/`. Dot-separated dates became ISO dates at the end of each filename; historical contents were preserved. The dated summary document is `archive/summary_2026-09-29.docx`. The original single-sample OLS comparison reference now resolves to `archive/results_ols_2026-05-06.xlsx`.

File and import changes do not rename dataset columns, change formulas, change regression specifications, or alter stored dataset values. Active results workbooks are regenerated so their embedded paths and module references use the current filenames.

## Migration validation

- All seven renamed modules imported successfully.
- Both dataset builders reproduced their existing canonical datasets exactly; validation summaries included row counts, schema, missingness, and outcome statistics.
- All four notebooks executed successfully against the active files.
- All 64 OLS models and all 48 quantile models were estimated, with no skipped models; the quantile run recorded no convergence warnings.
- Numerical regression outputs, estimation sample sizes, and diagnostics results were compared with the pre-migration workbooks and remained unchanged.
- All renamed datasets and historical archive files retained their original byte content.
- Active workbook descriptions were refreshed to use the new filenames and module names.
