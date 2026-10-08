# Regression workbook presentation

Updated 8 October 2026. Comparable regression matrices in primary OLS, the compact interaction supplement and quantile reports share one presentation rule in `code_format_regression_tables.py`.

- Coefficient and existing significance symbols on the first line; p-value in parentheses on the second line. Existing three-decimal display and full-precision technical values are retained.
- Arial 10, wrapped cells and vertically centred text; model results horizontally centred and identifiers/variable labels left aligned.
- Coefficient rows at least 30 points, dependent-variable descriptions at least 42 points and headers at least 36 points. Taller researcher-note rows remain taller.
- Scenario columns 28 characters, variable-label columns 34, model columns 24. Existing hidden-column and outline settings, freeze panes, filters and reader annotations are preserved.
- Existing colours, bold/italic emphasis and custom researcher highlighting are retained. Shared writers use the established blue comparison-table headers.
- Comparison-table gridlines are hidden, matching the interaction report.

Scope: OLS `Compare_Main`/`Compare_Raw`; interaction `01_EXPORT_SIZE`/`02_PROFIT_MANUFACTURING`; quantile `Compare_FULL`/`Compare_P1`/`Compare_P2`/`Compare_P3`. Technical long tables and severe-decline coefficient tables retain separate typed coefficient/p-value columns for direct analysis; no values are merged or replaced there. Canonical datasets, historical archives and other analytical outputs are not reformatted by this command.

Run `python code_format_regression_tables.py` to apply formatting to the three existing active workbooks without fitting models. All three report writers also call the shared formatter, so subsequent rebuilds retain the layout. The updater preserves original XML namespaces, appends/reuses styles without changing existing style definitions, verifies every targeted cell's original value/formula representation, checks compatibility-prefix declarations and checks that all unrelated worksheet parts remain byte-identical before replacing each file. A concurrent file edit aborts publication. Repeat runs reuse existing matching fonts/styles.

Validation on the current files: every cell value in every worksheet matched the pre-formatting copies exactly; **3,232 coefficient/p-value cells** across eight comparison sheets have wrapping enabled, Arial 10 and sufficient row height. No regression, estimation sample, data-quality rule or exclusion changed.
