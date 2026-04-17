# AGENTS.md

## Purpose

This repository supports the preparation of research-grade datasets and analysis for firm resilience and growth trajectories based on Polish company panel data for 2019–2024.

The work must prioritise:
- reproducibility
- schema consistency
- transparent variable definitions
- exact compliance with requested data structures

Do not optimise by replacing requested variables with “similar” alternatives.

---

## General principles

1. Treat the dataset-building scripts as the source of truth.
2. Treat documentation as part of the deliverable, not as an optional extra.
3. Do not silently simplify schemas.
4. Do not silently rename variables unless explicitly instructed.
5. Do not substitute conceptually similar variables for requested ones.
6. If a required variable cannot be built, report it explicitly.
7. Prefer exact compliance over compactness or creativity.

---

## Canonical files

### Annual canonical dataset
- `Data_core_2019-2024.parquet`
- `Data_core_2019-2024.xlsx`

This is the canonical annual firm-year dataset.
It must remain stable and documented.

### Period canonical dataset
- `Data_period_2019-2024.parquet`
- `Data_period_2019-2024.xlsx`

This is the canonical firm-level period dataset derived from the annual canonical dataset.

### Legacy input
- `Data_panel_2019-2024.parquet`

This is a legacy enriched panel input and is not the canonical master file.

---

## Required workflow for data changes

If a change affects the structure or logic of a canonical dataset:

1. update the build script
2. rebuild the dataset
3. update the corresponding documentation file
4. print validation output
5. keep naming and schema consistent

Do not manually edit canonical output files.

---

## Documentation requirements

Each canonical dataset must have a matching documentation file:

- `Data_core_2019-2024_documentation.md`
- `Data_period_2019-2024_documentation.md`

Documentation must always match the current dataset.

For calculated variables, documentation must include:
- variable name
- formula or rule
- handling of missing values
- handling of zero denominators where relevant

Do not leave documentation behind after schema changes.

---

## Naming rules

Use exact requested naming conventions.

### Period variables
Use:
- `growth_real_P1`, `growth_real_P2`, `growth_real_P3`
- `growth_log_P1`, `growth_log_P2`, `growth_log_P3`
- `growth_log_ann_P1`, `growth_log_ann_P2`, `growth_log_ann_P3`

Do not replace with alternative naming such as:
- `p1_sales_growth`
- `p1_sales_real_growth`
- `p1_sales_log_growth`

### Start-of-period covariates
Use:
- `variable_start_P1`
- `variable_start_P2`
- `variable_start_P3`

Example:
- `ln_sales_start_P1`
- `export_ratio_start_P2`
- `roa_start_P3`

### Lagged growth variables
Use only explicitly requested lag variables.
Do not invent extra lag structures.

---

## Variable discipline

### Keep required variables exactly as specified
If a variable is explicitly requested, it must be kept.

Examples of variables that must not be dropped when requested:
- `rank_2019`
- `in_rank_2019`
- `owner`
- `owner_num`
- `manufacturing`

### Do not substitute “similar” variables
If requested:
- `capital_ratio`

Do not replace it with:
- `equity_ratio`

If requested:
- Dupont ratios

Do not create alternative ratio sets unless explicitly asked.

---

## Core dataset rules

`Data_core_2019-2024` must contain annual firm-year variables only.

It may include:
- identifiers
- structural descriptors
- annual raw financial variables
- annual derived variables
- annual ratios
- annual productivity measures
- annual growth variables
- annual data quality flags

It must not include:
- old paper-specific growth summaries
- old starting_ variables from prior design
- HGX / Hint variables
- trajectory classes
- regression outputs

---

## Period dataset rules

`Data_period_2019-2024` must contain one row per firm only.

It must include:
- stable descriptors
- period growth outcomes
- annualised log growth variables
- lagged growth variables
- start-of-period covariates
- period availability flags

It must not include:
- raw annual panel rows
- duplicate naming systems
- exploratory trajectory classes
- regression outputs

---

## Growth variable rules

### Annual canonical dataset
Annual growth variables are year-on-year within firm.

### Period dataset
Use exact period definitions:
- `P1`: 2019 → 2020
- `P2`: 2020 → 2022
- `P3`: 2022 → 2024

Required period growth forms:
1. real growth
2. log growth
3. annualised log growth

Annualised log growth is required for period regressions because periods differ in length.

---

## Missing-value and denominator rules

- If a denominator is zero, return `NaN`
- Do not silently replace missing values with zero
- Do not silently clip or transform values unless explicitly instructed
- If logs require positive values and values are non-positive, return `NaN`
- Report counts of non-positive values that prevent log calculations

---

## Validation rules

Every dataset build must print validation output.

At minimum include:
- input row count
- output row count
- unique firm count
- duplicate check
- final column list
- final column count
- missing counts for key variables
- summary statistics for key outcomes

If a required block of variables is missing, report this explicitly.

---

## Notebook rules

Inspection notebooks are for checking only.

They may:
- load the final dataset
- show shape, dtypes, missingness, basic summaries

They must not:
- contain the main transformation pipeline
- become the source of truth for data preparation

---

## Coding style

- use clear pandas code
- use helper functions where useful
- comment key transformations
- prefer clarity over cleverness
- keep deterministic behaviour

---

## If uncertain

If unsure whether to:
- drop a variable
- rename a variable
- substitute a variable
- simplify a schema

default to:
- keeping the requested variable
- preserving the requested name
- reporting uncertainty explicitly