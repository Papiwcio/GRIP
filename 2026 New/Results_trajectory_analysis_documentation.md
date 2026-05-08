# Results_trajectory_analysis

## Purpose

`run_trajectory_analysis.py` produces descriptive trajectory-analysis reporting from `Data_period_2019-2024.parquet`.

The workbook is a descriptive reporting layer. It does not estimate regressions and does not create trajectory classes beyond the deterministic trajectory variables already present in the period dataset.

## Input And Output

Input:

- `Data_period_2019-2024.parquet`

Output:

- `Results_trajectory_analysis.xlsx`

## Selected Trajectory Family

The script is controlled by:

- `CONFIG["trajectory_family"]`

Allowed values:

- `real`
- `nominal`

Current script configuration:

- `nominal`

## Current Variable Names

### Real Trajectory Family

Real period growth:

- `rgrowth_P1`
- `rgrowth_P2`
- `rgrowth_P3`

Real log growth:

- `rgrowth_log_P1`
- `rgrowth_log_P2`
- `rgrowth_log_P3`

Real annualised log growth:

- `rgrowth_log_ann_P1`
- `rgrowth_log_ann_P2`
- `rgrowth_log_ann_P3`

Real trajectory descriptors:

- `has_complete_rtrajectory`
- `rP1_sign`
- `rP2_sign`
- `rP3_sign`
- `rtrajectory_3step`
- `rtrajectory_label`
- `rtrajectory_group`
- `rindex_2019`
- `rindex_2020`
- `rindex_2022`
- `rindex_2024`

### Nominal Trajectory Family

Nominal period growth:

- `ngrowth_P1`
- `ngrowth_P2`
- `ngrowth_P3`

Nominal log growth:

- `ngrowth_log_P1`
- `ngrowth_log_P2`
- `ngrowth_log_P3`

Nominal annualised log growth:

- `ngrowth_log_ann_P1`
- `ngrowth_log_ann_P2`
- `ngrowth_log_ann_P3`

Nominal trajectory descriptors:

- `has_complete_ntrajectory`
- `nP1_sign`
- `nP2_sign`
- `nP3_sign`
- `ntrajectory_3step`
- `ntrajectory_label`
- `ntrajectory_group`
- `nindex_2019`
- `nindex_2020`
- `nindex_2022`
- `nindex_2024`

## Sample Definitions

Samples are built using the selected trajectory family's complete flag.

For real mode, the complete flag is:

- `has_complete_rtrajectory`

For nominal mode, the complete flag is:

- `has_complete_ntrajectory`

Sample definitions:

- `All`: `{complete_flag} == 1`
- `Rank2019`: `{complete_flag} == 1 and in_rank_2019 == 1`
- `Rank2019_Manufacturing`: `{complete_flag} == 1 and in_rank_2019 == 1 and manufacturing == 1`

## Supporting Descriptors

The reporting layer also uses descriptors where available:

- `nip`
- `company`
- `sector_en`
- `owner_num`
- `owner`
- `owner_type`
- `in_rank_2019`
- `manufacturing`

Ownership labels are derived from `owner_num`, `owner`, or `owner_type`, depending on available columns.

## Workbook Structure

`Results_trajectory_analysis.xlsx` contains:

- `Config`
- `Sample_Overview`
- `Trajectory_Distribution`
- `Growth_By_Trajectory`
- `Path_Index`
- `Sector_Owner_Profile`
- `Firm_Trajectories`

## Sheet Descriptions

### Config

Documents the selected trajectory family, input and output files, resolved source columns, sample definitions, period definitions, and trajectory levels.

### Sample_Overview

Summarises sample sizes and shares for the selected trajectory family.

### Trajectory_Distribution

Reports counts and shares for:

- three-step trajectory sequence
- descriptive trajectory label
- consolidated trajectory group

### Growth_By_Trajectory

Reports descriptive growth statistics by trajectory label and group.

Medians are the primary descriptive growth statistic because means may be influenced by extreme values.

### Path_Index

Reports descriptive path-index statistics based on the selected index columns.

For real mode these are:

- `rindex_2019`
- `rindex_2020`
- `rindex_2022`
- `rindex_2024`

For nominal mode these are:

- `nindex_2019`
- `nindex_2020`
- `nindex_2022`
- `nindex_2024`

### Sector_Owner_Profile

Reports trajectory-group counts and shares by:

- `sector_en`
- ownership label

### Firm_Trajectories

Firm-level trajectory inspection sheet with identifiers, selected trajectory variables, growth variables, and index columns.

## Presentation Notes

- Percent and share columns are formatted as percentages in Excel where applicable.
- Headers are formatted and frozen.
- The workbook is intended for descriptive interpretation and quality checks before regression interpretation.

## Last Updated For

- Script: `run_trajectory_analysis.py`
- Output file: `Results_trajectory_analysis.xlsx`
- Main change covered: current real/nominal trajectory variable names, selected trajectory family workflow, and current workbook sheet structure
- Date: 2026-05-08
