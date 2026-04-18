# Results_trajectory_analysis

## Purpose

`run_trajectory_analysis.py` produces descriptive trajectory-analysis reporting across three analytical samples derived from `Data_period_2019-2024.parquet`.

The workbook is intended to support interpretation of trajectory patterns and later regression results. It is a descriptive reporting layer only.

## Input file

- `Data_period_2019-2024.parquet`

## Sample definitions

- `All`: firms with `has_complete_trajectory == 1`
- `Rank2019`: firms with `has_complete_trajectory == 1` and `in_rank_2019 == 1`
- `Rank2019_Manufacturing`: firms with `has_complete_trajectory == 1`, `in_rank_2019 == 1`, and `manufacturing == 1`

## Variables used

- `growth_real_P1`
- `growth_real_P2`
- `growth_real_P3`
- `trajectory_3step`
- `trajectory_group`
- `index_2019`
- `index_2020`
- `index_2022`
- `index_2024`

Supporting descriptors may also be used for cross-tabs and firm-level inspection, including:

- `sector_en`
- `owner_num` and derived ownership labels
- `company`
- `in_rank_2019`
- `manufacturing`

## Descriptive emphasis

Medians are the primary descriptive growth statistic in grouped outputs because means may be influenced by extreme values.

Means are included for completeness but should be interpreted with caution in the presence of outliers.

## Workbook structure

Sheet naming convention:

- `All_*` for the full complete-trajectory sample
- `R19_*` for the 2019-ranking sample
- `R19M_*` for the 2019-ranking manufacturing sample

Sheets:

- `All_Overview`
- `All_Traj3Step`
- `All_TrajGroup`
- `All_GrowthByGroup`
- `All_PathIndex`
- `All_BySector`
- `All_ByOwner`
- `R19_Overview`
- `R19_Traj3Step`
- `R19_TrajGroup`
- `R19_GrowthByGroup`
- `R19_PathIndex`
- `R19_BySector`
- `R19_ByOwner`
- `R19M_Overview`
- `R19M_Traj3Step`
- `R19M_TrajGroup`
- `R19M_GrowthByGroup`
- `R19M_PathIndex`
- `R19M_BySector`
- `R19M_ByOwner`
- `Firm_Trajectories`
