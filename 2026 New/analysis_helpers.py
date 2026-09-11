"""
Shared utility functions for the GRIP 2019-2024 resilience analysis.

The supporting period dataset covers 2018-2024; 2018 is used only for P1 lag
growth and does not change the main 2019-2024 analytical periods.

This file contains reusable helper functions for formatting, validation,
sample counting, safe statistics and workbook writing utilities.

Stable analytical definitions remain in analysis_config.py.
Script-specific modelling logic remains in individual runner scripts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analysis_config import SAMPLE_ORDER, build_sample_mask


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def safe_median(series: pd.Series) -> float:
    numeric = safe_numeric(series).dropna()
    return numeric.median() if len(numeric) else np.nan


def safe_quantile(series: pd.Series, q: float) -> float:
    numeric = safe_numeric(series).dropna()
    return numeric.quantile(q) if len(numeric) else np.nan


def safe_count(series: pd.Series) -> int:
    return int(safe_numeric(series).notna().sum())


def safe_share(numerator, denominator):
    if denominator == 0 or pd.isna(denominator):
        return np.nan
    return numerator / denominator


def has_ownership_data(df: pd.DataFrame) -> bool:
    return (
        "Foreign" in df.columns
        or "owner_num" in df.columns
        or "owner" in df.columns
        or "owner_type" in df.columns
    )


def build_shared_sample_counts(df: pd.DataFrame, complete_flag_column: str) -> dict[str, dict[str, int]]:
    counts = {}
    for sample_name in SAMPLE_ORDER:
        sample_df = df.loc[build_sample_mask(df, sample_name, complete_flag_column)]
        counts[sample_name] = {
            "rows": len(sample_df),
            "firms": sample_df["nip"].nunique() if "nip" in sample_df.columns else len(sample_df),
        }
    return counts


def apply_order_column(
    df: pd.DataFrame,
    column: str,
    order_map: dict,
    output_column: str,
) -> pd.DataFrame:
    output = df.copy()
    output[output_column] = output[column].map(order_map)
    return output


def set_table_column_widths(
    worksheet,
    df: pd.DataFrame,
    numeric_min_width: int = 12,
    numeric_max_width: int = 15,
    text_min_width: int = 18,
    text_max_width: int = 35,
) -> None:
    for col_idx, column_name in enumerate(df.columns):
        series = df[column_name]
        if series.empty:
            max_content_width = len(str(column_name))
        else:
            sample_width = series.astype("string").fillna("").map(len).max()
            max_content_width = max(len(str(column_name)), int(sample_width))
        if pd.api.types.is_numeric_dtype(series):
            width = min(max(len(str(column_name)) + 2, numeric_min_width), numeric_max_width)
        else:
            width = min(max(max_content_width + 2, text_min_width), text_max_width)
        worksheet.set_column(col_idx, col_idx, width)


def apply_header_format(worksheet, df: pd.DataFrame, header_format) -> None:
    for col_idx, column_name in enumerate(df.columns):
        worksheet.write(0, col_idx, column_name, header_format)


def apply_safe_autofilter(worksheet, df: pd.DataFrame) -> None:
    if not df.empty:
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)


def freeze_and_hide_gridlines(worksheet, row: int = 1, col: int = 0) -> None:
    worksheet.freeze_panes(row, col)
    worksheet.hide_gridlines(2)
