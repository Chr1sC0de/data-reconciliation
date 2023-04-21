import numpy as np
from numpy import typing as np_types
import polars as pl
from ..data import (
    TableReconciliationData,
    TableReconciliationSummarizationData,
)


def _get_validations(lf: pl.LazyFrame) -> pl.LazyFrame:
    validation_columns = [
        c for c in lf.columns if "~*validation*~" in c.lower()
    ]
    return lf.select([pl.col(c) for c in validation_columns])


def _make_2d(arr: np_types.NDArray) -> np_types.NDArray:
    if len(arr.shape) < 2:
        return arr.reshape(-1, 1)
    return arr


def _summarizer(arr: np_types.DTypeLike):
    rows, cols = arr.shape
    n_entries = rows * cols

    n_entries_passed = arr.sum()
    n_entries_failed = n_entries - n_entries_passed

    n_rows_passed = (arr.sum(axis=1) == cols).sum()
    n_rows_partially_passed = (
        np.logical_and((arr.sum(axis=1) > 0), (arr.sum(axis=1) != cols))
    ).sum()

    if len(arr) > 0:
        avg_row_invalidations = (arr == 0).sum(1).mean()
        std_row_invalidations = (arr == 0).sum(1).std()
    else:
        avg_row_invalidations = 0
        std_row_invalidations = 0

    n_rows_failed = rows - n_rows_passed - n_rows_partially_passed

    try:
        entry_validation_ratio = n_entries_passed / n_entries
    except ZeroDivisionError:
        entry_validation_ratio = 0

    try:
        row_validation_ratio = n_rows_passed / rows
    except ZeroDivisionError:
        row_validation_ratio = 0

    return (
        rows,
        cols,
        n_entries,
        n_entries_passed,
        n_entries_failed,
        n_rows_passed,
        n_rows_partially_passed,
        n_rows_failed,
        entry_validation_ratio,
        row_validation_ratio,
        avg_row_invalidations,
        std_row_invalidations,
    )


def _get_totals(lf: pl.LazyFrame):
    left_col = [c for c in lf.columns if "**left**" in c.lower()][0]
    right_col = [c for c in lf.columns if "**right**" in c.lower()][0]
    intersecting_col = [c for c in lf.columns if "**both**" in c.lower()][0]
    n_total_rows_left = lf.select(left_col).collect().to_numpy().sum()
    n_total_rows_right = lf.select(right_col).collect().to_numpy().sum()
    n_total_rows_intersection = (
        lf.select(intersecting_col).collect().to_numpy().sum()
    )
    n_total_rows_union = len(lf.select(left_col).collect())

    return (
        n_total_rows_left,
        n_total_rows_right,
        n_total_rows_intersection,
        n_total_rows_union,
    )


def _shared_summarize(
    lf: pl.LazyFrame, filter_col: str
) -> TableReconciliationData:
    lf_original = lf
    if filter_col is not None:
        lf = lf.filter(pl.col(filter_col))
    arr = _make_2d(_get_validations(lf).collect().to_numpy())
    return TableReconciliationSummarizationData(
        *_summarizer(arr), *_get_totals(lf_original)
    )


def _left_summarize(lf: pl.LazyFrame) -> TableReconciliationSummarizationData:
    filter_col = [c for c in lf.columns if "**left**" in c.lower()][0]
    return _shared_summarize(lf, filter_col)


def _right_summarize(lf: pl.LazyFrame) -> TableReconciliationSummarizationData:
    filter_col = [c for c in lf.columns if "**right**" in c.lower()][0]
    return _shared_summarize(lf, filter_col)


def _inner_summarize(lf: pl.LazyFrame) -> TableReconciliationSummarizationData:
    filter_col = [c for c in lf.columns if "**both**" in c.lower()][0]
    return _shared_summarize(lf, filter_col)


def _outer_summarize(lf: pl.LazyFrame) -> TableReconciliationSummarizationData:
    return _shared_summarize(lf, None)


summarizer_map = {
    "left": _left_summarize,
    "right": _right_summarize,
    "inner": _inner_summarize,
    "outer": _outer_summarize,
}


def summarize_reconciliation(
    reconciliation: TableReconciliationData, join: str = "outer"
) -> TableReconciliationSummarizationData:
    results = reconciliation.results
    column_indexes = reconciliation.columns_indexes
    columns_ignored = reconciliation.columns_ignored

    # columns to process
    processable_columns = [
        pl.col(c)
        for c in results.columns
        if all([c not in t for t in (columns_ignored + column_indexes)])
    ]

    # grab the validations
    processable_table = results.select(processable_columns)

    # summarizer method
    summarizer_method = summarizer_map[join]

    return summarizer_method(processable_table)
