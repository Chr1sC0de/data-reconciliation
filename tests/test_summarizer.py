import numpy as np
import polars as pl
from dbqq import connectors
from datarec import tables


try:
    from tests.queries import query1, query2
except ImportError:
    import sys
    import pathlib as pt

    cwd = pt.Path(__file__).parent.absolute()
    sys.path.append(cwd.parent.absolute().as_posix())
    from tests.queries import query1, query2


def validate_no_index():
    columns = [
        "sum_energy",
        "sum_marketfeevalue",
        "sum_feerate",
        "sum_feeunits",
    ]

    pl_columns = [pl.col(c) for c in columns]

    connector1 = connectors.oracle.improd()
    connector2 = connectors.databricks.prod()

    p1 = connector1.cache()(query1).select(
        [pl.col(p.upper()) for p in columns]
    )

    p2 = connector2.cache()(query2).select(pl_columns)

    return tables.is_close_numeric(
        p1,
        p2,
        show_both_first=True,
        show_failed_first=True,
    )


def validate_with_index():
    connector1 = connectors.oracle.improd()
    connector2 = connectors.databricks.prod()

    p1 = connector1.cache()(query1)

    p2 = connector2.cache()(query2)

    return tables.is_close_numeric(
        p1,
        p2,
        column_indexes=range(0, 6),
        columns_to_ignore=0,
        show_both_first=True,
        show_failed_first=True,
    )


validation_no_index = validate_no_index()
validation_with_index = validate_with_index()


class TestSummarization:
    def test_left_no_index(self):
        summary = tables.summarize_reconciliation(validation_no_index, "left")
        assert summary.validation_ratio_entries == 1, "failed"

    def test_right_no_index(self):
        summary = tables.summarize_reconciliation(validation_no_index, "right")
        assert np.isclose(
            summary.validation_ratio_entries, 0.9523809523809523
        ), "failed"

    def test_inner_no_index(self):
        summary = tables.summarize_reconciliation(validation_no_index, "inner")
        assert summary.validation_ratio_entries == 1, "failed"

    def test_outer_no_index(self):
        summary = tables.summarize_reconciliation(validation_no_index, "outer")
        assert np.isclose(
            summary.validation_ratio_entries, 0.9523809523809523
        ), "failed"

    def test_left_with_index(self):
        summary = tables.summarize_reconciliation(
            validation_with_index, "left"
        )
        assert summary.validation_ratio_entries == 1, "failed"

    def test_right_with_index(self):
        summary = tables.summarize_reconciliation(
            validation_with_index, "right"
        )
        assert np.isclose(
            summary.validation_ratio_entries, 0.9523809523809523
        ), "failed"

    def test_inner_with_index(self):
        summary = tables.summarize_reconciliation(
            validation_with_index, "inner"
        )
        assert summary.validation_ratio_entries == 1, "failed"

    def test_outer_with_index(self):
        summary = tables.summarize_reconciliation(
            validation_with_index, "outer"
        )
        assert np.isclose(
            summary.validation_ratio_entries, 0.9523809523809523
        ), "failed"


if __name__ == "__main__":
    T = TestSummarization()
    T.test_right_no_index()
    T.test_left_no_index()
    T.test_inner_no_index()
    T.test_outer_no_index()

    T.test_right_with_index()
    T.test_left_with_index()
    T.test_inner_with_index()
    T.test_outer_with_index()
