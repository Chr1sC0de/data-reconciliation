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


def test_validate_no_index():
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

    validation = tables.is_close_numeric(
        p1,
        p2,
        pl1_name="im_prod",
        pl2_name="databricks_prod",
        show_both_first=True,
        show_failed_first=True,
    )

    validation.results.collect().write_csv("reports/.is_close_no_index.csv")


def test_validate_with_index():
    connector1 = connectors.oracle.improd()
    connector2 = connectors.databricks.prod()

    p1 = connector1.cache()(query1)

    p2 = connector2.cache()(query2)

    validation = tables.is_close_numeric(
        p1,
        p2,
        pl1_name="im_prod",
        pl2_name="databricks_prod",
        column_indexes=range(0, 6),
        columns_to_ignore=0,
        show_both_first=True,
        show_failed_first=True,
    )

    validation.results.collect().write_csv("reports/.is_close_with_index.csv")


if __name__ == "__main__":
    test_validate_with_index()
    test_validate_no_index()
