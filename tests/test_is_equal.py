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
    connector1 = connectors.oracle.improd()
    connector2 = connectors.databricks.prod()
    p1 = connector1.cache()(query1)
    p2 = connector2.cache()(query2)
    validation = tables.is_equal(
        p1,
        p2,
        show_both_first=True,
        show_failed_first=True,
    )
    validation.results.collect().write_csv("reports/.is_equal_no_index.csv")


def test_validate_with_index():
    connector1 = connectors.oracle.improd()
    connector2 = connectors.databricks.prod()

    p1 = connector1.cache()(query1)

    p2 = connector2.cache()(query2)

    validation = tables.is_equal(
        p1,
        p2,
        column_indexes=range(0, 6),
        columns_to_ignore=0,
        show_both_first=True,
        show_failed_first=True,
    )

    validation.results.collect().write_csv("reports/.is_equal_with_index.csv")


if __name__ == "__main__":
    test_validate_with_index()
    test_validate_no_index()
