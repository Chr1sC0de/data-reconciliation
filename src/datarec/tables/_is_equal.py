import typing as T

import polars as pl

from ..data import TableReconciliationData
from ._method_base import _ReconcilerMethodBase
from ._reconciler import Reconciler


class _IsEqualReconcilerMethodBase(_ReconcilerMethodBase):
    def validator(
        self, test_columns: str, merged: pl.LazyFrame
    ) -> pl.LazyFrame:
        get_left = self.methods.get_left
        get_right = self.methods.get_right
        setcase = self.methods.setcase

        validation = merged.with_columns(
            [
                (pl.col(get_left(c)) == pl.col(get_right(c))).alias(
                    "%s ~*%s*~" % (c, setcase("validation"))
                )
                for c in test_columns
            ]
        )
        return validation


class _IsEqualReconcilerMethodeNoIndex(_IsEqualReconcilerMethodBase):
    def __call__(
        self,
        pl1: pl.LazyFrame,
        pl2: pl.LazyFrame,
        columns_to_ignore: T.List[int],
    ) -> pl.LazyFrame:
        test_columns = list(pl1.columns)
        return super().__call__(pl1, pl2, test_columns, columns_to_ignore)


class _IsEqualReconcilerMethodWithIndex(_IsEqualReconcilerMethodBase):
    def __call__(
        self,
        pl1: pl.LazyFrame,
        pl2: pl.LazyFrame,
        columns_to_ignore: T.List[int],
        columns_as_indexes: T.List[str],
    ) -> pl.LazyFrame:
        index_columns = [pl1.columns[i] for i in columns_as_indexes]
        test_columns = list(
            filter(lambda x: x not in index_columns, pl1.columns)
        )
        return super().__call__(pl1, pl2, test_columns, columns_to_ignore)


def is_equal(
    p1: pl.LazyFrame,
    p2: pl.LazyFrame,
    pl1_name="left",
    pl2_name="right",
    interlaced: bool = True,
    column_case: str = "upper",
    show_both_first: bool = True,
    show_failed_first: bool = True,
    columns_to_ignore: T.Iterable[int] = None,
    column_indexes: T.Iterable[int] = None,
) -> TableReconciliationData:
    return Reconciler(
        _IsEqualReconcilerMethodeNoIndex, _IsEqualReconcilerMethodWithIndex
    )(
        p1,
        p2,
        pl1_name=pl1_name,
        pl2_name=pl2_name,
        interlaced=interlaced,
        column_case=column_case,
        show_both_first=show_both_first,
        show_failed_first=show_failed_first,
        columns_to_ignore=columns_to_ignore,
        column_indexes=column_indexes,
    )
