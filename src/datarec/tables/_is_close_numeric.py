from typing import Iterable, List

import polars as pl

from ..data import TableReconciliationData
from ._method_base import _ReconcilerMethodBase
from ._reconciler import Reconciler


class _ReconcilerMethodIsCloseFloat(_ReconcilerMethodBase):
    def validator(
        self, test_columns: str, merged: pl.LazyFrame, **kwargs
    ) -> pl.LazyFrame:
        get_left = self.methods.get_left
        get_right = self.methods.get_right
        setcase = self.methods.setcase
        a_tol = kwargs["a_tol"]
        r_tol = kwargs["r_tol"]

        validation = merged.with_columns(
            [
                (
                    (
                        (pl.col(get_left(c)) - pl.col(get_right(c))).abs()
                        < a_tol
                    )
                    | (
                        (
                            (pl.col(get_left(c)) - pl.col(get_right(c)))
                            / (
                                (pl.col(get_left(c)) + pl.col(get_right(c)))
                                / 2
                                + 1e-20
                            )
                        ).abs()
                        < r_tol
                    )
                )
                .fill_null(pl.lit(False))
                .alias("%s ~*%s*~" % (c, setcase("validation")))
                for c in test_columns
            ]
        )
        return validation


class _ReconcilerMethodIsCloseFloatNoIndex(_ReconcilerMethodIsCloseFloat):
    def __call__(
        self,
        pl1: pl.LazyFrame,
        pl2: pl.LazyFrame,
        columns_to_ignore: List[int],
        *,
        a_tol: float,
        r_tol: float
    ) -> pl.LazyFrame:
        test_columns = list(pl1.columns)
        return super().__call__(
            pl1, pl2, test_columns, columns_to_ignore, a_tol=a_tol, r_tol=r_tol
        )


class _ReconcilerMethodIsCloseFloatWithIndex(_ReconcilerMethodIsCloseFloat):
    def __call__(
        self,
        pl1: pl.LazyFrame,
        pl2: pl.LazyFrame,
        columns_to_ignore: List[int],
        columns_as_indexes: List[str],
        *,
        a_tol: float,
        r_tol: float
    ) -> pl.LazyFrame:
        index_columns = [pl1.columns[i] for i in columns_as_indexes]
        test_columns = list(
            filter(lambda x: x not in index_columns, pl1.columns)
        )
        return super().__call__(
            pl1, pl2, test_columns, columns_to_ignore, a_tol=a_tol, r_tol=r_tol
        )


def is_close_numeric(
    p1: pl.LazyFrame,
    p2: pl.LazyFrame,
    pl1_name="left",
    pl2_name="right",
    interlaced: bool = True,
    column_case: str = "upper",
    show_both_first: bool = True,
    show_failed_first: bool = True,
    columns_to_ignore: Iterable[int] = None,
    column_indexes: Iterable[int] = None,
    a_tol: float = 10e-3,
    r_tol: float = 10e-4,
) -> TableReconciliationData:
    return Reconciler(
        _ReconcilerMethodIsCloseFloatNoIndex,
        _ReconcilerMethodIsCloseFloatWithIndex,
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
        a_tol=a_tol,
        r_tol=r_tol,
    )
