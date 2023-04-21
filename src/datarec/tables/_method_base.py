import abc
from dataclasses import dataclass
import typing as T

import polars as pl

from ..data import MethodData


@dataclass
class _ReconcilerMethodBase(abc.ABC):
    methods: MethodData

    @abc.abstractmethod
    def validator(
        self, test_columns: str, merged: pl.LazyFrame
    ) -> pl.LazyFrame:
        ...

    def __call__(
        self,
        pl1: pl.LazyFrame,
        pl2: pl.LazyFrame,
        test_columns: T.List[str],
        columns_to_ignore: T.List[int],
        **kwargs
    ) -> pl.LazyFrame:
        get_left = self.methods.get_left
        get_right = self.methods.get_right
        setcase = self.methods.setcase

        original_columns = pl1.columns.copy()

        pl1 = pl1.rename({c: get_left(c) for c in test_columns})
        pl1 = pl1.with_columns(pl.lit(True).alias(setcase("**left**")))
        pl2 = pl2.rename({c: get_right(c) for c in test_columns})
        pl2 = pl2.with_columns(pl.lit(True).alias(setcase("**right**")))

        original_test_columns = test_columns.copy()

        if len(columns_to_ignore) > 0:
            ignore = list(map(lambda x: test_columns[x], columns_to_ignore))
            test_columns = filter(lambda x: x not in ignore, test_columns)

        if not (len(original_test_columns) == len(original_columns)):
            merged = (
                pl1.join(
                    pl2,
                    how="outer",
                    on=[c for c in pl1.columns if c in original_columns],
                )
                .collect()
                .lazy()
            )
        else:
            merged = (
                (
                    pl1.with_row_count(name="row_number").join(
                        pl2.with_row_count(name="row_number"),
                        how="outer",
                        on=["row_number"],
                    )
                )
                .collect()
                .lazy()
            )

        merged = merged.with_columns(
            [
                pl.col(setcase(c)).fill_null(pl.lit(False))
                for c in ["**left**", "**right**"]
            ]
        )

        validation = self.validator(test_columns, merged, **kwargs)

        validation = validation.with_columns(
            (pl.col(setcase("**left**")) & pl.col(setcase("**right**"))).alias(
                setcase("**both**")
            )
        )

        return validation
