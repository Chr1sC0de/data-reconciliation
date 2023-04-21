import typing as T

from ..data import MethodData, TableReconciliationData
from ..utils import GetSuffixed, SetCase
from ..utils.functions import (
    convert_iterable_to_list,
    get_formated_ordered_union,
    group_suffixed,
    validate_index_columns,
)

try:
    from collections.abc import Callable
except ImportError:
    from typing import Callable

import polars as pl


class Reconciler:
    def __init__(
        self, NoIndexConstructor: Callable, WithIndexConstructor: Callable
    ):
        self.NoIndexConstructor = NoIndexConstructor
        self.WithIndexConstructor = WithIndexConstructor

    def __call__(
        self,
        pl1: pl.LazyFrame,
        pl2: pl.LazyFrame,
        pl1_name: str = "left",
        pl2_name: str = "right",
        interlaced: bool = True,
        column_case: str = "upper",
        show_both_first: bool = True,
        show_failed_first: bool = True,
        columns_to_ignore: T.Iterable = None,
        column_indexes: T.Iterable = None,
        **kwargs
    ) -> TableReconciliationData:
        assert pl1_name != pl2_name, "tables names must be different"

        setcase = SetCase(column_case)
        get_left = GetSuffixed(setcase, pl1_name)
        get_right = GetSuffixed(setcase, pl2_name)
        methods = MethodData(setcase, get_left, get_right)

        compare_no_index = self.NoIndexConstructor(methods)
        compare_with_index = self.WithIndexConstructor(methods)

        if column_indexes is None:
            column_indexes = []

        column_indexes = convert_iterable_to_list(column_indexes)

        if columns_to_ignore is None:
            columns_to_ignore = []

        columns_to_ignore = convert_iterable_to_list(columns_to_ignore)

        shared_columns = get_formated_ordered_union(
            pl1.columns, pl2.columns, formatter=setcase
        )

        pl1 = pl1.rename({c: setcase(c) for c in pl1.columns})
        pl2 = pl2.rename({c: setcase(c) for c in pl2.columns})

        pl1 = pl1.select([pl.col(c) for c in shared_columns])
        pl2 = pl2.select([pl.col(c) for c in shared_columns])

        _all_columns = pl1.columns
        _columns_used_as_indexes = [shared_columns[i] for i in column_indexes]
        _columns_tested = [
            c for c in _all_columns if c not in _columns_used_as_indexes
        ]
        _columns_ignored = [_columns_tested[i] for i in columns_to_ignore]
        _index_and_tested = _columns_used_as_indexes + _columns_ignored
        _tested_columns = [
            c for c in _all_columns if c not in _index_and_tested
        ]

        if len(column_indexes) == 0:
            validation = compare_no_index(
                pl1, pl2, columns_to_ignore, **kwargs
            )
        else:
            validate_index_columns(pl1.clone(), column_indexes, "left")
            validate_index_columns(pl2.clone(), column_indexes, "right")

            validation = compare_with_index(
                pl1, pl2, columns_to_ignore, column_indexes, **kwargs
            )

        if interlaced:
            sorted_columns = group_suffixed(validation.columns)

            validation = validation.select([pl.col(c) for c in sorted_columns])

        if show_failed_first:
            validation_columns = filter(
                lambda x: setcase("~*validation*~") in x, validation.columns
            )
            validation = validation.sort(by=validation_columns)

        if show_both_first:
            validation = validation.sort(
                by=setcase("**both**"), descending=True
            )

        return TableReconciliationData(
            validation,
            pl1_name,
            pl2_name,
            _all_columns,
            _columns_used_as_indexes,
            _columns_ignored,
            _tested_columns,
        )
