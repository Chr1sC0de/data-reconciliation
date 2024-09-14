import yaml
import json
from pprint import pformat
from typing import List
import polars as pl
from dataclasses import dataclass
from .utils._set_case import SetCase
from .utils._get_suffixed import GetSuffixed


@dataclass
class MethodData:
    setcase: SetCase
    get_left: GetSuffixed
    get_right: GetSuffixed


@dataclass
class TableReconciliationData:
    results: pl.LazyFrame
    left: str
    right: str
    columns_all: List[str]
    columns_indexes: List[str]
    columns_ignored: List[str]
    columns_tested: List[str]

    @property
    def is_left_col(self) -> List[str]:
        return [c for c in self.results.columns if "**left**" in c.lower()][0]

    @property
    def is_right_col(self) -> List[str]:
        return [c for c in self.results.columns if "**right**" in c.lower()][0]

    @property
    def is_intersection_col(self) -> List[str]:
        return [c for c in self.results.columns if "**both**" in c.lower()][0]

    @property
    def validation_columns(self) -> List[str]:
        return [
            c for c in self.results.columns if "~*validation*~" in c.lower()
        ]

    @property
    def left_columns(self) -> List[str]:
        return [c for c in self.results.columns if " ~%s~" % self.left in c]

    @property
    def right_columns(self) -> List[str]:
        return [c for c in self.results.columns if " ~%s~" % self.right in c]

    def get_results_union(self) -> pl.LazyFrame:
        return self.results

    def get_results_left(self) -> pl.LazyFrame:
        return self.results.filter(pl.col(self.is_left_col))

    def get_results_right(self) -> pl.LazyFrame:
        return self.results.filter(pl.col(self.is_right_col))

    def get_results_intersection(self) -> pl.LazyFrame:
        return self.results.filter(pl.col(self.is_intersection_col))

    def get_results_disjoint(self) -> pl.LazyFrame:
        return self.results.filter(pl.col(self.is_intersection_col).is_not())

    def get_rows_left_only(self) -> pl.LazyFrame:
        return (
            self.results.filter(
                pl.col(self.is_left_col) & pl.col(self.is_right_col).is_not()
            )
            .select(self.columns_indexes + self.left_columns)
            .rename(
                {
                    o: n
                    for o, n in zip(
                        self.left_columns,
                        [
                            c.replace(" ~%s~" % self.left, "")
                            for c in self.left_columns
                        ],
                    )
                }
            )
        )

    def get_rows_right_only(self) -> pl.LazyFrame:
        return (
            self.results.filter(
                pl.col(self.is_right_col) & pl.col(self.is_left_col).is_not()
            )
            .select(self.columns_indexes + self.right_columns)
            .rename(
                {
                    o: n
                    for o, n in zip(
                        self.right_columns,
                        [
                            c.replace(" ~%s~" % self.right, "")
                            for c in self.right_columns
                        ],
                    )
                }
            )
        )


@dataclass
class TableReconciliationSummarizationData:
    n_tested_rows: int
    n_tested_cols: int
    n_tested_entries: int

    n_tested_entries_passed: int
    n_tested_entries_failed: int

    n_tested_rows_passed: int
    n_tested_rows_passed_partially: int
    n_tested_rows_failed: int

    validation_ratio_entries: float
    validation_ratio_rows: float

    stats_invalidations_per_row_avg: float
    stats_invalidations_per_row_std: float

    n_total_rows_left: int
    n_total_rows_right: int
    n_total_rows_intersecting: int
    n_total_rows_union: int

    pass_ratio: float = 1

    @property
    def PASS(self):
        if self.n_tested_entries > 0:
            return (
                self.n_tested_entries_passed / self.n_tested_entries
                > self.pass_ratio
            )
        return False

    @property
    def flag(self):
        if self.PASS:
            return "PASSED"
        return "FAILED"

    def to_dict(self):
        return dict(
            TestedMeta=dict(
                flag=self.flag,
                passed=self.PASS,
                tested_rows=self.n_tested_rows,
                tested_cols=self.n_tested_cols,
                tested_entries=self.n_tested_entries,
            ),
            Totals=dict(
                n_rows_left=self.n_total_rows_left,
                n_rows_right=self.n_total_rows_right,
                n_rows_intersecting=self.n_total_rows_intersecting,
                n_rows_union=self.n_total_rows_union,
            ),
            TestedRows=dict(
                n_tested_rows_passed=self.n_tested_rows_passed,
                n_tested_rows_passed_partially=self.n_tested_rows_passed_partially,
                n_tested_rows_failed=self.n_tested_rows_failed,
            ),
            TestedEntries=dict(
                n_tested_entries_passed=self.n_tested_entries_passed,
                n_tested_entries_failed=self.n_tested_entries_failed,
            ),
            Validations=dict(
                pass_ratio=self.pass_ratio,
                validation_ratio_entries=self.validation_ratio_entries,
                validation_ratio_rows=self.validation_ratio_rows,
            ),
            RowStats=dict(
                stats_invalidations_per_row_avg=self.stats_invalidations_per_row_avg,
                stats_invalidations_per_row_std=self.stats_invalidations_per_row_std,
            ),
        )

    def get_string(self, sort_dicts=False, width=25, compact=True):
        return pformat(
            self.to_dict(), sort_dicts=sort_dicts, width=width, compact=compact
        )
