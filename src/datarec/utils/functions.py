import re
import polars as pl
from typing import Iterable, Callable, Tuple, List
from collections import OrderedDict


def get_lazyframe_length(lf: pl.LazyFrame) -> int:
    try:
        return lf.with_row_count().select("row_nr").last().collect().item() + 1
    except ValueError:
        return 0


def get_prefixes(suffixed_iterable: Iterable[str], regex: str) -> List[str]:
    """Extracts the unique prefixes from a given iterable of strings with a
    given regex.

    Args:
        suffixed_iterable (Iterable): An iterable of strings to extract prefixes
        from.

        regex (str): A regex string pattern to use for extracting the prefixes.

    Returns:
        list[str]: A list of unique prefixes extracted from the given iterable
        of strings.
    """

    found = set()

    def set_found(x):
        prefix = re.sub(regex, "", x)
        found.add(prefix)
        return prefix

    base_columns = [
        set_found(x)
        for x in suffixed_iterable
        if re.sub(regex, "", x) not in found
    ]

    return base_columns


def group_suffixed(
    suffixed_iterable: Iterable[str], regex: str = " ~.+?~"
) -> List[str]:
    """Groups a given iterable of strings by their prefixes with a given regex.

    Args:
        suffixed_iterable (Iterable): An iterable of strings to group by
        prefixes.

        regex (str, optional): A regex string pattern to use for extracting the
        prefixes. Defaults to " ~.+?~".

    Returns:
        list[str]: A list of strings with the given iterable of strings grouped
        by their prefixes.
    """

    base_columns = get_prefixes(suffixed_iterable, regex)

    grouped_column_names = OrderedDict()

    for key in base_columns:
        grouped_column_names[key] = []

    base_suffix_map = {c: re.sub(regex, "", c) for c in suffixed_iterable}

    for new, original in base_suffix_map.items():
        grouped_column_names[original].append(new)

    sorted_columns = []

    for cols in grouped_column_names.values():
        sorted_columns.extend(cols)

    return sorted_columns


def get_ordered_union(set_1: Iterable[str], set_2: Iterable[str]) -> List[str]:
    """Computes the ordered union of two given iterables of strings.

    Args:
        set_1 (Iterable): The first iterable of strings.

        set_2 (Iterable): The second iterable of strings.

    Returns:
        list[str]: A list of strings with the ordered union of the two given
        iterables of strings.
    """
    union_columns = set(set_1).union(set(set_2))
    return [col for col in set_1 if col in union_columns]


def get_formated_ordered_union(
    iter_1: Iterable[str],
    iter_2: Iterable[str],
    formatter: Callable = lambda x: x,
) -> List[str]:
    """Computes the formatted ordered union of two given iterables of objects.

    Args:
        iter_1 (Iterable): The first iterable of objects.

        iter_2 (Iterable): The second iterable of objects.

        formatter (Callable, optional): A callable object to format each object
        in the iterables (default identity function).

    Returns:
        list[str]: A list of objects with the formatted ordered union of the two
        given iterables of objects.
    """
    iter_1 = [formatter(c) for c in iter_1]
    iter_2 = [formatter(c) for c in iter_2]
    return get_ordered_union(iter_1, iter_2)


def validate_index_columns(
    df: pl.LazyFrame, index_of_columns: Iterable[int], title: str
) -> Tuple[int, int]:
    """Validates that the given columns in a DataFrame are unique.

    Args:
        df (pl.LazyFrame): A polars DataFrame to validate the columns of.

        index_of_columns (Iterable): An iterable of indices of columns in the
        DataFrame to validate.

        title (str): A string title of the DataFrame being validated.

    Returns:
        Tuple[int, int]: A tuple of integers with the number of unique columns
        and total number of rows in the DataFrame.
    """

    columns = [df.columns[c] for c in index_of_columns]

    repetitions_columns = (
        df.select([pl.col(c) for c in columns])
        .groupby([pl.col(c) for c in columns])
        .agg(pl.count().alias("group_repetitions"))
        .with_columns(pl.col("group_repetitions").map(lambda x: x == 1))
        .select("group_repetitions")
    ).collect()

    n_unique = repetitions_columns.sum().to_numpy().squeeze()
    n_rows = len(repetitions_columns)

    assert (
        n_unique == n_rows
    ), f"indexes must be unique. there are {n_unique} \
            unique columns out of a total {n_rows} for the {title} df"

    return n_unique, n_rows


def filter_iterable(_l: Iterable[object], c: Iterable[object]) -> List[str]:
    """Filters a given iterable of objects by excluding the elements at the
    given indices.

    Args:
        _l (Iterable): The iterable of objects to filter.

        c (Iterable): An iterable of indices to exclude from the filtered
        iterable.

    Returns:
        list[str]: A list of objects with the given iterable of objects filtered
        by excluding the elements at the given indices.
    """

    return [_l[i] for i in range(len(_l)) if i not in c]


def convert_iterable_to_list(iterable: Iterable[object]) -> List[object]:
    """Converts an iterable to a list of objects.

    Args:
        iterable (Iterable): The iterable of objects to convert to a list.

    Returns:
        list[object]: A list of objects with the given iterable converted to a
        list.
    """

    if isinstance(iterable, Iterable):
        iterable = list(iterable)
    else:
        iterable = list([iterable])
    return iterable
