import re
import functools
import itertools

from typing import Iterable

import pyspark
from pyspark.sql import DataFrame


def cp_read(
    filepath: str,
    pattern: str = ".*",
    recursive: bool = False,
    fs_func: callable = None,
    read_func: callable = None,
    union_func: callable = DataFrame.unionByName,
) -> pyspark.sql.DataFrame:
    pattern = re.compile(pattern)
    files = filter(
        lambda fp: isinstance(pattern.match(fp), re.Match),
        fs_func(filepath, recursive=recursive),
    )
    return functools.reduce(union_func, map(read_func, files))


def cp_read_fwf(
    filepath: str,
    column_mapping: Iterable[tuple[str, int]],
    column_extract: str = "_c0",
    drop_extract: bool = True,
) -> DataFrame:
    """
    Iteratively extract data from `column_extract` using the column names and positions
    provided in the `column_mapping` sequence.

    Example
    -------
    >>> column_mapping = [
        ("column1", 1),         # creates `column1` as substring 1:4
        ("column2", 5),         # creates `column2` as substring 5:14
        ("column3", 15),        # creates `column3` as substring 15:...
        # ... additional column-index pairs ...
        ("columnN", 100),       # creates `columnN` as substring 100:200
        ("", 200)               # column will not be created since there is no next pair
    ]

    >>> data = cp__read_fwf(filepath="path/to/fwf.txt", column_mapping=column_mapping)
    >>> # ... good idea to run `clean_df(data)` to remove extra spaces ...
    """
    ERROR_MESSAGE_TYPES = "Please revise your column_mapping. Each pair must be of type (str, int). See example for more details."
    assert all(
        isinstance(name, str) and isinstance(index, int)
        for name, index in column_mapping
    ), ERROR_MESSAGE_TYPES

    ERROR_MESSAGE_ORDER = "Please revise your column mapping. The starting index of all pairs must be in ascending order."
    assert column_mapping == sorted(
        column_mapping, key=lambda pair: pair[1]
    ), ERROR_MESSAGE_ORDER

    return (
        cp.read(filepath)
        .withColumns(
            {
                column: F.substring(column_extract, pos=start, len=end - start)
                for (column, start), (_, end) in itertools.pairwise(column_mapping)
            }
        )
        .drop(column_extract if drop_extract else "")
    )
