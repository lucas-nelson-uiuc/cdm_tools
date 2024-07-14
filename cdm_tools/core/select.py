from typing import Optional, Union, Iterable
import re
import functools

from pyspark.sql import types, DataFrame


def _select_name(data_schema: types.StructType, columns: list[str]) -> list[str]:
    """Select columns from schema by name"""
    return filter(lambda column: column.name in columns, data_schema)


def _select_regex(data_schema: types.StructType, pattern: str) -> list[str]:
    """Select columns from schema by regular expression"""
    pattern = re.compile(pattern)
    return filter(lambda column: pattern.search(column.name), data_schema)


def _select_dtype(data_schema: types.StructType, dtype: types.DataType) -> list[str]:
    """Select columns from schema by data type"""
    return filter(lambda column: isinstance(column.dataType, dtype), data_schema)


def _select_custom(data_schema: types.StructType, predicate: callable) -> list[str]:
    """Select columns from schema by user-defined predicate function."""
    return filter(predicate, data_schema)


def select_columns(
    data: DataFrame,
    columns: Optional[list[str]] = None,
    pattern: Optional[Union[str, list[str]]] = None,
    dtype: Optional[Union[types.DataType, list[types.DataType]]] = None,
    predicate: Optional[callable] = None,
    strict: bool = False,
    exclude: bool = False,
) -> DataFrame:
    """Select columns from the DataFrame based on specified predicates.

    Parameters
    ==========
    data: DataFrame
        The input DataFrame from which columns will be selected.
    columns: Optional[list[str]]
        List of column names to select.
    pattern: Optional[Union[str, list[str]]]
        Regular expression pattern to match column names.
    dtype Optional[Union[types.DataType, list[types.DataType]]]
        Data type to filter columns by.
    predicate: Optional[callable]
        Function to filter columns by.
    strict: bool
        If True, select columns that satisfy all predicates (intersection).
        If False, select columns that satisfy any predicate (union).

    Returns
    =======
    DataFrame
        DataFrame containing only the selected columns.

    Raises
    ======
    ValueError
        If none of 'columns', 'pattern', 'dtype', or 'predicate' is provided.

    Notes
    =====
        - At least one of 'columns', 'pattern', 'dtype', or 'predicate' must be provided.
        - If 'strict' is True, only columns that satisfy all provided predicates are selected.
        - If 'strict' is False, columns that satisfy any of the provided predicates are selected.
    """
    data_schema = data.schema
    queued_columns = list()

    if columns:
        queued_columns.append(_select_name(data_schema, columns=columns))
    if pattern:
        if isinstance(pattern, Iterable) and not isinstance(pattern, str):
            pattern = f"({'|'.join(pattern)})"
        queued_columns.append(_select_regex(data_schema, pattern=pattern))
    if dtype:
        queued_columns.append(_select_dtype(data_schema, dtype=dtype))
    if predicate:
        queued_columns.append(_select_custom(data_schema, predicate=predicate))

    if not queued_columns:
        raise ValueError(
            "Please revise the values passed for 'columns', 'pattern', or 'dtype'."
        )

    selected_columns = functools.reduce(
        set.intersection if strict else set.union, map(set, queued_columns)
    )
    return data.select(*list(column.name for column in selected_columns))
