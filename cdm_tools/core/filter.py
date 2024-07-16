from typing import Iterable, Any

import operator
import functools

from pyspark.sql import functions as F, DataFrame


def _filter_predicate(
    columns: list[str], predicate: callable, strict: bool = True, negate: bool = False
) -> callable:
    """Apply predicate to all columns and return boolean expression"""
    comparison_operator = operator.and_ if strict else operator.or_
    query = functools.reduce(comparison_operator, map(predicate, map(F.col, columns)))
    return operator.neg(query) if negate else query


def filter_nulls(data: DataFrame, columns: list[str], **predicate_kwargs) -> DataFrame:
    """Remove all null/whitespace entries from a DataFrame"""
    predicate = lambda column: column.isNull() | column.rlike("^\s*$")
    query = _filter_predicate(columns=columns, predicate=predicate, **predicate_kwargs)
    return data.filter(~query)


def filter_regex(
    data: DataFrame, columns: list[str], pattern: str, **predicate_kwargs
) -> DataFrame:
    """Keep all observations that match a regular expression"""
    predicate = lambda column: column.rlike(pattern)
    query = _filter_predicate(columns=columns, predicate=predicate, **predicate_kwargs)
    return data.filter(query)


def filter_substring(
    data: DataFrame, columns: list[str], substring: str, **predicate_kwargs
) -> DataFrame:
    """Keep all observations that contain a substring"""
    predicate = lambda column: column.contains(substring)
    query = _filter_predicate(columns=columns, predicate=predicate, **predicate_kwargs)
    return data.filter(query)


def filter_range(
    data: DataFrame,
    columns: list[str],
    range: Iterable[Any],
    inclusive: bool = True,
    **predicate_kwargs,
) -> DataFrame:
    """Keep all observations that exist in a range."""

    assert all(
        type(elem) == type(range[0]) for elem in range
    ), "Elements must be same type."
    assert (
        len(type) == 2
    ), "Must provide exactly two elements (sorted smallest to largest)"

    lower_bound, upper_bound = sorted(range)
    lower_func, upper_func = (
        (operator.ge, operator.le) if inclusive else (operator.gt, operator.lt)
    )
    predicate = lambda column: lower_func(column, lower_bound) & upper_func(
        column, upper_bound
    )
    query = _filter_predicate(columns=columns, predicate=predicate, **predicate_kwargs)
    return data.filter(query)


def filter_custom(
    data: DataFrame, columns: list[str], predicate: callable, **predicate_kwargs
) -> DataFrame:
    """Keep all observations that meet a user-defined predicate function."""
    query = _filter_predicate(columns=columns, predicate=predicate, **predicate_kwargs)
    return data.filter(query)
