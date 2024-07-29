from typing import Optional
import re
import difflib
import itertools
import collections

from pyspark.sql import Column, DataFrame, functions as F, types as T

from cdm_tools.models.formats import DTYPE_REGEX_PATTERNS


def match_any_regex(column: str, regexes: list[str]) -> Column:
    """Create a SQL expression to check if any regex matches the column value."""
    return F.col(column).rlike("|".join(regexes))


def format_schema(
    schema: dict[str, T.DataType],
    max_len: Optional[int] = 0,
    default_max_len: Optional[int] = 16,
) -> list[str]:
    """
    Format a schema dictionary into a list of string representations.

    Parameters
    ----------
    schema : dict[str, T.DataType]
        Schema dictionary where keys are column names and values are data T.
    max_len : int, optional
        Maximum length for padding column names, by default 0.
    default_max_len : int, optional
        Default maximum length if max_len is not provided, by default 16.

    Returns
    -------
    list[str]
        List of formatted schema strings.
    """
    max_len = max(max_len, default_max_len)
    return [f"{key:<{max_len}}\t{value}" for key, value in schema.items()]


def compare_schema(*schema: tuple[dict]) -> list[list[str]]:
    """
    Compare multiple schemas and return the differences.

    Parameters
    ----------
    schema : tuple[dict]
        Tuple of schema dictionaries to compare.

    Returns
    -------
    list[list[str]]
        List of lists containing differences between schemas.
    """
    max_len = max(map(lambda s: max(map(len, s.keys())), schema))
    pairs = itertools.pairwise(schema)
    return [
        list(
            difflib.ndiff(
                format_schema(schema_observed, max_len),
                format_schema(schema_expected, max_len),
            )
        )
        for schema_observed, schema_expected in pairs
    ]


def assert_schema_equal(*schema: tuple[dict]) -> None:
    """
    Assert that multiple schemas are equal and print differences if they are not.

    Parameters
    ----------
    schema : tuple[dict]
        Tuple of schema dictionaries to compare.

    Raises
    ------
    AssertionError
        If schemas are not equal.
    """

    MISMATCH = re.compile("^[+-?]")
    DIFFERENCE = re.compile("\^")

    def is_difference(line: str) -> bool:
        return MISMATCH.search(line) or DIFFERENCE.search(line)

    def contains_difference(report: list[str]) -> bool:
        return any(map(is_difference, report))

    comparison = compare_schema(*schema)
    diff_schema = list(map(contains_difference, comparison))
    if diff_schema:
        for mismatch in itertools.compress(comparison, diff_schema):
            mismatch_lines = filter(is_difference, mismatch)
            print("[WARNING] Difference detected in schemas")
            print("\n".join(mismatch_lines) + "\n")


def cdm_detect(
    *data: tuple[DataFrame],
    sample_fraction: Optional[float] = 0.2,
    dtype_threshold: Optional[float] = 0.9,
    assert_equal: Optional[bool] = False,
    expected_schema: Optional[dict[str, T.DataType]] = None,
    _regex_patterns: Optional[dict[T.DataType, list[str]]] = DTYPE_REGEX_PATTERNS,
) -> dict:
    """
    Infer the schema for all DataFrame(s) passed. Using a randomly selected
    sample of the data, each column is matched against multiple regular expressions,
    returning the "best guess" based on the threshold provided.

    If multiple DataFrames are passed, `assert_equal` will check if all schemas
    are identical. Else, a validation report will be printed showing differences
    between the unique schemas.

    If `expected_schema` is passed, all unique schemas will be compared against
    the expectation. All differences, if any, will be displayed in the same
    validation report generated by `assert_equal = True`.

    Parameters
    ----------
    data : tuple[DataFrame]
        DataFrames to infer schema from.
    sample_fraction : float, optional
        Fraction of the data to sample for inference, by default 0.2.
    dtype_threshold : float, optional
        Threshold for dtype matching, by default 0.9.
    assert_equal : bool, optional
        If True, assert that all DataFrame schemas are equal, by default False.
    expected_schema : dict[str, T.DataType], optional
        Expected schema to compare against, by default None.
    _regex_patterns : dict[T.DataType, list[str]], optional
        Dictionary of regex patterns for data type matching, by default REGEX_PATTERNS.

    Returns
    -------
    dict[str, T.DataType]
        Mapping of all DataFrame(s) columns to their inferred data type.
    """

    def detect_schema(data: DataFrame) -> dict[str, T.DataType]:
        """
        Return inferred schema of DataFrame as dictionary.

        Parameters
        ----------
        data : DataFrame
            DataFrame to infer schema from.

        Returns
        -------
        dict[str, T.DataType]
            Inferred schema dictionary.
        """

        data = data.sample(fraction=sample_fraction).select(
            [F.col(c).cast(T.StringType()).alias(c) for c in data.columns]
        )
        data_count = data.count()

        def check_threshold(
            matches: int, total: int = data_count, threshold: float = dtype_threshold
        ) -> bool:
            """
            Check if proportion of matches meets or exceeds threshold.

            Parameters
            ----------
            matches : int
                Number of matches.
            total : int, optional
                Total number of rows, by default data_count.
            threshold : float, optional
                Threshold for matching, by default dtype_threshold.

            Returns
            -------
            bool
                True if matches meet or exceed threshold, else False.
            """
            return (matches / total) >= threshold

        def match_dtype(column: str) -> T.DataType:
            """
            Infer data type of column given regular expression matching.

            Parameters
            ----------
            column : str
                Column name.

            Returns
            -------
            T.DataType
                Inferred data type.
            """
            is_null = data.filter(F.col(column).isNull())
            if check_threshold(is_null.count()):
                return T.NullType()

            for dtype, regex in _regex_patterns.items():
                temp = data.filter(match_any_regex(column, regex))
                if check_threshold(temp.count()):
                    return dtype

            return T.StringType()

        return {column: match_dtype(column) for column in data.columns}

    def group_schemas(
        detected_schemas: tuple[dict[str, T.DataType]],
    ) -> dict[tuple[str, T.DataType], list[int]]:
        """
        Group together identical schemas and store indices.

        Parameters
        ----------
        detected_schemas : tuple[dict[str, T.DataType]]
            Tuple of detected schema dictionaries.

        Returns
        -------
        dict[tuple[str, T.DataType], list[int]]
            Dictionary grouping identical schemas with indices.
        """
        schema_groups = collections.defaultdict(list)
        for index, schema in enumerate(detected_schemas):
            schema_tuple = tuple(schema.items())
            schema_groups[schema_tuple].append(index)
        return schema_groups

    def display_schema_differences(
        schema_groups: dict, expected_schema: Optional[dict] = None
    ) -> None:
        """
        Display all unique schemas detected across all passed DataFrame(s).

        If `expected_schema` is passed, compare it to all unique schemas and
        report differences. Else, show all schemas without comparisons.

        Parameters
        ----------
        schema_groups : dict
            Dictionary of schema groups.
        expected_schema : dict, optional
            Expected schema to compare against, by default None.
        """
        unique_schemas = list(schema_groups.keys())
        if len(unique_schemas) > 1:
            print("[ERROR] Detected different schemas across DataFrames.")
            for i, (schema, indices) in enumerate(schema_groups.items()):
                schema = dict(schema)
                print("_" * 50)
                print(f"\nSchema {i + 1} -> DataFrames: {indices}")
                print("_" * 50)
                for field in format_schema(schema=schema):
                    print(field)
                if expected_schema:
                    print()
                    assert_schema_equal(schema, expected_schema)

    detected_schema = tuple(map(detect_schema, data))
    if assert_equal:
        schema_groups = group_schemas(detected_schema)
        display_schema_differences(schema_groups, expected_schema=expected_schema)

    return detected_schema
