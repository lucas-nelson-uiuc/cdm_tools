from typing import Iterable

from pyspark.sql import types, functions as F, DataFrame, Column
import pyspark.pandas as ps

from .select import select_columns


def summarize_data(
    data: DataFrame,
    columns: list[str] = None,
    pattern: list[str] = None,
    dtype: types.DataType = None,
    strict: bool = False,
    include_types: bool = True,
) -> DataFrame:
    """Summarize a DataFrame with generic and type-specific aggregation functions.

    Examples
    --------
    >>> # generic summary on all columns in a DataFrame
    >>> summarize_data(data=demo_data)

    >>> # generic summary on specific columns in a DataFrame
    >>> summarize_data(data=demo_data, columns=["date_effective", "ttype", "journal_number"])

    >>> # type-specific summaries on columns of passed type(s) in a DataFrame
    >>> summarize_data(data=demo_data, dtype=types.StringType).show()
    >>> summarize_data(data=demo_data, dtype=(types.IntegerType, types.LongType)).show()
    >>> summarize_data(data=demo_data, dtype=types.DateType).show()

    >>> # note: mixed types (e.g. string, numeric) will return a generic summary
    >>> summarize_data(data=demo_data, dtype=(types.StringType, types.LongType)).show()
    """

    def generic_queue(data: DataFrame) -> tuple[Column]:
        """Universal aggregation functions"""
        columns = data.columns
        return (
            *[F.count(column).alias(f"count_{column}") for column in columns],
            *[
                F.count_distinct(column).alias(f"distinct_{column}")
                for column in columns
            ],
            *[
                F.sum(F.isnull(column).cast(types.IntegerType())).alias(
                    f"nulls_{column}"
                )
                for column in columns
            ],
        )

    def string_queue(data: DataFrame) -> tuple[Column]:
        """String-specific aggregation functions"""
        columns = data.columns
        return (
            *generic_queue(data=data),
            *[F.mode(column).alias(f"mode_{column}") for column in columns],
        )

    def numeric_queue(data: DataFrame) -> tuple[Column]:
        """Numeric-specific aggregation functions"""
        columns = data.columns
        return (
            *generic_queue(data=data),
            *[F.sum(column).alias(f"sum_{column}") for column in columns],
            *[F.min(column).alias(f"min_{column}") for column in columns],
            *[F.mean(column).alias(f"mean_{column}") for column in columns],
            *[F.max(column).alias(f"max_{column}") for column in columns],
        )

    def date_queue(data: DataFrame) -> tuple[Column]:
        """Date-specific aggregation functions"""
        columns = data.columns
        return (
            *generic_queue(data=data),
            *[F.min(column).alias(f"min_{column}") for column in columns],
            *[F.max(column).alias(f"max_{column}") for column in columns],
        )

    def gather_queue(dtype: Iterable[types.DataType]) -> tuple[Column]:
        DTYPE_MAPPING = {
            (types.StringType, types.NullType): string_queue,
            (types.LongType, types.IntegerType, types.DecimalType): numeric_queue,
            (types.DateType, types.TimestampType): date_queue,
        }
        for dtypes, function in DTYPE_MAPPING.items():
            if all(dt in dtypes for dt in dtype):
                return function
        return generic_queue

    def generic_summarize(data: DataFrame, queue: tuple[Column] = None) -> DataFrame:
        """Summarize dataset given aggregation queue"""
        summary = data.agg(*queue).withColumn("index", F.lit(0))
        return summary.withColumns(
            {
                column: F.col(column).cast(types.StringType())
                for column in summary.columns
            }
        )

    def reshape_summary(data: DataFrame) -> DataFrame:
        """Melt, pivot data for easier reading"""
        return (
            data.melt(
                ids="index",
                values=data.columns,
                variableColumnName="variable",
                valueColumnName="value",
            )
            .withColumn("column", F.regexp_replace("variable", "^[a-zA-Z]+_", ""))
            .withColumn("statistic", F.regexp_extract("variable", "^([a-zA-Z]+)_", 1))
            .filter(~(F.col("statistic").isNull() | F.col("statistic").rlike("^\s*$")))
            .groupby("column")
            .pivot("statistic")
            .agg(F.first("value"))
            .sort("column")
        )

    def attach_typehints(data: DataFrame, summary: DataFrame) -> DataFrame:
        """Attach types to column for easier reading"""
        column_names = ["column", "dtype"]
        typehint = ps.DataFrame(
            [(field.name, field.dataType.__class__.__name__) for field in data.schema],
            columns=column_names,
        ).to_spark()
        column_names += [c for c in summary.columns if c not in column_names]
        return summary.join(typehint, on="column", how="left").select(column_names)

    # gather data based on column-selecting parameters
    if not any((columns, pattern, dtype)):
        columns = data.columns
    data = select_columns(
        data, columns=columns, pattern=pattern, dtype=dtype, strict=strict
    )

    # gather aggregation queue based on dtype(s) passed
    dtype = (dtype,) if not isinstance(dtype, Iterable) else tuple(dtype)
    queue = gather_queue(dtype=dtype)(data=data)

    # summarize, reshape, mutate data
    summary = reshape_summary(generic_summarize(data=data, queue=queue))
    if include_types:
        summary = attach_typehints(data=data, summary=summary)
    return summary
