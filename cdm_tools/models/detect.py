import re
from pyspark.sql import DataFrame, functions as F, types


import calendar
import itertools


def generate_date_formats(
    date_formats: tuple[str],
    day_components: tuple[str],
    month_components: tuple[str],
    year_components: tuple[str],
    sep_components: tuple[str],
) -> tuple[str]:
    """Generate collection of possible date formats"""
    format_combinations = itertools.product(
        day_components, month_components, year_components
    )
    return list(
        df.format(day=day, month=month, year=year).replace(".", sep)
        for df in date_formats
        for day, month, year in format_combinations
        for sep in sep_components
    )


day_components = ("\d{1,2}",)
month_components = (
    *list(mn for mn in calendar.month_name if mn != ""),
    *list(ma for ma in calendar.month_abbr if ma != ""),
    "\d{1,2}",
)
year_components = ("\d{2}", "\d{4}")
sep_components = ("/", ".", "-")


date_formats = (
    "{month}.{day}.{year}",
    "{year}.{month}.{day}",
    "{day}.{month}.{year}",
    "{year}.{day}.{month}",
    "{day}.{year}.{month}",
    "{month}.{year}.{day}",
)

timestamp_formats = (
    "'T'HH:mm:ss aa",
    "'T'HH:mm:ss",
    "'T'HH:mm aa",
    "'T'HH:mm" "HH:mm:ss aa",
    "HH:mm:ss",
    "HH:mm aa",
    "HH:mm",
    "'T'hh:mm:ss aa",
    "'T'hh:mm:ss",
    "'T'hh:mm aa",
    "'T'hh:mm" "hh:mm:ss aa",
    "hh:mm:ss",
    "hh:mm aa",
    "hh:mm",
)


DATE_FORMATS = generate_date_formats(
    date_formats, day_components, month_components, year_components, sep_components
)

TIMESTAMP_FORMATS = [f"{df} {tf}" for df in DATE_FORMATS for tf in timestamp_formats]


# Define multiple regex patterns for each type
DECIMAL_REGEXES = [
    r"^\$?\-?([1-9]{1}[0-9]{0,2}(\,\d{3})*(\.\d{0,2})?|[1-9]{1}\d{0,}(\.\d{0,2})?|0(\.\d{0,2})?|(\.\d{1,2}))$|^\-?\$?([1-9]{1}\d{0,2}(\,\d{3})*(\.\d{0,2})?|[1-9]{1}\d{0,}(\.\d{0,2})?|0(\.\d{0,2})?|(\.\d{1,2}))$|^\(\$?([1-9]{1}\d{0,2}(\,\d{3})*(\.\d{0,2})?|[1-9]{1}\d{0,}(\.\d{0,2})?|0(\.\d{0,2})?|(\.\d{1,2}))\)$"
]
INTEGER_REGEXES = [r"^-?\d+$"]
DATE_REGEXES = DATE_FORMATS
DATETIME_REGEXES = TIMESTAMP_FORMATS
BOOLEAN_REGEXES = [r"^(true|false|1|0)$"]


def match_any_regex(column, regexes):
    return F.col(column).rlike("|".join(regexes))


def cdm_detect(
    data: DataFrame, sample_fraction: float = 0.2, dtype_threshold: float = 0.9
) -> dict:
    data = data.sample(fraction=sample_fraction).select(
        [F.col(c).cast(types.StringType()).alias(c) for c in data.columns]
    )
    data_count = data.count()

    data_metadata = {}

    for column in data.columns:
        column_detected = False

        temp = data.filter(F.col(column).isNotNull())
        if (temp.count() / data_count) <= dtype_threshold:
            data_metadata[column] = types.NullType()
            column_detected = True

        # Detect if boolean column
        if not column_detected:
            temp = data.filter(match_any_regex(column, BOOLEAN_REGEXES))
            if (temp.count() / data_count) >= dtype_threshold:
                data_metadata[column] = types.BooleanType()
                column_detected = True

        # Detect if datetime column
        if not column_detected:
            temp = data.filter(match_any_regex(column, DATETIME_REGEXES))
            if (temp.count() / data_count) >= dtype_threshold:
                data_metadata[column] = types.TimestampType()
                column_detected = True

        # Detect if date column
        if not column_detected:
            temp = data.filter(match_any_regex(column, DATE_REGEXES))
            if (temp.count() / data_count) >= dtype_threshold:
                data_metadata[column] = types.DateType()
                column_detected = True

        # Detect if integer column
        if not column_detected:
            temp = data.filter(match_any_regex(column, INTEGER_REGEXES))
            if (temp.count() / data_count) >= dtype_threshold:
                data_metadata[column] = types.IntegerType()
                column_detected = True

        # Detect if decimal column
        if not column_detected:
            temp = data.filter(match_any_regex(column, DECIMAL_REGEXES))
            if (temp.count() / data_count) >= dtype_threshold:
                data_metadata[column] = types.DecimalType(38, 6)
                column_detected = True

        # Default to string if no type detected
        if not column_detected:
            data_metadata[column] = types.StringType()

    return data_metadata
