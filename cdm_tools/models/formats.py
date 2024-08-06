from typing import Optional
import itertools
import calendar

from pyspark.sql import types as T


DATE_FORMATS = (
    "{year}.{month}.{day}",
    "{month}.{day}.{year}",
    "{day}.{month}.{year}",
    "{year}.{day}.{month}",
    "{month}.{year}.{day}",
    "{day}.{year}.{month}",
)
PYSPARK_TIMESTAMP_FORMATS = (
    "T\d{1,2}:\d{2}:\d{2} [AP]M",
    "T\d{1,2}:\d{2}:\d{2} [ap]m",
    "T\d{1,2}:\d{2} [AP]M",
    "T\d{1,2}:\d{2} [ap]m",
    "T\d{1,2}:\d{2}:\d{2}",
    "T\d{1,2}:\d{2}:\d{2}",
    "T\d{1,2}:\d{2}",
    "T\d{1,2}:\d{2}",
    "\d{1,2}:\d{2}:\d{2} [AP]M",
    "\d{1,2}:\d{2}:\d{2} [ap]m",
    "\d{1,2}:\d{2} [AP]M",
    "\d{1,2}:\d{2} [ap]m",
    "\d{1,2}:\d{2}:\d{2}",
    "\d{1,2}:\d{2}:\d{2}",
    "\d{1,2}:\d{2}",
    "\d{1,2}:\d{2}",
)
PYSPARK_DAY_COMPONENTS = ("\d{1,2}",)
PYSPARK_MONTH_COMPONENTS = (
    "\d{1,2}",
    *list(mn for mn in calendar.month_name if mn != ""),
    *list(ma for ma in calendar.month_abbr if ma != ""),
)
PYPSARK_YEAR_COMPONENTS = ("\d{2}", "\d{4}")
PYSPARK_SEP_COMPONENTS = ("/", ".", "-")


def generate_date_formats(
    date_formats: tuple[str] = DATE_FORMATS,
    day_components: tuple[str] = PYSPARK_DAY_COMPONENTS,
    month_components: tuple[str] = PYSPARK_MONTH_COMPONENTS,
    year_components: tuple[str] = PYPSARK_YEAR_COMPONENTS,
    sep_components: tuple[str] = PYSPARK_SEP_COMPONENTS,
    timestamp_formats: Optional[tuple[str]] = None,
) -> set[str]:
    """Generate collection of possible date formats"""
    all_formats = set(
        [
            f"{df}".format(day=day, month=month, year=year).replace(".", sep)
            for df in date_formats
            for day, month, year in itertools.product(
                day_components, month_components, year_components
            )
            for sep in sep_components
        ]
    )
    if timestamp_formats:
        return [f"{df} {tf}" for df in all_formats for tf in timestamp_formats]
    return all_formats


# Define multiple regex patterns for each type
BOOLEAN_REGEXES = [r"^(true|false|yes|no)$"]
INTEGER_REGEXES = [r"^-?\d+$"]
DECIMAL_REGEXES = [
    r"^\$?\-?([1-9]{1}[0-9]{0,2}(\,\d{3})*(\.\d{0,5})?|[1-9]{1}\d{0,}(\.\d{0,5})?|0(\.\d{0,5})?|(\.\d{1,5}))$|^\-?\$?([1-9]{1}\d{0,2}(\,\d{3})*(\.\d{0,5})?|[1-9]{1}\d{0,}(\.\d{0,5})?|0(\.\d{0,5})?|(\.\d{1,5}))$|^\(\$?([1-9]{1}\d{0,2}(\,\d{3})*(\.\d{0,5})?|[1-9]{1}\d{0,}(\.\d{0,5})?|0(\.\d{0,5})?|(\.\d{1,5}))\)$"
]
DATE_REGEXES = generate_date_formats()
TIMESTAMP_REGEXES = generate_date_formats(timestamp_formats=PYSPARK_TIMESTAMP_FORMATS)


DTYPE_REGEX_PATTERNS = {
    T.BooleanType(): BOOLEAN_REGEXES,
    T.IntegerType(): INTEGER_REGEXES,
    T.DecimalType(38, 6): DECIMAL_REGEXES,
    T.TimestampType(): TIMESTAMP_REGEXES,
    T.DateType(): DATE_REGEXES,
}
