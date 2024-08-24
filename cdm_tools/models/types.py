import datetime
import decimal

from pyspark.sql import types as T


PYDANTIC_TYPES = {
    str: T.StringType(),
    int: T.IntegerType(),
    datetime.date: T.DateType(),
    datetime.datetime: T.TimestampType(),
    decimal.Decimal: T.DecimalType(38, 6)
}

DATE_FORMATS = (
    "M/d/yyyy",
    "M/dd/yyyy",
    "MM/d/yyyy",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "MM-dd-yyyy",
    "dd-MM-yyyy",
)

TIMESTAMP_FORMATS = [f"{date_fmt} HH:mm:ss" for date_fmt in DATE_FORMATS]
