import datetime
import decimal

from pyspark.sql import types


PYDANTIC_TYPES = {
    str: types.StringType(),
    int: types.IntegerType(),
    datetime.date: types.DateType(),
    datetime.datetime: types.TimestampType(),
    decimal.Decimal: types.DecimalType(38, 6),
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

TIMESTAMP_FORMATS = (f"{date_fmt} HH:mm:ss" for date_fmt in DATE_FORMATS)
