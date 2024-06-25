import datetime
import decimal

from pyspark.sql import types


PYDANTIC_TYPES = {
    str: types.StringType(),
    int: types.IntegerType(),
    datetime.date: types.DateType(),
    decimal.Decimal: types.DecimalType(38, 6),
}
