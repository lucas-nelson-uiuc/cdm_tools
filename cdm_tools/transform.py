import functools

from pyspark.sql import types, functions as F

from .types import PYDANTIC_TYPES, DATE_FORMATS, TIMESTAMP_FORMATS


def cdm_transform(model):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            for field, field_info in model.get_request_form().items():
                field_name = field_info.get("name")
                field_type = PYDANTIC_TYPES.get(
                    field_info.get("dtype"), types.NullType()
                )
                data = data.withColumnRenamed(field_name, field)
                if field_type == types.DateType():
                    formatted_dates = [
                        F.to_date(F.col(field), fmt) for fmt in DATE_FORMATS
                    ]
                    data = data.withColumn(field, F.coalesce(*formatted_dates))
                elif field_type == types.TimestampType():
                    formatted_timestamps = [
                        F.to_timestamp(F.col(field), fmt) for fmt in TIMESTAMP_FORMATS
                    ]
                    data = data.withColumn(field, F.coalesce(*formatted_timestamps))
                else:
                    data = data.withColumn(field, F.col(field).cast(field_type))
            return data

        return wrapper

    return decorator
