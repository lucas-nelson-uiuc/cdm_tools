import functools

from pyspark import types, functions as F

from cdm_tools.types import PYDANTIC_TYPES


def cdm_transform(model):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            request_form = model.get_request_form()
            for field in request_form:
                field_info = request_form.get(field)
                field_name = field_info.get("name")
                field_type = PYDANTIC_TYPES.get(
                    field_info.get("dtype"), types.NullType()
                )
                data = data.withColumnRenamed(field_name, field).withColumn(
                    field, F.col(field).cast(field_type)
                )
            return data

        return wrapper

    return decorator
