import functools

from pyspark.sql import types, functions as F

from .types import PYDANTIC_TYPES, DATE_FORMATS, TIMESTAMP_FORMATS


def cdm_transform(model):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            all_transformations = dict()
            for field, field_info in model.model_fields.items():
                # rename column to specified name
                all_transformations[field] = dict()
                field_name = field_info.alias or field
                if field_name in data.columns:
                    if field_name != field:
                        all_transformations[field]["rename"] = (
                            f"{field_name} to {field}"
                        )
                        data = data.withColumn(field, F.col(field_name))

                    # cast to specified data type; attempt multiple formats for dates, timestamps
                    field_type = PYDANTIC_TYPES.get(
                        field_info.annotation, types.NullType()
                    )
                    if field_type != types.StringType():
                        all_transformations[field]["cast"] = (
                            f"{field} as {field_type.__class__.__name__}"
                        )
                    if field_type == types.DateType():
                        formatted_dates = [
                            F.to_date(F.col(field), fmt) for fmt in DATE_FORMATS
                        ]
                        data = data.withColumn(field, F.coalesce(*formatted_dates))
                    elif field_type == types.TimestampType():
                        formatted_timestamps = [
                            F.to_timestamp(F.col(field), fmt)
                            for fmt in TIMESTAMP_FORMATS
                        ]
                        data = data.withColumn(field, F.coalesce(*formatted_timestamps))
                    else:
                        data = data.withColumn(field, F.col(field).cast(field_type))

                if field_name not in data.columns:
                    data = data.withColumn(
                        field_name, F.lit(None).cast(types.StringType())
                    )

                # mutate field according to default value, if provided
                if field_info.default_factory:
                    data = data.withColumn(field, field_info.default_factory("x"))
                    all_transformations[field]["mutate"] = (
                        "default assigned as <operation-here>"
                    )

            for field, transformation in all_transformations.items():
                if transformation:
                    print(f">>> Transformed {field}")
                    for operation, message in transformation.items():
                        print(f"\t[{operation.title()}] {message}")

            return data.select(*[field for field in model.model_fields.keys()])

        return wrapper

    return decorator
