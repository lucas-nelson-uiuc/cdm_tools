import typing
import functools

from pyspark.sql import types as T, functions as F

from .common import CommonDataModel
from .types import PYDANTIC_TYPES, DATE_FORMATS, TIMESTAMP_FORMATS


def extract_field_type(annotation) -> T.DataType:
    """
    Convert base Python type to PySpark DataType.
    If the annotation follows Optional[T], perform preliminary step to
    parse T prior to mapping.
    """
    if typing.get_origin(annotation):
        annotation = typing.get_args(annotation)[0]
    return PYDANTIC_TYPES.get(annotation, T.NullType())


def cdm_transform(model: CommonDataModel):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            all_transformations = dict()
            for field, field_info in model.model_fields.items():
                # gather, extract metadata
                all_transformations[field] = dict()
                field_name = field_info.alias or field
                field_type = extract_field_type(annotation=field_info.annotation)

                if field_name in data.columns:
                    # rename columns if alias provided
                    if field_name != field:
                        all_transformations[field][
                            "rename"
                        ] = f"{field_name} to {field}"
                        data = data.withColumn(field, F.col(field_name))

                    # cast to specified data type; attempt multiple formats for dates, timestamps
                    if not isinstance(field_type, T.StringType):
                        all_transformations[field][
                            "cast"
                        ] = f"{field} as {field_type.__class__.__name__}"
                    if isinstance(field_type, T.DateType):
                        if field_info.json_schema_extra:
                            date_format = field_info.json_schema_extra.get(
                                "date_format", "MM/dd/yyyy"
                            )
                            formatted_dates = [F.to_date(F.col(field), date_format)]
                        else:
                            formatted_dates = [
                                F.to_date(F.col(field), fmt) for fmt in DATE_FORMATS
                            ]
                        data = data.withColumn(field, F.coalesce(*formatted_dates))
                    elif isinstance(field_type, T.TimestampType):
                        formatted_timestamps = [
                            F.to_timestamp(F.col(field), fmt)
                            for fmt in TIMESTAMP_FORMATS
                        ]
                        data = data.withColumn(
                            field,
                            F.date_format(
                                F.coalesce(*formatted_timestamps), "MM/dd/yyyy HH:mm:ss"
                            ),
                        )
                        # data = data.withColumn(field, date_format(F.col(field).cast(field_type), "MM/dd/yyyy HH:mm:ss"))
                    elif isinstance(field_type, T.DecimalType):
                        data = data.withColumn(
                            field,
                            F.regexp_replace(
                                F.regexp_replace(F.col(field), "\(", "-"),
                                "[^-\d.eE]+",
                                "",
                            ).cast(field_type),
                        )
                    else:
                        data = data.withColumn(field, F.col(field).cast(field_type))

                # mutate field according to default value, if provided
                if field_info.default_factory:
                    data = data.withColumn(field, field_info.default_factory("x"))
                    all_transformations[field][
                        "mutate"
                    ] = f"default assigned as {field_info.default_factory}"

                # mutate fields with null values
                if field_name not in data.columns:
                    all_transformations[field]["mutate"] = "default assigned as NULL"
                    # assert isinstance(field_type, T.NullType), f"Missing mapping for required field: {field_name}"
                    data = data.withColumn(field_name, F.lit(None).cast(T.StringType()))

            for field, transformation in all_transformations.items():
                if transformation:
                    print(f">>> Transformed {field}")
                    for operation, message in transformation.items():
                        print(f"\t[{operation.title()}] {message}")

            return data.select(*[field for field in model.model_fields.keys()])

        return wrapper

    return decorator
