import functools
import operator

from pyspark.sql import functions as F


def cdm_validate(model):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            all_fields = dict()
            for field, field_info in model.model_fields.items():
                # collect all validations performed on a field
                field_validators = dict()

                if field_info.metadata:
                    for operation in field_info.metadata:
                        if hasattr(operation, "pattern"):
                            field_validators["pattern"] = F.column(field).rlike(
                                operation.pattern
                            )
                        if hasattr(operation, "ge"):
                            field_validators["maximum_ge"] = operator.ge(
                                F.column(field), operation.ge
                            )
                        if hasattr(operation, "gt"):
                            field_validators["maximum_gt"] = operator.gt(
                                F.column(field), operation.gt
                            )
                        if hasattr(operation, "le"):
                            field_validators["minimum_le"] = operator.le(
                                F.column(field), operation.le
                            )
                        if hasattr(operation, "lt"):
                            field_validators["minimum_lt"] = operator.lt(
                                F.column(field), operation.lt
                            )

                # build support for custom validations
                if field_info.json_schema_extra:
                    if field_info.json_schema_extra.get("distinct"):
                        field_name = field_info.alias or field
                        field_validators["custom_distinct"] = operator.sub(
                            data.count(),
                            data.drop_duplicates(subset=[field_name]).count(),
                        )

                all_fields[field] = field_validators

            for field, validation_queue in all_fields.items():
                if validation_queue == dict():
                    continue
                print(f">>> Validating `{field}`")
                for operation_name, operation in validation_queue.items():
                    if operation_name.startswith(
                        "custom_"
                    ):  # need better way of handling custom operations
                        validation_count = operation
                        operation = "<code unavailable>"
                    else:
                        validation_count = data.filter(~operation).count()
                    if validation_count > 0:
                        print(
                            f"\t[FAILURE] validation for {operation_name} flagged {validation_count:,} observations"
                        )
                        print(f"\t[>] Run code for sample: {operation}")
                    else:
                        print(
                            f"\t[SUCCESS] validation for {operation_name} flagged no observations"
                        )
            return data

        return wrapper

    return decorator
