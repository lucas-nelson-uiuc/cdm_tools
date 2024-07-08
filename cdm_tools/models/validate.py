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
                if field_info.metadata:
                    field_validators = dict()
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
                    all_fields[field] = field_validators
            for field, validation_queue in all_fields.items():
                print(f">>> Validating `{field}`")
                for operation_name, operation in validation_queue.items():
                    validation_count = data.filter(~operation).count()
                    if validation_count > 0:
                        print(
                            f"\t[FAILURE] `{operation_name}` validation flagged {validation_count:,} observations"
                        )
                        print(f"\t[>] Run code for sample: {operation}")
                    else:
                        print(
                            f"\t[SUCCESS] `{operation_name}` validation flagged flagged no observations"
                        )
            return data

        return wrapper

    return decorator
