import functools
import operator

from pyspark.sql import functions as F


def cdm_validate(model):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            model_properties = model.schema().get("properties")
            for field in model_properties:
                field_validators = dict()
                field_properties = model_properties.get(field)
                if "maximum" in field_properties:
                    field_validators["maximum"] = operator.le(
                        F.col(field), field_properties["maximum"]
                    )
                if "minimum" in field_properties:
                    field_validators["minimum"] = operator.ge(
                        F.col(field), field_properties["minimum"]
                    )
                if "testing_period" in field_properties:
                    field_validators["within_range"] = operator.and_(
                        operator.ge(
                            F.col(field),
                            field_properties["testing_period"]["start_date"],
                        ),
                        operator.le(
                            F.col(field), field_properties["testing_period"]["end_date"]
                        ),
                    )
                if field_validators:
                    for name, validator in field_validators.items():
                        invalid_observations = data.filter(~validator)
                        assert invalid_observations.isEmpty(), f"""
                            (`{field}`) `{name}` validator failed, returned {invalid_observations.count():,} rows
                            Expected: `{field_properties[name]}`
                            Sample data: data.filter(~ F.expr("{validator}")).limit(100).display() # change `data` to your dataset's name
                            """
            return data

        return wrapper

    return decorator
