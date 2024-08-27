from pyspark.sql import DataFrame, functions as F

from .common import CommonDataModel


def assert_not_empty(data: DataFrame, model: CommonDataModel) -> bool:
    required_fields = model.get_required_fields()
    n_rows = data.count()
    for field in required_fields:
        empty_values = data.filter(F.col(field).isNull() | F.col(field).rlike(r"^\s*$"))
        if not empty_values.isEmpty():
            n_values = empty_values.count()
            print(f"Missing values detected in {field} ({empty_values.count():,} values, {n_values / n_rows:.1%})")
        else:
            print(f"All values populated in {field}")
    return data
