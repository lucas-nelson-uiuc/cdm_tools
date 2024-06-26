import re
import string
import functools

import pyspark
from pyspark.sql import functions as F


def normalize_data(
    data: pyspark.sql.DataFrame, repl: str = "_", str_case: str = "lower"
) -> pyspark.sql.DataFrame:
    def normalize_column_values(data: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return data.withColumns(
            {column: F.trim(F.col(column)) for column in data.columns}
        )

    def normalize_column_names(
        data: pyspark.sql.DataFrame, repl: str, str_case: str
    ) -> pyspark.sql.DataFrame:
        def apply_rules(column: str, rules: tuple[dict]) -> str:
            return functools.reduce(
                lambda init_val, rule: re.sub(
                    re.compile(rule.get("pattern")), rule.get("repl"), init_val
                ),
                rules,
                column,
            )

        def rename_columns(
            data: pyspark.sql.DataFrame, column_mapping: dict[str, str]
        ) -> pyspark.sql.DataFrame:
            for old, new in column_mapping.items():
                data = data.withColumnRenamed(old, new)
            return data

        RE_INVALID_CHARACTERS = f"[\s{string.punctuation}]+"
        normalization_rules = (
            {
                "pattern": f"(^{RE_INVALID_CHARACTERS}|{RE_INVALID_CHARACTERS}$)",
                "repl": "",
            },
            {"pattern": f"{RE_INVALID_CHARACTERS}", "repl": repl},
        )
        columns = map(getattr(str, str_case), data.columns)
        column_mapping = {
            column: apply_rules(column=column, rules=normalization_rules)
            for column in columns
        }
        return rename_columns(data=data, column_mapping=column_mapping)

    data = normalize_column_names(data=data, repl=repl, str_case=str_case)
    data = normalize_column_values(data=data)
    return data
