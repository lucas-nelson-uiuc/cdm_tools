import re
import functools

import pyspark
from pyspark.sql import DataFrame


def cp_read(
    filepath: str,
    pattern: str = ".*",
    recursive: bool = False,
    fs_func: callable = None,
    read_func: callable = None,
    union_func: callable = DataFrame.unionByName,
) -> pyspark.sql.DataFrame:
    pattern = re.compile(pattern)
    files = filter(
        lambda fp: isinstance(pattern.match(fp), re.Match),
        fs_func(filepath, recursive=recursive),
    )
    return functools.reduce(union_func, map(read_func, files))
