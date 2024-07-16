# Common Data Model Tools

The package `cdm_tools` is a collection of functional modules with the purpose of 


## Philosophy

There are three modules available in `cdm_tools`:

- `core`: basic data transformation functions strapped with functional programming; generic queries such as `filter_nulls()` allows for explicit and expressive querying of any DataFrame

- `models`: integrated data modeling with Pydantic in PySpark; allows users to take advantage of the fundamentals of data models to reduce workloads, structure data lineage, and coherently describe data transformations

- `tidy`: experimental package for orchestrating tedious workflows; focuses on making data interactive and workflows into robust pipelines


## Usage

Most examples are built for the `core` module. However, as one adopts the other modules, it will become simple and obvious to transition to adopting the rest of the modules, as the intention for this package is to be an ecosystem rather than separate silos.

```python
### SELECTING COLUMNS
from pyspark.sql import types
from cdm_tools.core.select import select_columns

# select all columns from DataFrame
data = select_columns(data)

# select columns safely
data = select_columns(data, columns=["account_number", "account_description", "ColNotExist"])
# >>> returns DataFrame["account_number", "account_description"]

# select columns matching pattern(s)
data = select_columns(data, pattern="^fiscal")
data = select_columns(data, pattern=["^fiscal", "date$"]) # evaluates to: "(^fiscal|effective$)"

# select columns of specified data type(s)
data = select_columns(data, dtype=types.StringType)
data = select_columns(data, dtype=[types.IntegerType, types.DecimalType])

# select columns meeting any/all conditions
data = select_columns(
    data,
    columns=["account_number", "account_description", "ColNotExist"],
    pattern=["^fiscal", "date$"],
    dtype=types.DateType,
    strict=False
)
# >>> returns ["account_number", "account_description", "fiscal_year", "fiscal_period", "effective_date", "posted_date"]
data = select_columns(
    data,
    columns=["account_number", "account_description", "fiscal_year", "fiscal_period", "date_effective"],
    pattern=["^fiscal", "date$"],
    dtype=types.DateType,
    strict=True
)
# >>> returns ["effective_date", "posted_date"]


### FILTERING DATA
from cdm_tools.core.filter import filter_nulls, filter_regex

# remove entries with nulls in `account_number` and `account_description`
data = filter_nulls(data, columns=["account_number", "account_description"])

# keep entries matching a pattern
data = filter_regex(data, columns=["account_number"], pattern="^\d{4}-\d{3}$")
```

As mentioned previously, the goal is to create an ecosystem. The previous examples could work like such to create an intuitive, robust data pipeline.

```python
from pyspark.sql import functions as F

from cdm_tools.models import CommonDataModel
from cdm_tools.tidy import TidyDataFrame


class MyClientGL(CommonDataModel):
    account_number: str = Field(nulls=False, unique=True)
    account_description: str
    date_effective: datetime.date = Field(
        ge=datetime.date(2024, 1, 1),
        le=datetime.date(2024, 12, 31)
    )
    fiscal_period: int  = Field(ge=1, le=12)
    fiscal_year: int = Field(
        default_factory=lambda x: F.year(F.col("date_effective"))
    )


raw_client_gl = read(...)
processed_client_gl = (
    TidyDataFrame(raw_client_gl)
    .cdm_transform(MyClientGL)
    .cdm_validate(MyClientGL)
)
```

## Contributing

Please reach out to Lucas Nelson (lunelson@deloitte.com)
