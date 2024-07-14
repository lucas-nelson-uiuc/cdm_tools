
# `models` Module

The Models module leverages Pydantic to transform and validate data within PySpark. This module underscores the importance of data models in our workflow and utilizes the convenience of Pydantic for robust data handling.

## Features

Model-handling decorators for orchestrating data transformations and validations.

- `cdm_transform`: Transforms data using defined models.
- `cdm_validate`: Validates data against defined models.

## Example

The models module is built on defining complete and accurate data models. These models can be as simple as providing each field's name and type, or as complex as providing functions as defined by Pydantic's `validation` or `default_factory` parameters. The desired data model should completely represent the unique features of the data that are worth stating in plain code.

Let's define a simple model:

```python
import decimal
import datetime

from pyspark.sql import DataFrame, functions as F

from pydantic import BaseModel, Field


# define model specific to client-provided dataset
class ClientGL(BaseModel):
    account_number: str
    account_description: str
    fiscal_year: int
    fiscal_period: int
    debit_amount: decimal.Decimal
    credit_amount: decimal.Decimal
    date_effective: datetime.date
    time_effective: datetime.datetime
```

This simple model represents the basic structure of our data - we've listed all the field names and their field types. Within a glance, one can easily understand what their data looks like without having to explicitly view the data.

Since this model stores basic information about our data shape, we can use it to conform any raw data into the shape of that model with the help of the `@cdm_transform` and `@cdm_validate` decorators.

- `@cdm_transform`: Rename and cast the field given the listed field name and field type, respectively

- `@cdm_validate`: If provided, perform all validations and report SUCCESS (no rows flagged) or FAILURE (at least one row flagged)

```python
from cdm_tools.models.transform import cdm_transform
from cdm_tools.models.validate import cdm_validate


@cdm_validate(model=ClientGL)
@cdm_transform(model=ClientGL)
def preprocess_gl(data: DataFrame) -> DataFrame:
    # ... do some custom processing ...
    return data
```

Notice that the decorators accept one parameter: `model`. We only defined one (simple) model above, but we could define multiple models if we need our data to take on multiple formats. Maybe there is an intermediate schema we need our data to take on before moving onto its final form, or maybe we want to validate our data satisfies two models. This frameowrk is additive and exchangable.

Once you've completed your data processing function and decorated it with the models you want to transform/validate your data against, you can call it like you would. Monitor the log for any errors/warnings that arise. If the data returns successfully, you can guarantee it meets your "expectations" as described in the data model(s) you defined.

```python
raw_data = read_data(...)
transformed_data = preprocess_gl(data=raw_data)
```

Finally, here's an example taking advantage of all `cdm_tools.models` has to offer:

```python
# define basic model for general ledger detail dataset
class GeneralLedgerDetail(BaseModel):
    account_number: str
    account_description: str
    fiscal_year: int
    fiscal_period: int
    debit_amount: decimal.Decimal
    credit_amount: decimal.Decimal
    date_effective: datetime.date
    time_effective: datetime.datetime
    transaction_type: str
    is_standard: str

# define client-specific constants to be used in model
NONSTANDARD_VALUES = [
    "Manual",
    "Spreadsheet"
]
TESTING_PERIOD = {
    "date_start": {
        "fiscal_period": 1
        "fiscal_date": datetime.date(2024, 1, 1)
    },
    "date_end": {
        "fiscal_period": 12
        "fiscal_date": datetime.date(2024, 12, 31)
    }
}

# define client data model as nested model of general ledger detail model
class ClientDataModel(GeneralLedgerDetail):
    account_number: str = Field(
        pattern="^\d{4}-\d{3}$" # validate all values match regular expression
    )
    fiscal_period: int = Field(
        # ensure values are within the range [1, 12]
        ge=TESTING_PERIOD.get("date_start").get("fiscal_period"),
        le=TESTING_PERIOD.get("date_end").get("fiscal_period")
    )
    date_effective: datetime.date = Field(
        alias="approval_date",  # map to `approval_date` column in raw data
        # ensure values are within the range ["01/01/2024", "12/31/2024"]
        ge=TESTING_PERIOD.get("date_start").get("fiscal_date"),
        le=TESTING_PERIOD.get("date_end").get("fiscal_date")
    )
    is_standard: str = Field(
        # create field as function of other fields in model
        default_factory=lambda x: F.when(
            F.col("transaction_type").isin(NONSTANDARD_VALUES),
            F.lit("N")
        ).otherwise(F.lit("S")),
        # validate created field meets regular expression
        pattern="(N|S)"
    )
    # ... all other fields inherited from nested model ...


# make use of same framework to transform/validate data
@cdm_validate(model=ClientDataModel)
@cdm_transform(model=ClientDataModel)
def preprocess_gl(data: DataFrame) -> DataFrame:
    # ... do some custom processing ...
    return data

raw_data = read_data(...)
transformed_data = preprocess_gl(data=raw_data)
```