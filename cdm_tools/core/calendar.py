from typing import Union
import re
import datetime
import calendar

import pyspark
import pyspark.pandas as ps
from pyspark.sql.pandas import types, functions as F


def format_testing_period(
    fiscal_calendar: tuple[dict],
    period_start: int = 1,
    period_end: int = 12
) -> dict[str, datetime.date]:
    """Given a fiscal calendar, extract and format the testing period start/end dates."""
    
    def extract_testing_period(
        fiscal_calendar: tuple[dict],
        period_value: int,
        date_label: str
    ) -> datetime.date:
        """Filter fiscal calendar for requested period, return relevant date value."""
        testing_period = tuple(
            period
            for period in fiscal_calendar
            if period.get("fiscal_period") == period_value
        )
        return testing_period[0].get(date_label)
    
    return {
        date_label: extract_testing_period(fiscal_calendar, period_value, date_label)
        for date_label, period_value in zip(("date_start", "date_end"), (period_start, period_end))
    }


def get_fiscal_calendar(
    fiscal_calendar_dict: dict = None,
    fiscal_year: int = datetime.datetime.now().year,
    fiscal_period_start: int = 1,
    fiscal_period_end: int = 12,
    fiscal_period_step: int = 1,
) -> pyspark.sql.DataFrame:
    """Generate a fiscal calendar based on user-provided parameters.

    If a dictionary, contains all information already, this function will
    generate a DataFrame to store this information. Else, the user can pass
    parameters to create their own fiscal calendar, given the request is valid.
    """
    fiscal_period_end += 1

    assert (
        fiscal_period_start < fiscal_period_end
    ), "Cannot have end date prior to start date"
    assert (
        (fiscal_period_end - fiscal_period_start) % fiscal_period_step == 0
    ), "Step must divide evenly into the difference of `fiscal_period_end` - `fiscal_period_start`"

    if fiscal_calendar_dict is None:
        fiscal_calendar = calendar.Calendar()
        datetime_range = range(
            fiscal_period_start, fiscal_period_end, fiscal_period_step
        )
        return ps.DataFrame(
            {
                "period_name": [f"Period {i}" for i in datetime_range],
                "fiscal_period": [i for i in datetime_range],
                "date_start": [
                    datetime.date(fiscal_year, i, 1) for i in datetime_range
                ],
                "date_end": [
                    datetime.date(
                        fiscal_year,
                        i,
                        max(fiscal_calendar.itermonthdays(fiscal_year, i)),
                    )
                    for i in datetime_range
                ],
            }
        ).to_spark()

    return ps.DataFrame(
        {
            "period_name": fiscal_calendar_dict.keys(),
            "fiscal_period": [
                int(re.sub(re.compile("\D+"), "", key))
                for key in fiscal_calendar_dict.keys()
            ],
            "date_start": [
                value.get("date_start") for value in fiscal_calendar_dict.values()
            ],
            "date_end": [
                value.get("date_end") for value in fiscal_calendar_dict.values()
            ],
        }
    ).to_spark()


def get_fiscal_calendar_data(
    data: pyspark.sql.DataFrame,
    fiscal_calendar: Union[dict, pyspark.sql.DataFrame],
    date_column: str = "date_effective",
    boundary_columns: tuple[str] = ("date_start", "date_effective"),
) -> pyspark.sql.DataFrame:
    """Join fiscal calendar onto a pyspark.sql.DataFrame

    Given a fiscal calendar object, all relevant information can be joined onto
    the passed DataFrame for all observations within a given fiscal period.
    """
    if isinstance(fiscal_calendar, dict):
        fiscal_calendar = get_fiscal_calendar(fiscal_calendar_dict=fiscal_calendar)
    return data.withColumn(date_column, F.col(date_column).cast(types.DateType())).join(
        fiscal_calendar,
        on=F.col(date_column).between(
            F.col(boundary_columns[0]), F.col(boundary_columns[1])
        ),
        how="left",
    )
