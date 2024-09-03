import datetime

from pydantic import Field

from ...common import CommonDataModel


class FiscalCalendar(CommonDataModel):
    fiscal_period: str = Field()
    fiscal_period_end: datetime.date
    fiscal_period_start: datetime.date
    fiscal_quarter: str
    fiscal_year: int
    fiscal_year_identifier: str = Field(pattern="(Current|Prior) Period[\s\d]*")
    import_date: datetime.date