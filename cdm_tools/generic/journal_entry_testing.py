from pydantic import Field
from cdm_tools.models import CommonDataModel

import decimal
import datetime


class GeneralLedgerDetail(CommonDataModel):
    entity_id: str
    fiscal_period: int = Field(ge=1, le=12)
    date_effective: datetime.date
    date_posted: datetime.date
    net_amount_ec: decimal.Decimal


class TrialBalance(CommonDataModel):
    entity_id: str
    account_number: str
    account_description: str


class ChartOfAccounts(CommonDataModel):
    entity_id: str
    account_number: str
    account_description: str
    financial_statement_line: str
    financial_statement_line_number: int
    financial_statement_category: str
    financial_statement_subtotal_category: str
    account_grouping_1: str
    account_grouping_1_number: int
    abcotd: str
