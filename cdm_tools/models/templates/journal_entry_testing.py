from ..common import CommonDataModel

import decimal
import datetime


from typing import Optional
import datetime
import decimal

from pydantic import BaseModel, Field


# class CommonDataModel(BaseModel):
#     class Config:
#         arbitrary_types_allowed = True
#         extra = "forbid"

#     @classmethod
#     def get_fields(cls):
#         return cls.__annotations__.items()

#     @classmethod
#     def get_request_form(cls):
#         return {
#             field: {"name": field_info.alias or field, "dtype": field_info.annotation}
#             for field, field_info in cls.model_fields.items()
#         }


class GeneralLedgerDetailModel(CommonDataModel):
    ### client fields
    entity_id: str
    entity_name: str
    chart_of_accounts: str

    ### entry fields
    account_number: str
    account_description: Optional[str]
    journal_number: str
    journal_header_description: Optional[str]
    journal_line_number: Optional[str]
    journal_line_description: Optional[str]
    is_standard: str
    location: Optional[str]
    is_manual: Optional[str]
    business_area: Optional[str]
    controlling_area_for_cost_and_profit_center: Optional[str]
    cost_center: Optional[str]
    profit_center: Optional[str]
    project: Optional[str]
    custom_exclusion: Optional[str]
    ledger_id: Optional[str]
    ledger_group: Optional[str]

    ### datetime fields
    fiscal_period: int
    fiscal_year: int
    date_effective: datetime.date
    date_posted: datetime.date
    time_posted: datetime.datetime
    date_updated: Optional[datetime.date]
    time_updated: Optional[datetime.datetime]
    date_approved: Optional[datetime.date]
    time_approved: Optional[datetime.datetime]
    extract_date: Optional[datetime.date]
    import_date: Optional[datetime.date]

    ### currency fields
    net_amount_ec: decimal.Decimal
    debit_amount_ec: Optional[decimal.Decimal]
    credit_amount_ec: Optional[decimal.Decimal]
    entity_currency_ec: str = Field(pattern="^\w{2,5}$")
    net_amount_oc: decimal.Decimal
    debit_amount_oc: Optional[decimal.Decimal]
    credit_amount_oc: Optional[decimal.Decimal]
    original_currency_oc: str = Field(pattern="^\w{2,5}$")
    net_amount_gc: decimal.Decimal
    debit_amount_gc: Optional[decimal.Decimal]
    credit_amount_gc: Optional[decimal.Decimal]
    group_currency_gc: str = Field(pattern="^\w{2,5}$")
    dc_indicator: Optional[str]
    exchange_rate: Optional[float]
    foreign_exchange_date: Optional[datetime.date]
    forex_conversion_method: Optional[str]

    ### categorical variables
    transaction_type: str
    transaction_type_description: Optional[str]
    source: str
    source_description: Optional[str]
    userid_entered: Optional[str]
    user_name_entered: Optional[str]
    userid_approved: Optional[str]
    user_name_approved: Optional[str]
    updated_by: Optional[str]
    name_of_user_updated: Optional[str]
    misc1: Optional[str]
    misc2: Optional[str]
    misc3: Optional[str]
    misc4: Optional[str]
    misc5: Optional[str]

    ### chart of accounts fields
    coa_account_key: Optional[str]


class TrialBalanceModel(CommonDataModel):
    ### client fields
    entity_id: str
    entity_name: str
    chart_of_accounts: str

    ### account fields
    account_number: str
    account_description: Optional[str]

    ### datetime fields
    fiscal_period: int = Field(ge=1, le=12)
    fiscal_year: int
    period_end_date: datetime.date
    period_type: str = Field(pattern="(BEG|END)")
    extract_date: Optional[datetime.date]
    import_date: Optional[datetime.date]

    ### currency fields
    ending_balance_ec: decimal.Decimal
    entity_currency_ec: str = Field(pattern="^\w{2,5}$")

    ending_balance_gc: decimal.Decimal
    group_currency_gc: str = Field(pattern="^\w{2,5}$")

    beginning_balance_ec: Optional[decimal.Decimal]
    period_activity_ec: Optional[decimal.Decimal]
    adjusted_ec: Optional[decimal.Decimal]
    adjusted_journal_entry_ec: Optional[decimal.Decimal]
    reclassification_journal_entry_ec: Optional[decimal.Decimal]
    preliminary_ec: Optional[decimal.Decimal]

    ending_balance_gc: decimal.Decimal
    beginning_balance_gc: Optional[decimal.Decimal]
    period_activity_gc: Optional[decimal.Decimal]
    adjusted_gc: Optional[decimal.Decimal]
    adjusted_journal_entry_gc: Optional[decimal.Decimal]
    reclassification_journal_entry_gc: Optional[decimal.Decimal]
    consolidation_journal_entry_gc: Optional[decimal.Decimal]
    preliminary_gc: Optional[decimal.Decimal]

    ### categorical variables
    ledger_id: Optional[str]
    ledger_group: Optional[str]
    misc1: Optional[str]
    misc2: Optional[str]

    ### chart of accounts fields
    coa_account_key: Optional[str]


class ChartOfAccountsModel(CommonDataModel):
    ### client fields
    entity_id: str
    entity_name: str
    chart_of_accounts: str

    ### account fields
    account_number: str
    account_description: str

    ### datetime fields
    revision_date: Optional[datetime.date]
    extract_date: Optional[datetime.date]
    import_date: Optional[datetime.date]

    ### categorical variables
    content_area_id: Optional[str]
    content_area: Optional[str]
    tags: Optional[str]

    ### chart of accounts fields
    financial_statement_line: str
    financial_statement_line_num: str
    financial_statement_category: str = Field(
        pattern="(Assets|Liabilities|Equity|Revenue|Expenses)"
    )
    financial_statement_category_id: Optional[str]
    financial_statement_subtotal_category: str = Field(
        pattern="(Assets|Liabilities|Equity)"
    )
    financial_statement_subtotal_category_id: Optional[str]
    financial_statement_type: str = Field(pattern="(Balance Sheet|Income Statement)")
    account_grouping_1: str
    account_grouping_1_num: str
    abcotd: str
    abcotd_id: Optional[str]
    abcotd_significance: Optional[str]
    coa_account_key: Optional[str]
