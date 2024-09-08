from typing import Optional
import decimal
import datetime

from pydantic import Field

from pyspark.sql import functions as F

from ...common import CommonDataModel


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
    dc_indicator: Optional[str] = Field(
        default_factory=lambda x: F.when(
            F.col("net_amount_gc") > 0, F.lit("D")
        ).otherwise(F.lit("C"))
    )
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
