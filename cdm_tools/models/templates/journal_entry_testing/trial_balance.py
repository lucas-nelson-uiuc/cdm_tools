from typing import Optional, Iterable
import decimal
import datetime

from pyspark.sql import Row, DataFrame, functions as F
from pydantic import BaseModel, Field

from ...common import CommonDataModel


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


class OmniaTrialBalanceModel(CommonDataModel):
    detail_num: str
    detail: str
    account_grouping_1_num: str
    account_grouping_1: str
    beginning_balance: decimal.Decimal
    ending_balance: decimal.Decimal


class OmniaMetadataModel(BaseModel):
    chart_of_accounts_name: str
    trial_balance_name: str
    period_type: str
    period_end_date: datetime.date
    currency: str


def read_omnia_tb(
    data: DataFrame, period_end_dates: Iterable[datetime.date]
) -> DataFrame:
    def preprocess_omnia_tb(data: DataFrame) -> DataFrame:
        for column in data.columns:
            data = data.withColumnRenamed(column, column.lower().replace(" ", "_"))
        return data

    def get_metadata(data: DataFrame) -> list[Row]:
        return data.limit(len(OmniaMetadataModel.model_fields.keys())).collect()

    def get_balance_columns(data: DataFrame, metadata: list[Row]) -> list[str]:
        def is_expected_date(date_value: str) -> bool:
            OMNIA_DATE_FORMAT = "%m/%d/%Y"
            try:
                date_value = datetime.datetime.strptime(
                    date_value, OMNIA_DATE_FORMAT
                ).date()
                return True if date_value in period_end_dates else False
            except ValueError as e:
                return False

        for row in metadata:
            if row.account_grouping_1 == "Period End Date":
                period_values = row.asDict()
                break
        balance_columns = [
            column
            for column, date_value in period_values.items()
            if is_expected_date(date_value)
        ]
        for column, standard_name in zip(
            balance_columns, ("beginning_balance", "ending_balance")
        ):
            data = data.withColumnRenamed(column, standard_name)
        return data

    period_end_dates = sorted(period_end_dates)
    data = preprocess_omnia_tb(data=data)
    metadata = get_metadata(data=data)
    data = get_balance_columns(data=data, metadata=metadata)
    return data.select(*OmniaTrialBalanceModel.model_fields.keys()).filter(
        ~(F.col("detail_num").isNull() | F.col("detail_num").rlike("^\s*$"))
    )
