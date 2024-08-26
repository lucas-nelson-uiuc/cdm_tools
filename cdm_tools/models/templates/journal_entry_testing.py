from typing import Optional
import decimal
import datetime

from attrs import define, field
from pydantic import Field

from pyspark.sql import functions as F

from ..common import CommonDataModel, CommonAnalytic


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


@define
class JournalEntryTestingAnalytic(CommonAnalytic):
    name: str = "Journal Entry Testing"
    description: str = "..."
    general_ledger_detail: GeneralLedgerDetailModel = field(
        default=GeneralLedgerDetailModel
    )
    trial_balance: TrialBalanceModel = field(default=TrialBalanceModel)
    general_ledger_detail: ChartOfAccountsModel = field(default=ChartOfAccountsModel)

    def transform_models(self):
        columns_to_upper: tuple[str] = ("entity_id", "account_number")

        def transform_general_ledger() -> None:
            columns_numeric: tuple[str] = (
                "net_amount_ec",
                "net_amount_oc",
                "net_amount_gc",
            )

            self.general_ledger_detail = (
                self.general_ledger_detail.withColumns(
                    {column: F.upper(F.col(column)) for column in columns_to_upper}
                )
                .withColumns(
                    {
                        column.replace("net", "debit"): F.when(
                            F.col(column) > 0, F.abs(column)
                        ).otherwise(F.lit(0))
                        for column in columns_numeric
                    }
                )
                .withColumns(
                    {
                        column.replace("net", "credit"): F.when(
                            F.col(column) < 0, F.abs(column)
                        ).otherwise(F.lit(0))
                        for column in columns_numeric
                    }
                )
            )

        def transform_trial_balance() -> None:
            self.trial_balance = self.trial_balance.withColumns(
                {column: F.upper(F.col(column)) for column in columns_to_upper}
            )

        def transform_chart_of_accounts() -> None:
            columns_to_title: tuple[str] = (
                "account_description",
                "account_grouping_1",
                "financial_statement_line",
                "financial_statement_category",
                "financial_statement_subtotal_category",
                "abcotd",
            )
            self.chart_of_accounts = self.chart_of_accounts.withColumns(
                {column: F.upper(F.col(column)) for column in columns_to_upper}
            ).withColumns(
                {column: F.initcap(F.col(column)) for column in columns_to_title}
            )

        transform_general_ledger()
        transform_trial_balance()
        transform_chart_of_accounts()

    def filter_nonzero_entries(self) -> None:
        """Remove all observations from a model that contain a zero amount value"""

        def get_exclusion_query(
            columns: list[str] = None,
            inequality: bool = True,
        ) -> str:
            """
            Returns SQL query to remove zero-amounts from numeric column(s).

            If columns to perform exclusion on are not explicitly provided, then one of
            `pattern` or `dtype` must not be `None`. Whichever is provided will be used
            to get the appropriate columns. Finally, the columns are concatenated together
            in a SQL-like query. This will be passed to the exclusion function; hence,
            it must be a valid SQL expression.
            """
            return " OR ".join(
                map(lambda x: f"({x} {'!=' if inequality else '='} 0)", columns)
            )

        def filter_general_ledger_detail() -> None:
            RE_NET_AMOUNT_FIELDS = "^net_.*_[geo]c$"
            self.general_ledger_detail = self.general_ledger_detail.filter(
                get_exclusion_query(
                    columns=GeneralLedgerDetailModel.get_columns(
                        pattern=RE_NET_AMOUNT_FIELDS
                    ),
                    inequality=False,
                )
            )

        def filter_trial_balance() -> None:
            RE_BALANCE_FIELDS = "^.*_balance_[geo]c"
            self.trial_balance = self.trial_balance.filter(
                get_exclusion_query(
                    columns=TrialBalanceModel.get_columns(pattern=RE_BALANCE_FIELDS),
                    inequality=False,
                )
            )

        filter_general_ledger_detail()
        filter_trial_balance()
