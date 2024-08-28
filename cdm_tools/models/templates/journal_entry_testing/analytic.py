from attrs import define, field

from pyspark.sql import functions as F

from ...common import CommonAnalytic
from .general_ledger_detail import GeneralLedgerDetailModel
from .trial_balance import TrialBalanceModel
from .chart_of_accounts import ChartOfAccountsModel


@define
class JournalEntryTestingAnalytic(CommonAnalytic):
    name: str = "Journal Entry Testing"
    description: str = "..."
    general_ledger_detail: GeneralLedgerDetailModel = field(
        default=GeneralLedgerDetailModel
    )
    trial_balance: TrialBalanceModel = field(default=TrialBalanceModel)
    chart_of_accounts: ChartOfAccountsModel = field(default=ChartOfAccountsModel)

    def transform_models(self):
        """Perform generic transformations on models' data"""

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
