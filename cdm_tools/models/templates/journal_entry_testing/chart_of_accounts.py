from typing import Optional
import datetime

from pyspark.sql import functions as F
from pydantic import Field

from ...common import CommonDataModel


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
        default_factory=lambda x: (
            F.when(F.col('financial_statement_category').isin(["Equity", "Revenue", "Expenses"]), F.lit("Equity"))
            .otherwise(F.lit(F.col("financial_statement_category")))
        ),
        pattern="(Assets|Liabilities|Equity)"
    )
    financial_statement_subtotal_category_id: Optional[str]
    financial_statement_type: str = Field(
        default_factory=lambda x: (
            F.when(F.col('financial_statement_category').isin(["Revenue", "Expenses"]), F.lit("Income Statement"))
            .otherwise(F.lit("Balance Sheet"))
        ),
        pattern="(Balance Sheet|Income Statement)"
    )
    account_grouping_1: str
    account_grouping_1_num: str
    abcotd: str
    abcotd_id: Optional[str]
    abcotd_significance: Optional[str]
    coa_account_key: Optional[str]


class OmniaAccountGroupingModel(CommonDataModel):
    account_grouping_1_num: str
    account_grouping_1: str
    financial_statement_line_num: str
    financial_statement_line: str


class OmniaFinancialStatementLineModel(CommonDataModel):
    financial_statement_line_num: str
    financial_statement_line: str
    financial_statement_subtotal_category: str
    financial_statement_category: str
    financial_statement_type: str
