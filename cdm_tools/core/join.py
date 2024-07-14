import pyspark.pandas as ps
from pyspark.sql import types, functions as F, Row, DataFrame


def join_fiscal_calendar(
    data: DataFrame,
    fiscal_calendar: tuple[dict],
    date_column: str = "date_effective",
    boundary_columns: tuple[str] = ("date_start", "date_end"),
) -> DataFrame:
    """Join fiscal calendar onto a pyspark.sql.DataFrame

    Given a fiscal calendar object, all relevant information can be joined onto
    the passed DataFrame for all observations within a given fiscal period.
    """
    return (
        data.join(
            fiscal_calendar,
            on=F.col(date_column).between(
                F.col(boundary_columns[0]), F.col(boundary_columns[1])
            ),
            how="left",
        ),
    )


def join_currency_conversion(
    data: DataFrame,
    conversion_dictionary: dict[str, float],
    amount_column: str,
    currency_code_column: str,
    scale_column: str = None,
) -> DataFrame:
    """
    Joins a DataFrame `data` with a currency conversion dictionary to scale values
    based on the provided conversion factors.

    Parameters
    ==========
    data : DataFrame
        Input PySpark DataFrame containing data to be scaled.

    conversion_dictionary : dict[str, float]
        Dictionary mapping currency codes (keys) to conversion factors (values).

    amount_column : str
        Name of the column in `data` containing the amounts to be scaled.

    currency_code_column : str
        Name of the column in `data` containing the currency codes corresponding to `amount_column`.

    scale_column : str, optional
        Name of the column where scaled amounts will be stored. If None, overwrites `amount_column`.

    Returns
    =======
    DataFrame
        PySpark DataFrame with scaled amounts based on the conversion factors.

    Raises
    ======
    ValueError
        If `conversion_dictionary` is not a dictionary.

    Notes
    =====
    - This function ensures that the currency codes present in `data` align with
      those in `conversion_dictionary` to prevent NULL values in the resulting
      scaled amount column due to mismatched or missing codes.
    - Performs a left join of `data` with the converted DataFrame from `conversion_dictionary`.
    - Scales the `amount_column` using the corresponding conversion factors and stores
      the scaled values in `scale_column` or overwrites `amount_column`.
    """

    def extract_codes(data: DataFrame) -> list[Row]:
        """Retrieve unique currency codes from data"""
        return data.select(currency_code_column).distinct().collect()

    def assert_codes_align(data: DataFrame, conversion_codes: list[str]) -> None:
        """
        Ensures that the currency codes present in the DataFrame align with
        those in the conversion dictionary. This validation prevents potential
        issues like NULL values in the resulting amount column due to mismatched
        or missing codes.
        """
        data_codes = set([row.currency_code for row in extract_codes(data)])

        # dictionary contains codes found in data
        dictionary_complete = set(data_codes).difference(conversion_codes)
        data_complete = set(conversion_codes).difference(data_codes)

        # check intersection condition is met
        assert (
            dictionary_complete == set()
        ), f"Conversion dictionary missing: {dictionary_complete}"
        assert data_complete == set(), f"Data does not contain: {data_complete}"

    if not isinstance(conversion_dictionary, dict):
        raise ValueError(
            "Current support for currency conversion data is limited to dictionaries."
        )

    assert_codes_align(data=data, conversion_codes=conversion_dictionary.keys())

    conversion_columns = ["currency_code", "scale_factor"]
    conversion_data = ps.DataFrame(
        conversion_dictionary.items(), columns=conversion_columns
    ).to_spark()

    return data.join(
        conversion_data,
        on=data[currency_code_column] == conversion_data["currency_code"],
        how="left",
    ).withColumn(
        scale_column or amount_column,
        # casting unnecessary in practice
        (F.col(amount_column) * F.col("scale_factor")).cast(types.DecimalType(38, 6)),
    )
