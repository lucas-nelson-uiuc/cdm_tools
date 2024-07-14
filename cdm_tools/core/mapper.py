import operator
import functools

from pyspark.sql import DataFrame, Column, functions as F


def map_values(
    data: DataFrame, mapping: dict[str, dict], label_column: str = "mapped_label"
) -> DataFrame:
    """
    Maps values from the DataFrame columns to specified labels based on the provided mapping dictionary.

    Parameters
    ----------
    data : DataFrame
        The input DataFrame.
    mapping : dict[str, dict]
        A dictionary where keys are the labels to be assigned and values are dictionaries specifying
        column-value pairs to match.
    label_column : str
        Name of column with mapped labels.

    Returns
    -------
    DataFrame
        The input DataFrame with an additional 'policy' column containing the mapped labels.

    Examples
    --------
    >>> policy_mapping = {
        "Policy usd1": {"ttype": "A", "currency_code": "USD"},
        "Policy usd2": {"ttype": "B", "currency_code": "USD"},
        "Policy cad3A": {"ttype": "A", "currency_code": "CAD", "journal_number": "PGCLHUQJIF"},
        "Policy cad3": {"ttype": "A", "currency_code": "CAD"},
    }
    >>> map_values(data=data, mapping=policy_mapping, label_column="policy")
    +-------------+-----+--------------+------------+
    |currency_code|ttype|journal_number|      policy|
    +-------------+-----+--------------+------------+
    |          CAD|    C|    PGCLHUQJIF|Policy cad3A|
    |          CAD|    C|    XYFQOXBOOI| Policy cad3|
    |          GBP|    A|    SMTEZVPJBH|        NULL|
    |          USD|    A|    TYAHMHQXUD| Policy usd1|
    |          CHN|    B|    ARNHDLZUHN|        NULL|
    +-------------+-----+--------------+------------+


    Notes
    -----
    The ordering of the mapping values is important. PySpark will return the
    label of the first matched criteria. If two labels share similar criteria,
    always place the more explicit/restrictive critiera first.
    """

    def assert_valid_mapping(mapping: dict[str, dict]) -> None:
        """
        Asserts that the mapping dictionary has unique keys and unique values.
        Uniqueness is required to ensure mapping can be performed accurately.

        Parameters
        ----------
        mapping : dict[str, dict]
            The dictionary to validate.

        Raises
        ------
        AssertionError
            If there are non-unique keys or values in the mapping.
        """
        assert len(mapping.keys()) == len(
            set(mapping.keys())
        ), "Non-unique keys discovered."
        assert len(mapping.values()) == len(
            set(pair.values() for pair in mapping.values())
        ), "Non-unique values discovered."

    def generate_expressions(mapping: dict[str, dict]) -> dict[str, Column]:
        """
        Generates a dictionary of conditions (Column expressions) for the mapping.

        Parameters
        ----------
        mapping : dict[str, dict]
            A dictionary where keys are the labels and values are dictionaries of column-value pairs to match.

        Returns
        -------
        dict[str, Column]
            A dictionary where keys are labels and values are Column expressions representing the conditions.
        """

        return {
            label: functools.reduce(
                operator.and_,
                map(lambda item: F.col(item[0]) == F.lit(item[1]), params.items()),
            )
            for label, params in mapping.items()
        }

    def apply_expressions(
        data: DataFrame, mapping: dict[str, Column], label_column: str = label_column
    ) -> Column:
        """
        Applies the generated expressions to the DataFrame to create the 'policy' column.

        Parameters
        ----------
        data : DataFrame
            The input DataFrame.
        mapping : dict[str, Column]
            A dictionary of conditions (Column expressions).
        label_name : str
            Name of column containing mapped values.

        Returns
        -------
        DataFrame
            The DataFrame with the 'policy' column added.
        """

        conditions = list((key, cond) for key, cond in mapping.items())
        return data.withColumn(
            label_column,
            functools.reduce(
                lambda acc, cond: acc.when(cond[1], F.lit(cond[0])),
                conditions[1:],
                F.when(conditions[0][1], F.lit(conditions[0][0])),
            ),
        )

    assert_valid_mapping(mapping)
    return apply_expressions(
        data=data, mapping=generate_expressions(mapping), label_column=label_column
    )
