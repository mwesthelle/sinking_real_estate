import polars as pl


def get_list_column_max_len(df: pl.LazyFrame, list_column: str):
    return df.select(pl.col(list_column).list.len().max()).collect().item()


def one_hot_encode_list_column(df: pl.LazyFrame, list_column: str) -> pl.LazyFrame:
    """
    One-hot encode a column containing lists in a Polars DataFrame.

    Args:
        df: Input Polars DataFrame
        list_column: Name of the column containing lists

    Returns:
        DataFrame with one-hot encoded columns
    """
    # Get unique values across all lists
    unique_values = (
        df.select(pl.col(list_column).explode()).collect().to_series().unique().sort()
    )

    # Creat one-hot encoded columns
    encoded_columns = [
        pl.col(list_column).list.contains(value).alias(f"{list_column}_{value}")
        for value in unique_values
    ]

    return df.with_columns(encoded_columns)
