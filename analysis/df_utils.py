import polars as pl


def get_list_column_max_len(df: pl.DataFrame, list_column: str):
    return df.select(pl.col(list_column).list.len().max()).item()


def one_hot_encode_list_column(df: pl.DataFrame, list_column: str) -> pl.DataFrame:
    """
    One-hot encode a column containing lists in a Polars DataFrame.

    Args:
        df: Input Polars DataFrame
        list_column: Name of the column containing lists

    Returns:
        DataFrame with one-hot encoded columns
    """
    # Get unique values across all lists
    unique_values = df.select(pl.col(list_column).explode()).to_series().unique().sort()

    # Creat one-hot encoded columns
    encoded_columns = [
        pl.col(list_column).list.contains(value).alias(f"{list_column}_{value}")
        for value in unique_values
        if value is not None
    ]

    return df.with_columns(encoded_columns).drop(list_column)


def _normalize_rental_info(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add missing rentalInfo struct to pricingInfos and ensure consistent schema.
    Returns DataFrame with normalized pricingInfos structure.
    """
    # Get all field names from the listing struct
    listing_fields = [f.name for f in df.schema["listing"].fields]  # type: ignore

    # Check a sample row for rentalInfo structure
    sample_pricing = df.select(pl.col("listing").struct.field("pricingInfos")).row(0)[
        0
    ][0]

    # Create the complete rentalInfo structure
    complete_rental_info = {
        "period": None,
        "warranties": [],
        "monthlyRentalTotalPrice": None,
    }

    if "rentalInfo" not in sample_pricing:
        rental_info_update = complete_rental_info
    else:
        # If rentalInfo exists but is missing fields or is None, add them
        existing_rental_info = sample_pricing.get("rentalInfo", {}) or {}
        rental_info_update = {**complete_rental_info, **existing_rental_info}

    # Create a new column with updated pricingInfos
    df = df.with_columns(
        [
            pl.col("listing")
            .struct.field("pricingInfos")
            .map_elements(
                lambda x: [
                    {**pricing_info, "rentalInfo": rental_info_update}
                    for pricing_info in x
                ]
            )
            .alias("new_pricing_infos")
        ]
    )

    # Create expressions for all fields in the listing struct
    struct_fields = [
        pl.col("listing").struct.field(f).alias(f)
        for f in listing_fields
        if f != "pricingInfos"
    ]

    # Add the new pricingInfos
    struct_fields.append(pl.col("new_pricing_infos").alias("pricingInfos"))

    # Create the new listing struct
    df = df.with_columns([pl.struct(struct_fields).alias("listing")])

    # Drop the temporary column
    df = df.drop("new_pricing_infos")

    # Normalize capacityLimit to be List(Int64)
    df = df.with_columns(
        [
            pl.col("listing")
            .struct.field("capacityLimit")
            .cast(pl.List(pl.Int64))
            .alias("new_capacity_limit")
        ]
    )

    # Update the listing struct again for capacityLimit
    struct_fields = [
        pl.col("listing").struct.field(f).alias(f)
        for f in listing_fields
        if f != "capacityLimit"
    ]
    struct_fields.append(pl.col("new_capacity_limit").alias("capacityLimit"))

    df = df.with_columns([pl.struct(struct_fields).alias("listing")])

    # Drop the temporary column
    df = df.drop("new_capacity_limit")

    return df


def normalize_schemas(dfs: list[pl.DataFrame]) -> list[pl.DataFrame]:
    """
    Normalize schemas across multiple DataFrames to enable concatenation.
    """
    # Normalize each DataFrame
    normalized_dfs = [_normalize_rental_info(df) for df in dfs]

    # Verify schemas match
    base_schema = normalized_dfs[0].schema
    for df in normalized_dfs[1:]:
        assert (
            df.schema == base_schema
        ), f"Schema mismatch after normalization: {df.schema} != {base_schema}"

    return normalized_dfs
