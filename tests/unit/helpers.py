"""
Node helper functions for unit tests.

Intentionally minimal -- just enough logic to exercise the orchestrator
without any real business code.
"""

import polars as pl

from orchestrator.protocols import ConfigLike


def good_node(config: ConfigLike, raw_df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    return {"df_out": raw_df.with_columns(pl.lit(1).alias("processed"))}


def multi_output_node(
    config: ConfigLike,
    df_a: pl.DataFrame,
    df_b: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    return {"df_merged": df_a, "df_summary": df_b}


def failing_node(config: ConfigLike, raw_df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    raise ValueError("something went wrong in the business logic")


def wrong_outputs_node(
    config: ConfigLike, raw_df: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    # Declared "df_out" but returns "wrong_key" -- contract violation
    return {"wrong_key": raw_df}


def ingest_products_node(
    config: ConfigLike, raw_df_product: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    return {"df_product": raw_df_product.rename({"id": "product_id"})}


def enrich_products_node(
    config: ConfigLike, df_product: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    return {
        "df_product_enriched": df_product.with_columns(
            pl.lit("enriched").alias("status")
        )
    }


def pipeline_failing_node(
    config: ConfigLike, df_product: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    raise RuntimeError("downstream failure")


def never_called_node(
    config: ConfigLike, df_product: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    raise AssertionError("This node should never be called.")
