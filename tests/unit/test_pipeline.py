from unittest.mock import MagicMock

import polars as pl
import pytest

from orchestrator.exceptions import DataDumpError, MissingInputError, NodeExecutionError
from orchestrator.node import Node
from orchestrator.pipeline import Pipeline
from orchestrator.protocols import ConfigLike, DataManagerLike
from tests.unit.helpers import (
    ingest_products_node,
    never_called_node,
    pipeline_failing_node,
)

# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_pipeline_runs_nodes_in_order(
    two_node_pipeline: Pipeline,
    mock_config: ConfigLike,
    mock_dm: DataManagerLike,
    raw_df: pl.DataFrame,
) -> None:
    context = two_node_pipeline.run(
        config=mock_config,
        dm=mock_dm,
        context={"raw_df_product": raw_df},
    )
    assert "df_product" in context
    assert "df_product_enriched" in context


def test_pipeline_passes_output_of_first_node_to_second(
    two_node_pipeline: Pipeline,
    mock_config: ConfigLike,
    mock_dm: DataManagerLike,
    raw_df: pl.DataFrame,
) -> None:
    context = two_node_pipeline.run(
        config=mock_config,
        dm=mock_dm,
        context={"raw_df_product": raw_df},
    )
    assert "status" in context["df_product_enriched"].columns


def test_pipeline_does_not_mutate_input_context(
    two_node_pipeline: Pipeline,
    mock_config: ConfigLike,
    mock_dm: DataManagerLike,
    raw_df: pl.DataFrame,
) -> None:
    original_context = {"raw_df_product": raw_df}
    two_node_pipeline.run(config=mock_config, dm=mock_dm, context=original_context)
    assert list(original_context.keys()) == ["raw_df_product"]


def test_pipeline_returns_accumulated_context(
    two_node_pipeline: Pipeline,
    mock_config: ConfigLike,
    mock_dm: DataManagerLike,
    raw_df: pl.DataFrame,
) -> None:
    context = two_node_pipeline.run(
        config=mock_config,
        dm=mock_dm,
        context={"raw_df_product": raw_df},
    )
    assert "raw_df_product" in context
    assert "df_product" in context
    assert "df_product_enriched" in context


# ---------------------------------------------------------------------------
# Auto-load
# ---------------------------------------------------------------------------


def test_auto_load_when_input_missing_from_context(
    mock_config: MagicMock, mock_dm: MagicMock, raw_df: pl.DataFrame
) -> None:
    mock_config.get.side_effect = lambda key, *args, **kwargs: (
        "data/raw/raw_df_product.parquet" if key == "data.raw_df_product" else None
    )
    mock_dm.read.return_value = raw_df

    pipeline = Pipeline(
        name="run_ingestion",
        nodes=[Node(func=ingest_products_node, outputs=["df_product"])],
    )
    context = pipeline.run(config=mock_config, dm=mock_dm)

    mock_dm.read.assert_called_once_with("data/raw/raw_df_product.parquet")
    assert "df_product" in context


def test_missing_input_raises_when_no_config_path(
    mock_config: MagicMock, mock_dm: MagicMock
) -> None:
    mock_config.get.return_value = None

    pipeline = Pipeline(
        name="run_ingestion",
        nodes=[Node(func=ingest_products_node, outputs=["df_product"])],
    )
    with pytest.raises(MissingInputError) as exc_info:
        pipeline.run(config=mock_config, dm=mock_dm)

    assert "raw_df_product" in str(exc_info.value)
    assert "ingest_products_node" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Auto-dump
# ---------------------------------------------------------------------------


def test_auto_dump_when_path_is_configured(
    mock_config: MagicMock, mock_dm: MagicMock, raw_df: pl.DataFrame
) -> None:
    mock_config.get.side_effect = lambda key, *args, **kwargs: (
        "data/ingestion/df_product.parquet" if key == "data.df_product" else None
    )

    pipeline = Pipeline(
        name="run_ingestion",
        nodes=[Node(func=ingest_products_node, outputs=["df_product"])],
    )
    pipeline.run(config=mock_config, dm=mock_dm, context={"raw_df_product": raw_df})

    mock_dm.write.assert_called_once()
    args = mock_dm.write.call_args
    assert args[0][1] == "data/ingestion/df_product.parquet"


def test_no_dump_when_path_not_configured(
    mock_config: MagicMock, mock_dm: MagicMock, raw_df: pl.DataFrame
) -> None:
    mock_config.get.return_value = None

    pipeline = Pipeline(
        name="run_ingestion",
        nodes=[Node(func=ingest_products_node, outputs=["df_product"])],
    )
    pipeline.run(config=mock_config, dm=mock_dm, context={"raw_df_product": raw_df})
    mock_dm.write.assert_not_called()


def test_dump_failure_raises_data_dump_error(
    mock_config: MagicMock, mock_dm: MagicMock, raw_df: pl.DataFrame
) -> None:
    mock_config.get.side_effect = lambda key, *args, **kwargs: (
        "data/ingestion/df_product.parquet" if key == "data.df_product" else None
    )
    mock_dm.write.side_effect = OSError("disk full")

    pipeline = Pipeline(
        name="run_ingestion",
        nodes=[Node(func=ingest_products_node, outputs=["df_product"])],
    )
    with pytest.raises(DataDumpError) as exc_info:
        pipeline.run(
            config=mock_config,
            dm=mock_dm,
            context={"raw_df_product": raw_df},
        )

    assert exc_info.value.key == "df_product"
    assert "disk full" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Fail-fast
# ---------------------------------------------------------------------------


def test_fail_fast_on_node_error(
    mock_config: ConfigLike, mock_dm: DataManagerLike, raw_df: pl.DataFrame
) -> None:
    pipeline = Pipeline(
        name="run_ingestion",
        nodes=[
            Node(func=ingest_products_node, outputs=["df_product"]),
            Node(func=pipeline_failing_node, outputs=["df_out"]),
            Node(func=never_called_node, outputs=["df_final"]),
        ],
    )
    with pytest.raises(NodeExecutionError):
        pipeline.run(
            config=mock_config,
            dm=mock_dm,
            context={"raw_df_product": raw_df},
        )


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


def test_repr(two_node_pipeline: Pipeline) -> None:
    r = repr(two_node_pipeline)
    assert "run_ingestion" in r
    assert "ingest_products_node" in r
    assert "enrich_products_node" in r
