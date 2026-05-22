import polars as pl
import pytest

from orchestrator.node import Node
from orchestrator.pipeline import Pipeline
from tests.unit.helpers import enrich_products_node, ingest_products_node


@pytest.fixture
def two_node_pipeline() -> Pipeline:
    return Pipeline(
        name="run_ingestion",
        nodes=[
            Node(func=ingest_products_node, outputs=["df_product"]),
            Node(func=enrich_products_node, outputs=["df_product_enriched"]),
        ],
    )


@pytest.fixture
def raw_df() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
