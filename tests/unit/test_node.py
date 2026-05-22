import polars as pl
import pytest

from orchestrator.exceptions import NodeExecutionError, NodeOutputKeyError
from orchestrator.node import Node
from orchestrator.protocols import ConfigLike
from tests.unit.helpers import (
    failing_node,
    good_node,
    multi_output_node,
    wrong_outputs_node,
)

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_name_derived_from_function() -> None:
    node = Node(func=good_node, outputs=["df_out"])
    assert node.name == "good_node"


def test_inputs_inferred_from_signature() -> None:
    node = Node(func=good_node, outputs=["df_out"])
    assert node.inputs == ["raw_df"]


def test_config_excluded_from_inputs() -> None:
    node = Node(func=multi_output_node, outputs=["df_merged", "df_summary"])
    assert "config" not in node.inputs
    assert node.inputs == ["df_a", "df_b"]


def test_outputs_stored() -> None:
    node = Node(func=multi_output_node, outputs=["df_merged", "df_summary"])
    assert node.outputs == ["df_merged", "df_summary"]


def test_repr_contains_key_info() -> None:
    node = Node(func=good_node, outputs=["df_out"])
    r = repr(node)
    assert "good_node" in r
    assert "raw_df" in r
    assert "df_out" in r


# ---------------------------------------------------------------------------
# Execution: happy path
# ---------------------------------------------------------------------------


def test_run_returns_correct_output(
    mock_config: ConfigLike, df_products: pl.DataFrame
) -> None:
    node = Node(func=good_node, outputs=["df_out"])
    result = node.run({"config": mock_config, "raw_df": df_products})
    assert "df_out" in result
    assert "processed" in result["df_out"].columns


def test_run_multi_output(
    mock_config: ConfigLike,
    df_products: pl.DataFrame,
    df_materials: pl.DataFrame,
) -> None:
    node = Node(func=multi_output_node, outputs=["df_merged", "df_summary"])
    result = node.run(
        {"config": mock_config, "df_a": df_products, "df_b": df_materials}
    )
    assert set(result.keys()) == {"df_merged", "df_summary"}


# ---------------------------------------------------------------------------
# Execution: error cases
# ---------------------------------------------------------------------------


def test_run_wraps_exception_in_node_execution_error(
    mock_config: ConfigLike, df_products: pl.DataFrame
) -> None:
    node = Node(func=failing_node, outputs=["df_out"])
    with pytest.raises(NodeExecutionError) as exc_info:
        node.run({"config": mock_config, "raw_df": df_products})
    assert exc_info.value.node_name == "failing_node"
    assert isinstance(exc_info.value.cause, ValueError)


def test_run_raises_on_missing_output_key(
    mock_config: ConfigLike, df_products: pl.DataFrame
) -> None:
    node = Node(func=wrong_outputs_node, outputs=["df_out"])
    with pytest.raises(NodeOutputKeyError) as exc_info:
        node.run({"config": mock_config, "raw_df": df_products})
    assert "df_out" in str(exc_info.value)
    assert "wrong_key" in str(exc_info.value)
