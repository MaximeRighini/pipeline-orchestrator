"""
Node: atomic unit of execution in a pipeline.

A node wraps a single callable and gives it:
- an identity  : derived automatically from the function name
- a contract   : input keys inferred from the function signature,
                 output keys declared explicitly
- observability: structured logs and execution timing

Design principles
-----------------
- A node only calls its function and handles cross-cutting concerns
  (logging, timing, error wrapping). Business logic lives in the callable.
- No I/O. The node never touches the filesystem. Loading inputs from disk
  and dumping outputs are the Pipeline's responsibility.
- No schema validation. Validation belongs in the business codebase, applied
  via decorators on the node function. This keeps the package dependency-free
  and co-locates validation with the data contracts it enforces.
- Zero redundancy. The function name is the node name. Input keys are inferred
  from the function signature. No need to declare them twice.

Naming convention
-----------------
Node function names follow the pattern: verb_noun_node
Examples: build_product_table_node, compute_lca_impact_node, export_results_node
"""

import inspect
import logging
import time
from collections.abc import Callable
from typing import Any

import polars as pl

from orchestrator.exceptions import NodeExecutionError, NodeOutputKeyError

logger = logging.getLogger(__name__)

# "config" is always injected by the Pipeline and never resolved from context.
_CONFIG_PARAM: str = "config"


class Node:
    """
    Wraps a callable into a named, self-describing pipeline step.

    Parameters
    ----------
    func:
        The callable that implements the business logic.

        Expected signature::

            def my_node(
                config: ConfigManager,
                input_key: pl.DataFrame,
                ...
            ) -> dict[str, pl.DataFrame]:

        Rules:
        - First parameter must be ``config`` (injected by the Pipeline).
        - All other parameter names are the input keys the Pipeline will
          resolve from context or load from disk.
        - Return value must be a dict whose keys match ``outputs``.

    outputs:
        Keys this node promises to produce. Used by the Pipeline to validate
        the return value and route data to downstream nodes.

    Derived attributes
    ------------------
    name:
        ``func.__name__`` -- unique identifier used in logs and error messages.
    inputs:
        Parameter names of ``func`` excluding ``config`` -- the keys the
        Pipeline must resolve before calling this node.

    Example
    -------
    In src/nodes/ingestion_nodes.py::

        def build_product_table_node(
            config: ConfigManager,
            raw_df_product: pl.DataFrame,
        ) -> dict[str, pl.DataFrame]:
            df = clean_column_names(raw_df_product)
            df = filter_active_products(df, config.get("ingestion.active_status"))
            return {"df_product": df}

    In src/pipelines/ingestion_pipeline.py::

        Node(func=build_product_table_node, outputs=["df_product"])
        # node.name   -> "build_product_table_node"
        # node.inputs -> ["raw_df_product"]  (inferred from signature)
    """

    def __init__(
        self,
        func: Callable[..., dict[str, pl.DataFrame]],
        outputs: list[str],
    ) -> None:
        self.func = func
        self.outputs = outputs
        self.name: str = func.__name__

        sig = inspect.signature(func)
        self.inputs: list[str] = [p for p in sig.parameters if p != _CONFIG_PARAM]

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, kwargs: dict[str, Any]) -> dict[str, pl.DataFrame]:
        """
        Calls the wrapped function with pre-resolved kwargs.

        The Pipeline builds ``kwargs`` by resolving all inputs before calling
        this method. The node only handles execution, logging and error wrapping.

        Parameters
        ----------
        kwargs:
            Fully resolved arguments -- includes ``config`` and all keys
            in ``self.inputs``.

        Returns
        -------
        dict[str, pl.DataFrame]
            The return value of ``func``, after output key validation.

        Raises
        ------
        NodeExecutionError
            If ``func`` raises any exception.
        NodeOutputKeyError
            If ``func`` does not return all declared output keys.
        """
        logger.info(f"[{self.name}] Starting")
        start = time.perf_counter()

        try:
            result = self.func(**kwargs)
        except Exception as exc:
            elapsed = time.perf_counter() - start
            logger.error(
                f"[{self.name}] Failed after {elapsed:.3f}s"
                f" - {type(exc).__name__}: {exc}"
            )
            raise NodeExecutionError(self.name, exc) from exc

        elapsed = time.perf_counter() - start

        received = list(result.keys())
        if not all(key in received for key in self.outputs):
            raise NodeOutputKeyError(self.name, self.outputs, received)

        logger.info(f"[{self.name}] Completed in {elapsed:.3f}s")
        return result

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Node("
            f"name={self.name!r}, "
            f"inputs={self.inputs!r}, "
            f"outputs={self.outputs!r})"
        )
