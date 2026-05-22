"""
Pipeline: ordered sequence of nodes with automatic I/O resolution.

The pipeline is the only place where orchestration concerns live:
- injecting ``config`` into every node
- resolving node inputs (in-memory context -> DataManager auto-load -> fail)
- merging node outputs back into the shared context
- auto-dumping outputs to disk when a data path is configured
- structured logging with per-node and total execution time

Design principles
-----------------
- Fail-fast. The first node failure stops the pipeline immediately.
- Context dict as the shared bus. Data flows between nodes via a plain
  dict[str, pl.DataFrame]. No hidden global state.
- Config-driven I/O. Data paths live in config/data.yaml, not in code.
  Path templating (e.g. ``data/{market}/...``) is resolved by ConfigManager
  at init time, before the pipeline runs.
- Node purity. The pipeline resolves and passes inputs; nodes just run.

Dependency on dataconf-manager
-------------------------------
This package is designed to work with DataManager and ConfigManager from
the dataconf-manager package (https://github.com/MaximeRighini/dataconf-manager).

Naming convention
-----------------
Pipeline names follow the pattern: verb_domain
Examples: run_ingestion, run_lca, run_export

Data config convention
----------------------
To enable auto-load / auto-dump for a key, add to config/data.yaml::

    data:
      raw_df_product: "data/{market}/raw/raw_df_product.parquet"
      df_product:     "data/{market}/ingestion/df_product.parquet"

{market} (and any other placeholder) is resolved by ConfigManager at init::

    config = ConfigManager("config/", market="FR")
    # config.get("data.df_product") -> "data/FR/ingestion/df_product.parquet"

Keys with no configured path are carried in memory only and not dumped.
"""

import logging
import time
from typing import Any

import polars as pl

from orchestrator.exceptions import DataDumpError, MissingInputError
from orchestrator.node import Node
from orchestrator.protocols import ConfigLike, DataManagerLike

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Executes an ordered list of nodes, managing their shared context.

    Parameters
    ----------
    name:
        Identifier used in logs. Convention: verb_domain in snake_case.
    nodes:
        Ordered list of nodes to execute.

    Example
    -------
    In src/pipelines/ingestion_pipeline.py::

        from orchestrator import Node, Pipeline
        from nodes.ingestion_nodes import (
            build_product_table_node,
            build_material_table_node,
        )

        ingestion_pipeline = Pipeline(
            name="run_ingestion",
            nodes=[
                Node(func=build_product_table_node,  outputs=["df_product"]),
                Node(func=build_material_table_node, outputs=["df_material"]),
            ],
        )

    In run.py::

        config = ConfigManager("config/", market=market)
        dm = DataManager()
        ingestion_pipeline.run(config=config, dm=dm)
    """

    def __init__(self, name: str, nodes: list[Node]) -> None:
        self.name = name
        self.nodes = nodes

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(
        self,
        config: ConfigLike,
        dm: DataManagerLike,
        context: dict[str, pl.DataFrame] | None = None,
    ) -> dict[str, pl.DataFrame]:
        """
        Executes all nodes in order, managing shared context and I/O.

        For each node, the pipeline:
        1. Resolves inputs: context first, then DataManager auto-load.
        2. Calls node.run().
        3. Merges outputs into the shared context.
        4. Auto-dumps outputs to disk for keys with a configured data path.

        Parameters
        ----------
        config:
            ConfigManager instance. Injected into every node as ``config``.
            Also used to look up data paths via config.get("data.<key>").
            Path templates (e.g. {market}) must be resolved at ConfigManager
            init time, not here.
        dm:
            DataManager instance. Used for auto-loading inputs and
            auto-dumping outputs.
        context:
            Pre-populated context dict. Useful for injecting test fixtures
            or chaining pipelines. Defaults to an empty dict.

        Returns
        -------
        dict[str, pl.DataFrame]
            Final context dict with all node outputs accumulated during the run.

        Raises
        ------
        MissingInputError
            If a required input is absent from context and has no configured
            data path.
        NodeExecutionError
            If a node's callable raises an unexpected exception.
        NodeOutputKeyError
            If a node does not return its declared output keys.
        DataDumpError
            If writing a node output to disk fails.
        """
        context = dict(context) if context else {}
        node_count = len(self.nodes)

        logger.info(f"[Pipeline: {self.name}] Starting - {node_count} node(s)")
        pipeline_start = time.perf_counter()

        for i, node in enumerate(self.nodes, start=1):
            logger.info(f"[Pipeline: {self.name}] Node {i}/{node_count}: {node.name}")

            # 1. Resolve inputs
            kwargs: dict[str, Any] = {"config": config}
            for input_key in node.inputs:
                if input_key in context:
                    kwargs[input_key] = context[input_key]
                else:
                    kwargs[input_key] = self._auto_load(
                        node_name=node.name,
                        input_key=input_key,
                        config=config,
                        dm=dm,
                    )

            # 2. Run -- fail-fast: any exception propagates immediately
            result = node.run(kwargs)

            # 3. Merge outputs into shared context
            context.update(result)

            # 4. Auto-dump outputs to disk
            for key, df in result.items():
                self._auto_dump(key=key, df=df, config=config, dm=dm)

        elapsed = time.perf_counter() - pipeline_start
        logger.info(
            f"[Pipeline: {self.name}] Completed in {elapsed:.3f}s"
            f" - {node_count} node(s) ran successfully"
        )
        return context

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_data_path(self, config: ConfigLike, key: str) -> str | None:
        """Returns the configured data path for ``key``, or None if absent."""
        return config.get(f"data.{key}")  # type: ignore[no-any-return]

    def _auto_load(
        self,
        node_name: str,
        input_key: str,
        config: ConfigLike,
        dm: DataManagerLike,
    ) -> pl.DataFrame:
        """
        Loads ``input_key`` from disk via DataManager.
        Raises MissingInputError if no path is configured.
        """
        path = self._get_data_path(config, input_key)
        if path is None:
            raise MissingInputError(node_name, input_key)

        logger.info(f"[Pipeline: {self.name}] Auto-loading '{input_key}' from '{path}'")
        return dm.read(path)

    def _auto_dump(
        self,
        key: str,
        df: pl.DataFrame,
        config: ConfigLike,
        dm: DataManagerLike,
    ) -> None:
        """
        Writes ``df`` to disk if a path is configured for ``key``.
        Keys with no configured path are skipped silently.
        Raises DataDumpError on write failure.
        """
        path = self._get_data_path(config, key)
        if path is None:
            return

        logger.info(f"[Pipeline: {self.name}] Auto-dumping '{key}' to '{path}'")
        try:
            dm.write(df, path)
        except Exception as exc:
            raise DataDumpError(key, path, exc) from exc

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        node_names = [n.name for n in self.nodes]
        return f"Pipeline(name={self.name!r}, nodes={node_names!r})"
