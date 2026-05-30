# pipeline-orchestrator

Lightweight pipeline orchestrator for Python data projects.

> **Important:** This is not a standalone application. This code is built to be imported as a library.

Pairs naturally with [dataconf-manager](https://github.com/MaximeRighini/dataconf-manager)
for config and I/O, but works with any object satisfying the `ConfigLike` and `DataManagerLike`
protocols defined in `src/orchestrator/protocols.py`.

---

## Table of Contents

- [Installation](#installation)
- [Core concepts](#core-concepts)
- [Node](#node)
- [Pipeline](#pipeline)
- [Code Quality](#code-quality)

---

## Installation

Add to your `pyproject.toml`:

```toml
dependencies = [
    "pipeline-orchestrator @ git+https://github.com/MaximeRighini/pipeline-orchestrator",
]
```

---

## Core concepts

The orchestrator is built around two classes: `Node` and `Pipeline`.

A **Node** wraps a single callable. It derives its name and input keys automatically
from the function signature, and declares its output keys explicitly. It handles
logging, timing, and error wrapping -- business logic stays in the callable.

A **Pipeline** executes an ordered list of nodes. It manages the shared in-memory
context that flows between nodes, injects `config` into every node, and handles
automatic I/O: loading inputs from disk when they are missing from context,
and dumping outputs to disk when a data path is configured.

Data paths are config-driven, not hardcoded. A key like `df_product` maps to a path
via `config.get("data.df_product")`. If no path is configured, the key lives in memory
only and is never written to disk.

---

## Node

Wraps a callable into a named, self-describing pipeline step.

**Function signature convention:**

```python
def build_product_table_node(
    config: ConfigManager,
    raw_df_product: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    df = clean_column_names(raw_df_product)
    df = filter_active_products(df, config.get("ingestion.active_status"))
    return {"df_product": df}
```

Rules:

- First parameter must be `config` -- injected automatically by the Pipeline.
- All other parameter names are the input keys the Pipeline will resolve.
- Return value must be a dict whose keys match the declared `outputs`.

**Usage:**

```python
from orchestrator import Node

node = Node(func=build_product_table_node, outputs=["df_product"])

# Derived automatically from the function:
node.name    # -> "build_product_table_node"
node.inputs  # -> ["raw_df_product"]
```

**Naming convention:** `verb_noun_node`

Examples: `build_product_table_node`, `compute_lca_impact_node`, `export_results_node`

**Behavior:**

- Raises `NodeExecutionError` if the callable raises any exception.
- Raises `NodeOutputKeyError` if the callable does not return all declared output keys.

---

## Pipeline

Executes an ordered list of nodes, managing their shared context and I/O.

**Usage:**

```python
from orchestrator import Node, Pipeline

ingestion_pipeline = Pipeline(
    name="run_ingestion",
    nodes=[
        Node(func=build_product_table_node,  outputs=["df_product"]),
        Node(func=build_material_table_node, outputs=["df_material"]),
    ],
)
```

In `run.py`:

```python
config = ConfigManager("config/", market="FR", env="prod")
dm = DataManager()

context = ingestion_pipeline.run(config=config, dm=dm)
# context now holds df_product and df_material
```

**Auto-load:** if a node requires `raw_df_product` and it is not in context,
the pipeline looks up `config.get("data.raw_df_product")` and loads from that path via
`DataManager`. Raises `MissingInputError` if neither source is available.

**Auto-dump:** after each node, outputs with a configured data path are written to disk
automatically. Keys with no configured path are kept in memory only.

**Data config convention** -- add to `config/data.yaml`:

```yaml
data:
  raw_df_product: "data/{market}/raw/raw_df_product.parquet"
  df_product:     "data/{market}/ingestion/df_product.parquet"
```

`{market}` (and any other placeholder) is resolved by `ConfigManager` at init time,
before the pipeline runs.

**Behavior:**

- Fail-fast. The first node failure stops the pipeline immediately.
- Does not mutate the input context dict.
- Returns the accumulated context with all node outputs.
- Raises `MissingInputError` if a required input cannot be resolved.
- Raises `DataDumpError` if writing a node output to disk fails.

**Naming convention for pipelines:** `verb_domain`

Examples: `run_ingestion`, `run_lca`, `run_export`

---

## Code Quality

Common tasks are available via `make` to simplify the developer experience.

```bash
make lint-fix      # Auto-fix formatting, style, and import order
make lint-verify   # Read-only checks -- what CI runs
make test          # Run unit tests
make all           # lint-fix -> lint-verify -> test
make clean         # Remove all cache directories
```

This package enforces code quality at three stages to keep the codebase clean
and ensure that what works locally also works in CI.

1. **`make lint-verify`** runs Ruff and Mypy in read-only mode -- catch style and type errors early.
2. **Pre-commit hooks** ensure badly formatted or broken code never reaches the remote repository.
3. **GitHub Actions** triggers on every push and blocks any pull request that fails linting or tests.
