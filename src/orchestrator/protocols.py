"""
Structural interfaces (Protocols) for the objects the orchestrator depends on.

Internal to the package, not part of the public API.

The orchestrator uses duck typing and never imports ConfigManager or DataManager
directly. These Protocols exist solely to enable static analysis within the
package and its tests, without creating a hard dependency on dataconf-manager.
"""

from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class ConfigLike(Protocol):
    """
    Structural interface for a config manager.

    Any object exposing a ``.get()`` method with this signature is compatible
    with the orchestrator, regardless of its concrete type.

    The dataconf-manager ``ConfigManager`` satisfies this interface out of the box.
    """

    def get(self, key: str, default: Any = None) -> Any:
        """Returns the config value at ``key`` using dot-notation traversal."""
        ...


@runtime_checkable
class DataManagerLike(Protocol):
    """
    Structural interface for a data manager.

    Any object exposing ``.read()`` and ``.write()`` with these signatures
    is compatible with the orchestrator.

    The dataconf-manager ``DataManager`` satisfies this interface out of the box.
    """

    def read(self, path: str | Path, **kwargs: Any) -> pl.DataFrame:
        """Reads a file from disk and returns a Polars DataFrame."""
        ...

    def write(self, df: pl.DataFrame, path: str | Path, **kwargs: Any) -> None:
        """Writes a Polars DataFrame to disk."""
        ...
