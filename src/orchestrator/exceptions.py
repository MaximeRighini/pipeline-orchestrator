"""
Orchestrator exceptions.

Each exception is raised with enough context to pinpoint the failure
without digging through the full traceback.
"""


class OrchestratorError(Exception):
    """Base class for all orchestrator exceptions."""


# ---------------------------------------------------------------------------
# Node-level
# ---------------------------------------------------------------------------


class NodeExecutionError(OrchestratorError):
    """
    Raised when a node's callable raises an unexpected exception.

    Wraps the original exception and exposes ``node_name`` so the pipeline
    can log a clear message without re-parsing the traceback.
    """

    def __init__(self, node_name: str, cause: Exception) -> None:
        self.node_name = node_name
        self.cause = cause
        super().__init__(
            f"Node '{node_name}' failed with {type(cause).__name__}: {cause}"
        )


class NodeOutputKeyError(OrchestratorError):
    """
    Raised when a node does not return all of its declared output keys.

    This is a contract violation: the node promised certain outputs but
    did not deliver them.
    """

    def __init__(
        self, node_name: str, expected: list[str], received: list[str]
    ) -> None:
        self.node_name = node_name
        missing = sorted(set(expected) - set(received))
        super().__init__(
            f"Node '{node_name}' did not return all declared output keys.\n"
            f"  Expected : {expected}\n"
            f"  Received : {received}\n"
            f"  Missing  : {missing}"
        )


# ---------------------------------------------------------------------------
# Pipeline-level
# ---------------------------------------------------------------------------


class MissingInputError(OrchestratorError):
    """
    Raised when a required input cannot be resolved for a node.

    The pipeline tries two resolution strategies in order:
    1. Pull the value from the in-memory context.
    2. Auto-load via DataManager using the path at ``data.<key>`` in config.

    If both fail, this exception is raised.
    """

    def __init__(self, node_name: str, input_key: str) -> None:
        self.node_name = node_name
        self.input_key = input_key
        super().__init__(
            f"Node '{node_name}' requires input '{input_key}' but it was not "
            f"found in the pipeline context and no path is configured at "
            f"'data.{input_key}' in the data config."
        )


class DataDumpError(OrchestratorError):
    """Raised when the pipeline fails to write a node output to disk."""

    def __init__(self, key: str, path: str, cause: Exception) -> None:
        self.key = key
        self.path = path
        self.cause = cause
        super().__init__(
            f"Failed to dump '{key}' to '{path}': {type(cause).__name__}: {cause}"
        )
