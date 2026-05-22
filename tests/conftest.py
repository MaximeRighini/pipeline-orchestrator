from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def mock_dm() -> MagicMock:
    """Mock DataManager satisfying the DataManagerLike protocol."""
    dm = MagicMock()
    dm.read.return_value = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    dm.write.return_value = None
    return dm


@pytest.fixture
def mock_config() -> MagicMock:
    """Mock ConfigManager satisfying the ConfigLike protocol."""
    config = MagicMock()
    config.get.return_value = None
    return config


@pytest.fixture
def df_products() -> pl.DataFrame:
    return pl.DataFrame(
        {"product_id": [1, 2], "name": ["A", "B"], "price": [9.99, 4.99]}
    )


@pytest.fixture
def df_materials() -> pl.DataFrame:
    return pl.DataFrame({"material_id": [10, 20], "label": ["Steel", "Plastic"]})
