import tempfile
from unittest.mock import MagicMock

import h5py
import pytest

from nexgen.nxs_write.NXmxWriter import NXmxFileWriter


@pytest.fixture
def dummy_nexus_file():
    test_hdf_file = tempfile.TemporaryFile()
    yield h5py.File(test_hdf_file, "w")


@pytest.fixture
def dummy_NXmxWriter():
    with tempfile.NamedTemporaryFile(suffix=".nxs", delete=True) as test_nxs_file:
        yield NXmxFileWriter(
            test_nxs_file.name,
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            10,
        )
