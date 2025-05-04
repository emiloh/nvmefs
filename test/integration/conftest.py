import pytest
import duckdb

from utils.device import NvmeDevice, setup_device

def pytest_addoption(parser):

    parser.addoption(
        "--extension_dir_path",
        type=str,
        help="Path to the extension directory",
        default="../../build/release/extension/nvmefs"
    )

    parser.addoption(
        "--device",
        type=str,
        help="Path to the device to be used for the extension",
        default="/dev/nvme1"
    )

@pytest.fixture(scope="session")
def configure(pytestconfig):
    path = pytestconfig.getoption("extension_dir_path")
    duckdb.sql(f"INSTALL nvmefs FROM '{path}';") # Ensures that when we call "LOAD nvmefs" that it can be found in the extension directory

    return True

@pytest.fixture(scope="session")
def device_path(configure, pytestconfig):
    if configure is False:
        pytest.fail("Extension not configured properly") 

    return pytestconfig.getoption("device")



@pytest.fixture(scope="module")
def device(device_path):
    device = NvmeDevice(device_path)
    namespace = setup_device(device, 1, enable_fdp=True)

    yield namespace

    device.reset()