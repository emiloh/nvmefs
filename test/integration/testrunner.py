import argparse
from dataclasses import dataclass
import os
import duckdb
import pytest

from utils.device import NvmeDevice

@dataclass
class Arguments:
    extension_dir_path: str = "../../build/release/extension/nvmefs"

    def valid(self) -> bool:
        
        return True

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "-e",
            "--extension_dir_path",
            type=str,
            help="Path to the directory where the extension is located",
            default="../../build/release/extension")

        args = parser.parse_args()
        
        arguments: Arguments = Arguments(
            args.extension_dir_path
        )

        if not arguments.valid():
            parser.print_help()
            exit(1)
        
        return arguments

def pytest_addoption(parser):
    parser.addoption(
        "--extension_dir_path",
        type=str,
        default="../../build/release/extension/nvmefs",
        help="Path to the directory where the extension is located"
    )

    parser.addoption(
        "--device_path",
        type=str,
        help="Path to the device to be used for the extension",
        default="/dev/ng1n1"
    )

@pytest.fixture(scope="session")
def extension_path(pytestconfig):
    return pytestconfig.getoption("extension_dir_path")

@pytest.fixture(scope="session")
def device_path(pytestconfig):
    return pytestconfig.getoption("device_path")

@pytest.fixture(scope="module")
def device(device_path):
    device = NvmeDevice(device_path)

    yield device

    device.deallocate(1)

if __name__ == "__main__":
    pytest.main()

    # args = Arguments.parse_args()
    # extension_filepath = os.path.join(args.extension_dir_path, "nvmefs.duckdb_extension")
    # con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

    # con.sql("PRAGMA platform;").show()
    # con.load_extension(extension_filepath)
    # con.sql("from duckdb_extensions()").show()

    # duckdb.connect("nvmefs:///test.db")