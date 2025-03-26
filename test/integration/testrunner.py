import argparse
from dataclasses import dataclass
import os
import duckdb

@dataclass
class Arguments:
    extension_dir_path: str = "../../build/release/extension"

    def valid(self) -> bool:
        
        return True

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "benchmark",
            type=str,
            help="Name of the benchmark to run(tpch)",
            default="tpch")


        args = parser.parse_args()
        
        arguments: Arguments = Arguments(
        )

        if not arguments.valid():
            parser.print_help()
            exit(1)
        
        return arguments

if __name__ == "__main__":
    args = Arguments.parse_args()
    extension_filepath = os.path.join(args.extension_dir_path, "nvmefs.duckdb_extension")

    duckdb.load_extension(extension_filepath)
    duckdb.connect("nvmefs:///test.db")