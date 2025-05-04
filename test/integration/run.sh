#!/bin/bash

EXTENSION_DIR=$1
CURRENT_DIR=$(pwd)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "${SCRIPT_DIR}/init.sh"

cd "${SCRIPT_DIR}"
pytest --extension_dir_path="../../build/release/extension/nvmefs" --device="/dev/nvme1"
cd "${CURRENT_DIR}"