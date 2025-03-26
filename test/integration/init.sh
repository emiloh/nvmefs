#!/bin/bash

init_environment() {
    current_dir=$1
    if [ -e "${current_dir}/.venv" ]; then
        source "${current_dir}/.venv/bin/activate"
        echo "Activating environment..."
    else
        echo "Creating environment and installing dependencies: ${current_dir}..."
        python3 -m venv "${current_dir}/.venv"
        source "${current_dir}/.venv/bin/activate"
        python3 -m pip install "${current_dir}/../../duckdb/tools/pythonpkg/"
        python3 -m pip install -r "${current_dir}/requirements.txt"
    fi
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

init_environment $SCRIPT_DIR
