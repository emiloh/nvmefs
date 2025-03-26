PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=nvmefs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

ifeq ($(BUILD_PYTHON),1)
	EXT_FLAGS=-DBUILD_PYTHON=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

run:
	@./build/release/duckdb

dealloc-device:
	@sh ./scripts/dealloc_device.sh

clean-run: clean release run

integration-test: release
	@bash "./test/integration/run.sh" './build/release/extension/nvmefs'
