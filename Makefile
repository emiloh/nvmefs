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

gtest: release
	@./build/release/gtests/nvmefs_gtest

clean-gtest: clean gtest

clean-run: clean release run

e2e-test: release
	@bash "./test/e2e/run.sh" './build/release/extension/nvmefs'
	@rm -rf ./build/release/extension/nvmefs/v1.2.0
	@EXTENSION_PATH="$${HOME}/.duckdb/extensions/v1.2.0/$$(cat build/release/duckdb_platform_out)/nvmefs.*" && rm $$EXTENSION_PATH
