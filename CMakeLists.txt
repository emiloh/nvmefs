cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME nvmefs)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES 
  src/nvmefs_extension.cpp
  src/nvmefs_temporary_block_manager.cpp
  src/nvmefs.cpp
  src/nvmefs_config.cpp
  src/device.cpp
  src/nvme_device.cpp
  src/temporary_file_metadata_manager.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link xNVMe
find_library(XNVME_LIB xnvme)
target_link_libraries(${EXTENSION_NAME} ${XNVME_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${XNVME_LIB})

add_subdirectory(test)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
