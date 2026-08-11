# SPDX-FileCopyrightText: 2026 Vane contributors
#
# SPDX-License-Identifier: Apache-2.0

# Build the directly vendored Lance extension against Vane's DuckDB fork.  The
# extension CMake file intentionally defines only a static target, so this
# registration cannot produce a separately loadable DuckDB binary.
set(_VANE_LANCE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/external/lance-duckdb")
set(_VANE_LANCE_TEST_DIR "${_VANE_LANCE_SOURCE_DIR}/test/sql")

# Some upstream SQLLogicTests intentionally mutate Lance datasets.  Run those
# tests from a generated copy so repeated test runs never alter vendored source
# fixtures.  Runtime-only builds do not stage or package the test tree.
if(BUILD_UNITTESTS)
  set(_VANE_LANCE_TEST_ROOT "${CMAKE_BINARY_DIR}/lance-extension-tests")
  file(REMOVE_RECURSE "${_VANE_LANCE_TEST_ROOT}")
  file(COPY "${_VANE_LANCE_SOURCE_DIR}/test"
       DESTINATION "${_VANE_LANCE_TEST_ROOT}")
  set(_VANE_LANCE_TEST_DIR "${_VANE_LANCE_TEST_ROOT}/test/sql")
endif()

duckdb_extension_load(
  lance
  LOAD_TESTS
  SOURCE_DIR
  "${_VANE_LANCE_SOURCE_DIR}"
  TEST_DIR
  "${_VANE_LANCE_TEST_DIR}"
  EXTENSION_VERSION
  "63c2446f7d9c8a59fd73a49fededb0c3725cc192")
