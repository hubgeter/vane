# SPDX-FileCopyrightText: 2026 Vane contributors
#
# SPDX-License-Identifier: Apache-2.0

# Fetch the independently maintained Lance extension at an immutable revision
# and build it against Vane's DuckDB fork. The extension defines only a static
# target, so this cannot produce a separately loadable DuckDB binary. For local
# or offline development, DuckDB's extension loader accepts an existing source
# checkout through the DUCKDB_LANCE_DIRECTORY environment variable.
set(_VANE_LANCE_GIT_URL "https://github.com/hubgeter/lance-duckdb.git")
set(_VANE_LANCE_GIT_TAG "856203ca15bdf21e1f6c2962038ccbd4598573b0")

duckdb_extension_load(
  lance
  LOAD_TESTS
  GIT_URL
  "${_VANE_LANCE_GIT_URL}"
  GIT_TAG
  "${_VANE_LANCE_GIT_TAG}"
  EXTENSION_VERSION
  "${_VANE_LANCE_GIT_TAG}")

# Some upstream SQLLogicTests intentionally mutate Lance datasets. Run those
# tests from a generated copy so repeated test runs never alter either the
# fetched checkout or a DUCKDB_LANCE_DIRECTORY development checkout.
if(BUILD_UNITTESTS)
  set(_VANE_LANCE_TEST_ROOT "${CMAKE_BINARY_DIR}/lance-extension-tests")
  file(REMOVE_RECURSE "${_VANE_LANCE_TEST_ROOT}")
  file(COPY "${DUCKDB_EXTENSION_LANCE_PATH}/test"
       DESTINATION "${_VANE_LANCE_TEST_ROOT}")
  set(DUCKDB_EXTENSION_LANCE_TEST_PATH "${_VANE_LANCE_TEST_ROOT}/test/sql")
endif()
