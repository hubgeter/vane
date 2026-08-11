// SPDX-FileCopyrightText: 2026 Vane contributors
// SPDX-License-Identifier: MIT

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/extension_scan_split_provider.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! An extension-owned, serializable unit of scan work. The payload is opaque
//! to the distributed planner; only the extension that produced it may decode
//! it. Cardinality and byte estimates are used solely for grouping/admission.
struct ExtensionScanSplit {
	string payload;
	idx_t estimated_cardinality = 0;
	idx_t estimated_bytes = 0;
};

//! ExtensionScanSplitProvider lets table functions participate in distributed
//! scans without pretending their work units are filesystem paths. Handles and
//! other process-local state must never be embedded in a payload.
class ExtensionScanSplitProvider {
public:
	virtual ~ExtensionScanSplitProvider() = default;

	//! Return all logical work units for the snapshot captured by bind data.
	virtual vector<ExtensionScanSplit> GetScanSplits() const = 0;

	//! Restrict this bind data to the supplied work units on a worker.
	virtual void SetScanSplits(const vector<string> &splits) = 0;

	//! CPU permits requested by each task produced for this scan. The worker
	//! clamps this value to its admitted CPU capacity.
	virtual idx_t GetTaskCpuSlots() const {
		return 1;
	}
};

} // namespace duckdb
