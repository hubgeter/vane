// SPDX-FileCopyrightText: 2026 Vane contributors
// SPDX-License-Identifier: MIT

#include "duckdb/execution/distributed/pipeline_node/sink.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/distributed/copy_finalize.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/distributed/plan/exchange_source_task.hpp"
#include "duckdb/execution/distributed/plan/runner.hpp"

#include <algorithm>
#include <mutex>
#include <unordered_map>

namespace duckdb {
namespace distributed {

namespace {

void MergeSingleWriterExchangeInput(TaskInput &target, const TaskInput &source) {
	if (target.kind != TaskInput::Kind::ExchangeSourceTask || source.kind != TaskInput::Kind::ExchangeSourceTask) {
		throw InvalidInputException("single-commit writer gather received a non-exchange task input");
	}
	auto merged = ExchangeSourceTaskDescriptor::DeserializeFromBytes(target.exchange_source_task_bytes);
	auto incoming = ExchangeSourceTaskDescriptor::DeserializeFromBytes(source.exchange_source_task_bytes);
	if (merged.replicated != incoming.replicated) {
		throw InvalidInputException("single-commit writer gather received incompatible exchange descriptors");
	}
	merged.partition_indices.insert(merged.partition_indices.end(), incoming.partition_indices.begin(),
	                                incoming.partition_indices.end());
	std::sort(merged.partition_indices.begin(), merged.partition_indices.end());
	merged.partition_indices.erase(std::unique(merged.partition_indices.begin(), merged.partition_indices.end()),
	                               merged.partition_indices.end());
	merged.source_handles.insert(merged.source_handles.end(), std::make_move_iterator(incoming.source_handles.begin()),
	                             std::make_move_iterator(incoming.source_handles.end()));
	merged.source_partition_count = MaxValue(merged.source_partition_count, incoming.source_partition_count);
	// All handles are consumed by the one writer task produced below.
	merged.source_task_count = 1;
	merged.mark_join_build_summary.Merge(incoming.mark_join_build_summary);
	target.exchange_source_task_bytes = merged.SerializeToBytes();
}

void MergeSingleWriterTaskInputs(WorkerTask &target, WorkerTask &source) {
	auto &target_inputs = target.mutable_inputs();
	for (auto &entry : source.mutable_inputs()) {
		auto existing = target_inputs.find(entry.first);
		if (existing == target_inputs.end()) {
			target_inputs.emplace(entry.first, std::move(entry.second));
			continue;
		}
		MergeSingleWriterExchangeInput(existing->second, entry.second);
	}
}

SubmittableTaskStream<WorkerTask> CoalesceSingleCommitWriterTasks(SubmittableTaskStream<WorkerTask> input) {
	struct CoalescingStream {
		explicit CoalescingStream(SubmittableTaskStream<WorkerTask> input_p) : input(std::move(input_p)) {
		}

		std::pair<bool, SubmittableTask<WorkerTask>> poll_next() {
			if (emitted) {
				return std::make_pair(false, SubmittableTask<WorkerTask>());
			}
			unique_ptr<WorkerTask> merged;
			while (true) {
				auto next = input.poll_next();
				if (!next.first) {
					break;
				}
				auto task = std::move(next.second).take_task();
				if (!merged) {
					merged = make_uniq<WorkerTask>(std::move(task));
				} else {
					MergeSingleWriterTaskInputs(*merged, task);
				}
			}
			emitted = true;
			if (!merged) {
				return std::make_pair(false, SubmittableTask<WorkerTask>());
			}
			return std::make_pair(true, SubmittableTask<WorkerTask>(std::move(*merged)));
		}

		std::pair<bool, SubmittableTask<WorkerTask>> try_poll_next() {
			// This is intentionally a barrier: the only writer task is not visible
			// until every upstream exchange handle has been collected.
			return poll_next();
		}

		bool is_exhausted() const {
			return emitted;
		}

		struct Iterator {
			CoalescingStream *parent = nullptr;
			std::pair<bool, SubmittableTask<WorkerTask>> current;
			Iterator() = default;
			explicit Iterator(CoalescingStream *parent_p) : parent(parent_p) {
				++(*this);
			}
			SubmittableTask<WorkerTask> operator*() {
				return std::move(current.second);
			}
			Iterator &operator++() {
				current = parent ? parent->poll_next() : std::make_pair(false, SubmittableTask<WorkerTask>());
				return *this;
			}
			bool operator==(const Iterator &other) const {
				return !current.first && !other.current.first;
			}
			bool operator!=(const Iterator &other) const {
				return !(*this == other);
			}
		};

		Iterator begin() {
			return Iterator(this);
		}
		Iterator end() {
			return Iterator();
		}

		SubmittableTaskStream<WorkerTask> input;
		bool emitted = false;
	};

	auto stream = boxed<SubmittableTask<WorkerTask>>(CoalescingStream(std::move(input)));
	return SubmittableTaskStream<WorkerTask>(std::move(stream));
}

} // namespace

static DuckPhysicalPlanRef AppendCopyOperator(DuckPhysicalPlanRef plan, DistributedCopySpec spec,
                                              const std::string &task_path) {
	if (!plan || !plan->HasRoot()) {
		throw InvalidInputException("CopySinkNode: input plan missing root");
	}
	if (!spec.bind_data) {
		throw InvalidInputException("CopySinkNode: copy bind_data is null");
	}
	auto &old_root = plan->Root();
	auto worker_return_type = spec.IsSingleCommitWriter() ? CopyFunctionReturnType::CHANGED_ROWS
	                                                      : CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	auto types = GetCopyFunctionReturnLogicalTypes(worker_return_type);

	if (spec.type != DistributedCopyType::BATCH_COPY_TO_FILE) {
		auto &copy_op = plan->Make<duckdb::PhysicalCopyToFile>(types, spec.function, std::move(spec.bind_data),
		                                                       old_root.estimated_cardinality);
		auto &cast_copy = copy_op.Cast<duckdb::PhysicalCopyToFile>();
		cast_copy.file_path = task_path;
		// Distributed COPY uses a staging directory; avoid per-task tmp renames.
		cast_copy.use_tmp_file = false;
		cast_copy.filename_pattern = spec.filename_pattern;
		cast_copy.file_extension = spec.file_extension;
		cast_copy.overwrite_mode = spec.overwrite_mode;
		cast_copy.parallel = spec.parallel;
		cast_copy.single_commit_writer = spec.IsSingleCommitWriter();
		cast_copy.task_cpu_slots = spec.task_cpu_slots;
		cast_copy.per_thread_output = spec.per_thread_output;
		cast_copy.file_size_bytes = spec.file_size_bytes;
		cast_copy.rotate = spec.rotate;
		cast_copy.return_type = worker_return_type;
		cast_copy.partition_output = spec.partition_output;
		cast_copy.write_partition_columns = spec.write_partition_columns;
		cast_copy.write_empty_file = spec.write_empty_file;
		cast_copy.hive_file_pattern = spec.hive_file_pattern;
		cast_copy.partition_columns = spec.partition_columns;
		cast_copy.names = spec.names;
		cast_copy.expected_types = spec.expected_types;
		cast_copy.children.push_back(old_root);
		plan->SetRoot(copy_op);
		return plan;
	}

	auto &copy_op = plan->Make<duckdb::PhysicalBatchCopyToFile>(types, spec.function, std::move(spec.bind_data),
	                                                            old_root.estimated_cardinality);
	auto &cast_copy = copy_op.Cast<duckdb::PhysicalBatchCopyToFile>();
	cast_copy.file_path = task_path;
	// Distributed COPY uses a staging directory; avoid per-task tmp renames.
	cast_copy.use_tmp_file = false;
	cast_copy.return_type = worker_return_type;
	cast_copy.write_empty_file = spec.write_empty_file;
	cast_copy.children.push_back(old_root);
	plan->SetRoot(copy_op);
	return plan;
}

SubmittableTaskStream<WorkerTask> CopySinkNode::produce_tasks(PlanExecutionContext &plan_context) {
	auto input_stream = child_->produce_tasks(plan_context);
	auto self = shared_from_this();
	auto node_id_val = this->node_id();
	auto node_ctx = context().to_hashmap();
	auto staging_root_base = staging_root_base_;
	auto staging_run_id = staging_run_id_;
	auto *client_context = plan_context.client_context();
	auto fragment_plan_cache = std::make_shared<std::unordered_map<const PhysicalPlan *, DuckPhysicalPlanRef>>();
	auto fragment_plan_cache_lock = std::make_shared<std::mutex>();

	if (!client_context) {
		throw InvalidInputException("CopySinkNode requires ClientContext for plan cloning");
	}
	auto &fs = FileSystem::GetFileSystem(*client_context);
	auto worker_base_res = CanonicalDistributedCopyBasePath(fs, spec_.file_path);
	if (worker_base_res.is_err()) {
		throw InvalidInputException(worker_base_res.error().what());
	}
	auto worker_base = std::move(worker_base_res).value();
	const auto single_commit_writer = spec_.IsSingleCommitWriter();
	const auto writer_started_barrier_path = writer_started_barrier_path_;
	if (single_commit_writer) {
		input_stream = CoalesceSingleCommitWriterTasks(std::move(input_stream));
	}

	return input_stream.map_tasks([self, node_id_val, node_ctx, staging_root_base, staging_run_id, client_context,
	                               worker_base, single_commit_writer, writer_started_barrier_path, fragment_plan_cache,
	                               fragment_plan_cache_lock](SubmittableTask<WorkerTask> task) mutable {
		auto *old_task = task.task();
		if (!old_task) {
			throw InvalidInputException("CopySinkNode: task missing");
		}

		const auto task_id = old_task->task_context().task_id();

		auto base_plan = old_task->plan();
		DuckPhysicalPlanRef fragment_plan;
		{
			std::lock_guard<std::mutex> guard(*fragment_plan_cache_lock);
			auto it = fragment_plan_cache->find(base_plan.get());
			if (it != fragment_plan_cache->end()) {
				fragment_plan = it->second;
			}
		}
		if (!fragment_plan) {
			auto local_spec = self->spec_.Clone();
			auto plan_template_path =
			    single_commit_writer ? local_spec.file_path : BuildCopyPlanTemplatePath(local_spec, node_id_val);
			auto cache_key = base_plan.get();
			DuckPhysicalPlanRef working_plan;
			auto rc = base_plan.use_count();
			if (rc <= 2) {
				working_plan = std::move(base_plan);
			} else {
				working_plan = ClonePhysicalPlanOrThrow(base_plan, "CopySinkNode", client_context);
			}
			auto candidate_plan =
			    AppendCopyOperator(std::move(working_plan), std::move(local_spec), plan_template_path);
			{
				std::lock_guard<std::mutex> guard(*fragment_plan_cache_lock);
				auto emplace_result = fragment_plan_cache->emplace(cache_key, candidate_plan);
				fragment_plan = emplace_result.first->second;
			}
		}

		TaskContext ctx = old_task->task_context();
		ctx.add_node_id(node_id_val);
		auto merged_ctx = MergeTaskContext(old_task->context(), node_ctx);
		if (single_commit_writer) {
			if (writer_started_barrier_path.empty()) {
				throw InvalidInputException("single-commit writer is missing its durable barrier path");
			}
			auto local_fs = FileSystem::CreateLocal();
			auto barrier_res = WriteDistributedCopyTextFileAtomically(
			    *local_fs, writer_started_barrier_path, "state=writer_started\nrun_id=" + staging_run_id + "\n");
			if (barrier_res.is_err()) {
				throw IOException("failed to persist Lance writer barrier: %s", barrier_res.error().what());
			}
			merged_ctx["single_commit_writer"] = "true";
			merged_ctx["single_commit_writer_started"] = "true";
			merged_ctx["single_commit_writer_barrier_path"] = writer_started_barrier_path;
			merged_ctx["task_cpu_slots"] = std::to_string(std::max<idx_t>(1, self->spec_.task_cpu_slots));
		} else {
			merged_ctx["copy_output_base"] = staging_root_base;
			merged_ctx["copy_output_run_id"] = staging_run_id;
			merged_ctx["copy_output_remote_base"] = worker_base;
		}
		WorkerTask new_task(ctx, fragment_plan, old_task->config(), std::move(merged_ctx));
		new_task.mutable_inputs() = old_task->inputs();
		return SubmittableTask<WorkerTask>(std::move(new_task));
	});
}

} // namespace distributed
} // namespace duckdb
