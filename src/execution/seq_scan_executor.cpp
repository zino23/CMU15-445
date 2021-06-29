//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

/**
 * Note: call base class AbstractExecutor's constructor to hold a copy of pointer to ExecutorContext
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

/**
 * Initialize information this executor needs:
 * 1. A table iterator to fetch tuple sequentially
 * 2. Schema of the table related to this executor
 */
void SeqScanExecutor::Init() {
  // Initialize a table_iterator pointing to the first tuple of the target table
  table_oid_ = plan_->GetTableOid();
  // Fetch table_heap via exec_ctx
  auto table_metadata = exec_ctx_->GetCatalog()->GetTable(table_oid_);
  auto table_heap = table_metadata->table_.get();
  schema_ = &table_metadata->schema_;
  table_iterator_ = table_heap->Begin(exec_ctx_->GetTransaction());
  table_iterator_end_ = table_heap->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  bool found_tuple = false;
  // Iterate through the table with table iterator
  while (table_iterator_ != table_iterator_end_) {
    // Fetch tuple
    // Returned tuple is a copy of the real tuple through copy assignment of Tuple
    // Dereference and increment table iterator to leave out mess
    *tuple = *(table_iterator_++);
    // 1. Select operator (optional)
    // Filter tuples according to predicate in plan_
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      bool eval = predicate->Evaluate(tuple, schema_).GetAs<bool>();
      if (!eval) {
        // Current tuple does not satify predicate, test next
        continue;
      }
    }

    // 2. Projection operator
    // 2.1 Produce Value based on ColumnValueExpression
    // 2.2 Produce Tuple based on output schema
    // Note: if there is no predicate, do projection directly on {tuple}
    auto output_schema = plan_->OutputSchema();
    std::vector<Value> values;
    // Fetch ColumnValueExpression of schema
    for (const auto &col : output_schema->GetColumns()) {
      auto expr = col.GetExpr();
      values.emplace_back(expr->Evaluate(tuple, output_schema));
    }

    // the output tuple
    *tuple = Tuple(values, output_schema);
    found_tuple = true;
    break;
  }

  return found_tuple;
}

}  // namespace bustub
