//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  // EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  // Create table
  auto table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  table_metadata = catalog->GetTable(table_name);
  EXPECT_EQ(table_name, table_metadata->name_);

  // Create index
  // Index and table schema
  std::string index_name = "Weight";
  Schema *key_schema = ParseCreateStatement("a bigint");
  columns.clear();
  columns.emplace_back("Price", TypeId::BIGINT);
  columns.emplace_back("Weight", TypeId::BIGINT);
  schema = Schema(columns);
  table_name = "Fruit Weight";

  // Create txn
  txn_id_t txn_id = 0;
  auto txn = new Transaction(txn_id);
  // build index on column 0
  std::vector<uint32_t> key_attrs{0};
  size_t key_size = 8;  // match GenericKey<8>
  auto index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn, index_name, table_name, schema,
                                                                                   *key_schema, key_attrs, key_size);
  index_info = catalog->GetIndex(index_name, table_name);
  EXPECT_EQ(table_name, index_info->table_name_);

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
