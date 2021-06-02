//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  // The extra slot is for sibling pointer
  SetMaxSize(max_size - 1);
  // The first key of leaf page is valid, so set initial size to 0
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that items[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // Binary search to find the smallest index i where KeyAt(i) >= {key}. This is equal to find the
  // left boundary of an interval x where all keys in x >= {key}
  int l = 0;
  // Set r to GetSize() - 1 will generate corner case where the key is larger than all the keys
  int r = GetSize();

  while (l < r) {
    int mid = (l + r) >> 1;
    // middle_key >= key
    if (comparator(KeyAt(mid), key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  auto item = GetItem(index);
  return item.first;
}

/*
 * Helper method to find and return the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  auto item = GetItem(index);
  return item.second;
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const {
  assert(index >= 0 && index < GetSize());
  return items_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  assert(GetSize() < static_cast<int>(LEAF_PAGE_SIZE - 1));
  // Find the index to insert
  int key_index = KeyIndex(key, comparator);

  // Check if the key is a duplicate
  if (key_index < GetSize() && comparator(KeyAt(key_index), key) == 0) {
    return GetSize();
  }

  // Move items_[key_index:GetSize() - 1] right for one space
  // size: GetSize() - 1 - key_index + 1
  int moved_size = GetSize() - key_index;
  // Use memmove to deal with overlapping issue
  std::memmove(GetItems() + key_index + 1, GetItems() + key_index, sizeof(MappingType) * moved_size);
  SetItem(key_index, {key, value});
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  // Assert this node and recipient is in correct state
  assert(GetSize() == GetMaxSize() + 1);
  assert(recipient->GetSize() == 0);

  // Move {GetMinSize() + 1} items to its right sibling
  auto items = GetItems();
  auto size = GetSize();
  auto remain_size = GetMinSize();
  auto num_copy_items = size - remain_size;
  recipient->CopyHalfFrom(items + remain_size, num_copy_items);
  IncreaseSize(-num_copy_items);
  // TODO(IMPORTANT): update sibling pointer
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
  auto my_items = GetItems();
  std::memmove(my_items, items, sizeof(MappingType) * size);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  // Binary search to locate the first key that is larger than or equal to {key}
  auto key_index = KeyIndex(key, comparator);
  if (key_index < GetSize() && comparator(key, KeyAt(key_index)) == 0) {
    *value = ValueAt(key_index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE:
 * 1. It is guaranteed that this method need only perform delete and need not worry about merge or redistribution, just
 * like Insert
 * 2. Store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  assert(GetSize() > 0);
  ValueType value;
  if (!Lookup(key, &value, comparator)) {
    return GetSize();
  }

  // The key exists, we can safely use KeyIndex to find the key's corresponding index
  int key_index = KeyIndex(key, comparator);
  // Check the key is equal to the one we want to delete
  if (key_index < GetSize() && comparator(key, KeyAt(key_index)) == 0) {
    int st = key_index + 1;
    int size_moved = GetSize() - 1 - st + 1;
    auto items = GetItems();
    std::memmove(items + key_index, items + st, sizeof(MappingType) * size_moved);
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  // Merge *this to left sibling
  // Note: the difference to internal page's version is leaf page does not need to use middle_key to as the key of
  // recipient's first item
  assert((GetSize() + recipient->GetSize() <= recipient->GetMaxSize()) &&
         "Merge error: recipient does not have enough space to accommodate the underfull node!");
  auto items = GetItems();
  recipient->CopyAllFrom(items, GetSize());
  // TODO(IMPORTANT): update sibling pointer here is clearer
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
  // this node will be deleted in Coalesce
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  auto my_items = GetItems();
  std::memmove(my_items, items, sizeof(MappingType) * size);
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                  BufferPoolManager *buffer_pool_manager) {
  auto first_item = GetItem(0);
  recipient->CopyLastFrom(first_item);
  auto items = GetItems();
  std::memmove(items, items + 1, GetSize() - 1);
  IncreaseSize(-1);

  // Update parent's separator_key
  page_id_t parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent = reinterpret_cast<B_PLUS_TREE_LEAF_PARENT_TYPE *>(parent_page->GetData());
  int value_index = parent->ValueIndex(GetPageId());
  parent->SetKeyAt(value_index, KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_id, true);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int size = GetSize();
  SetItem(size, item);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                   BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > GetMinSize());
  int size = GetSize();
  auto last_item = GetItem(size - 1);
  auto reci_items = recipient->GetItems();

  // Leave space for recipient
  std::memmove(reci_items + 1, reci_items, recipient->GetSize());
  CopyFirstFrom(last_item);
  IncreaseSize(-1);

  // Update parent's separator_key between *this and recipient
  auto parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<B_PLUS_TREE_LEAF_PARENT_TYPE *>(parent_page->GetData());
  auto value_index = parent_node->ValueIndex(recipient->GetPageId());
  parent_node->SetKeyAt(value_index, last_item.first);
  buffer_pool_manager->UnpinPage(parent_id, true);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  SetItem(0, item);
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
