//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>

#include <algorithm>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  // The first key/value pair will always exist, cos the tree grows at the root
  SetSize(0);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  // TODO(q): starting index. assume 1 now, 0 for the invalid
  return items_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { items_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { items_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetPairAt(int index, const MappingType &pair) { items_[index] = pair; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // Note: value is not sorted
  for (int i = 0; i < GetSize(); i++) {
    if (items_[i].second == value) {
      return i;
    }
  }
  // find fail, return 0
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return items_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer (page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key (the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // Start with the second key (index 1), binary search to find the smallest index which contains a key larger than the
  // given {key}. This is equal to find an interval's left boundary.

  int l = 1;
  int r = GetSize();
  while (l < r) {
    // separate the interval into two subintervals [l, mid], [mid + 1, r]
    int mid = (l + r) >> 1;
    auto mid_key = KeyAt(mid);
    if (comparator(mid_key, key) > 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  // return the found index decremented by 1
  return ValueAt(l - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * @param:  old_value
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // *this points to root node
  assert(IsRootPage());
  SetValueAt(0, old_value);
  SetPairAt(1, {new_key, new_value});
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value == old_value
 * @param: old_value   old_page_id
 * @param: new_key     new seprator key
 * @param: new_value   new_page_id
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  auto value_index = ValueIndex(old_value);
  auto moved_size = GetSize() - 1 - (value_index + 1) + 1;
  std::memmove(GetItems() + value_index + 2, GetItems() + value_index + 1, sizeof(MappingType) * moved_size);
  SetPairAt(value_index + 1, {new_key, new_value});
  IncreaseSize(1);
  return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // Move the latter half to (*this)'s right sibling
  assert(GetSize() == GetMaxSize() + 1);
  assert(recipient->GetSize() == 0);
  auto items = GetItems();
  auto size = GetSize();
  auto remain_size = GetMinSize();
  auto num_copy_items = size - remain_size;
  // call CopyNFrom to copy half items to recipient
  // recipient will be empty at this point
  recipient->CopyNFrom(items + remain_size, num_copy_items, buffer_pool_manager);
  IncreaseSize(-num_copy_items);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size,
                                                  BufferPoolManager *buffer_pool_manager) {
  // This node is empty at this point
  auto my_items = GetItems();
  int my_size = GetSize();
  page_id_t page_id = GetPageId();
  for (int i = 0; i < size; i++) {
    auto item = *(items + i);
    *(my_items + my_size + i) = item;
    // Update each item's parent_page_id
    auto item_page_id = static_cast<page_id_t>(item.second);
    auto item_page = buffer_pool_manager->FetchPage(item_page_id);
    auto item_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(item_page->GetData());
    // Adopt copied nodes
    item_node->SetParentPageId(page_id);
    buffer_pool_manager->UnpinPage(item_page_id, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // TODO(q): should I consider underfull here
  auto size = GetSize();
  auto items = GetItems();
  auto move_size = size - 1 - index;
  std::memmove(items + index, items + index + 1, sizeof(MappingType) * move_size);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // Will only be called when the root is internal and has only one item whose key is invalid
  assert(GetSize() == 1);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // Merge *this to its left sibling (merge to left is easier to implement)
  assert((GetSize() + recipient->GetSize() <= recipient->GetMaxSize()) &&
         "Merge error: recipient does not have enough space to accommodate the underfull node!");
  // 1. Move down middle_key to be the first key which is invalid before
  SetKeyAt(0, middle_key);
  // 2. Move this's items to the end of recipient
  recipient->CopyNFrom(GetItems(), GetSize(), buffer_pool_manager);

  // Note: this node will be deleted in Coalesce
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // *this points to recipient
  // During Coalesce, the left sibling copies all items from the underfull node
  auto my_items = GetItems();
  int my_size = GetSize();
  page_id_t page_id = GetPageId();
  for (int i = 0; i < size; i++) {
    auto item = *(items + i);
    *(my_items + my_size + i) = item;
    // Update each item's parent_page_id
    auto item_page_id = static_cast<page_id_t>(item.second);
    auto item_page = buffer_pool_manager->FetchPage(item_page_id);
    auto item_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(item_page->GetData());
    // Adopt copied nodes
    item_node->SetParentPageId(page_id);
    buffer_pool_manager->UnpinPage(item_page_id, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // This node's size is larger than half, so it can lend its key/value pairs to recipient, i.e. this's left sibling
  // Move the middle_key/separator_key down to be the key of moved pair
  auto moved_pair = std::make_pair(middle_key, ValueAt(0));
  // Update size
  IncreaseSize(-1);
  // Reorganize pairs in this node
  auto items = GetItems();
  std::memmove(items, items + 1, sizeof(MappingType) * GetSize());
  // Update middle_key in parent node
  auto parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto value_index = parent_node->ValueIndex(static_cast<ValueType>(GetPageId()));
  parent_node->SetKeyAt(value_index, KeyAt(0));
  // Now recipient can recieve moved_pair
  recipient->CopyLastFrom(moved_pair, buffer_pool_manager);
  // TODO(IMPORTANT): timing of unpin parent_page
  buffer_pool_manager->UnpinPage(parent_id, true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // *this points to a underfull node which attemps to borrow pairs from its right sibling
  auto size = GetSize();
  SetPairAt(size, pair);
  auto page_id = static_cast<page_id_t>(pair.second);
  auto page = buffer_pool_manager->FetchPage(page_id);
  // Update the new node's parent id
  auto node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // *this points to a node whose size is larger than GetMinSize(). So it can borrow pairs to its right sibling,
  // i.e. recipient
  assert(GetSize() > GetMinSize());
  auto size = GetSize();
  auto moved_pair = std::make_pair(KeyAt(size - 1), ValueAt(size - 1));

  // Move middle_key/separator_key down to be the first key of recipient which is invalid before
  recipient->SetKeyAt(0, middle_key);
  // Move recipient's pairs right to leave space for moved_pair
  auto reci_items = recipient->GetItems();
  std::memmove(reci_items + 1, reci_items, sizeof(MappingType));
  IncreaseSize(-1);

  // Update middle_key in parent node
  auto parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto value_index = parent_node->ValueIndex(static_cast<ValueType>(recipient->GetPageId()));
  parent_node->SetKeyAt(value_index, moved_pair.first);
  recipient->CopyFirstFrom(moved_pair, buffer_pool_manager);
  // TODO(IMPORTANT): timing of unpin parent_page
  buffer_pool_manager->UnpinPage(parent_id, true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // *this points to a underfull node attempting to borrow pairs from its left sibling
  assert(GetSize() < GetMinSize());
  SetPairAt(0, pair);
  // Update parent id of this pair
  auto page_id = static_cast<page_id_t>(pair.second);
  auto page = buffer_pool_manager->FetchPage(page_id);
  auto node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
