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
  // the first key/value pair
  SetSize(1);
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
  // given {key}. This is equal to find an interval's left boundary. The returned {value} is at the found index
  // decremented by 1. Call ValueAt() to return the value/child pointer, i.e. page_id

  int l = 1;
  int r = GetSize();
  while (l < r) {
    // separate the interval into two subintervals [l, mid], [mid + 1, r]
    int mid = (l + r) >> 1;
    auto mid_key = KeyAt(mid);
    // [mid_key:] should all > key
    if (comparator(mid_key, key)) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  // return the found index decremented by 1
  return ValueAt(l - 1);

  // Another way of binary search, find the largest index that is smaller or equal than the given {key}. This is equal
  // to find an interval's right boundary. This cannot be used because comparator(a, b) return false if a == b while (l
  // < r) {
  //   int mid = (l + r + 1) >> 1;
  //   auto mid_key = KeyAt(mid);
  //   // [:mid_key] should all
  //   if (comparator(mid_key, key)) {
  //     l = mid;
  //   } else {
  //     r = mid - 1;
  //   }
  // }
  // return ValueAt(l);
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
  SetValueAt(0, old_value);
  SetPairAt(1, {new_key, new_value});
  // TODO(q): the first value is empty at beginning
  IncreaseSize(1);
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
  // assums there is space
  assert(GetMaxSize() > GetSize());
  auto value_index = ValueIndex(old_value);
  std::memmove(GetItems() + value_index + 2, GetItems() + value_index + 1,
               sizeof(MappingType) * GetSize() - value_index - 1);
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
  // *this points to sender node
  auto items = GetItems();
  auto size = GetSize();
  // starting index of to be moved items
  auto st = size / 2;
  auto num_copy_items = size - st + 1;
  // call CopyNFrom to copy half items to recipient
  // recipient should be empty at this point
  recipient->CopyNFrom(items + st, num_copy_items, buffer_pool_manager);
  // TODO(q): no need to actual delete, just update metadata?
  IncreaseSize(-num_copy_items);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // *this points to recipient
  // Two use cases:
  // 1. copy items to emtry recipient after split
  // 2. copy items from its left sibling during merge
  // Note: after copy, this->GetItems()[0].second has value. But this value is treated as invalid

  // Space issue is not considered here, this node have enough space to accommodate items at this point. It only need to
  // perform copy operation
  auto my_items = GetItems();
  page_id_t page_id = GetPageId();
  for (int i = 0; i < size; i++) {
    auto item = *(items + i);
    *(my_items + i) = item;
    // update each item's parent_page_id
    auto item_page_id = static_cast<page_id_t>(item.second);
    // TODO(q): item_page will be unpinned at b_plus_tree.cpp::InsertIntoParent or ??? in the second case?
    auto item_page = buffer_pool_manager->FetchPage(item_page_id);
    auto item_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(item_page->GetData());
    // adopt copied nodes
    item_node->SetParentPageId(page_id);
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
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { return INVALID_PAGE_ID; }
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
  // merge *this to its right sibling
  assert((GetSize() + recipient->GetSize() <= recipient->GetMaxSize()) &&
         "Merge error: recipient does not have enough space to accommodate the underfull node!");
  // 1. move down recipient's middle_key to be the first key which is invalid before
  recipient->SetKeyAt(0, middle_key);
  // 2. find the key/value pair that points to *this, and use it to update middle_key that separates *this and
  // *recipient
  auto parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto value_index_this = parent_node->ValueIndex(static_cast<ValueType>(GetPageId()));
  auto key_to_this = parent_node->KeyAt(value_index_this);
  auto value_index_reci = parent_node->ValueIndex(static_cast<ValueType>(recipient->GetPageId()));
  parent_node->SetKeyAt(value_index_reci, key_to_this);
  // 4. move this's pairs to the beginning of recipient
  auto size_moved = GetSize();
  auto reci_items = recipient->GetItems();
  // move recipient's items right to leave space to accommodate this node's items
  std::memmove(reci_items + size_moved, reci_items, sizeof(MappingType) * recipient->GetSize());
  recipient->CopyNFrom(GetItems(), GetSize(), buffer_pool_manager);
  // 5. delete the page the contains *this
  auto page_id = GetPageId();
  // TODO(IMPORTANT): unpin first! set is_dirty to false cos it will be deleted right away
  buffer_pool_manager->UnpinPage(page_id, false);
  buffer_pool_manager->DeletePage(page_id);  // TODO(err): cannot delete itself, should be deleted in Coalesce
  // 6. delete the pair that points to this
  Remove(value_index_this);
  // TODO(q): the timing of unpin parent
  buffer_pool_manager->UnpinPage(parent_id, true);
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
  // *this points to a node whose size is larger than half, so it can lend its key/value pairs to recipient, which is
  // this's right sibling
  // TODO(q): why only move one pair when we can calculate how many pairs recipient is needed to make it at least half
  auto size = GetSize();
  assert((size - 1 >= size / 2) && "Redistribution error: this node does not have enough key/value pairs!");
  auto moved_pair = std::make_pair(middle_key, ValueAt(0));
  // Update size
  IncreaseSize(-1);
  // Reorganize pairs in this node
  auto items = GetItems();
  memmove(items, items + 1, sizeof(MappingType) * GetSize());
  // Update middle_key in parent node
  auto parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto value_index = parent_node->ValueIndex(static_cast<ValueType>(GetPageId()));
  parent_node->SetKeyAt(value_index, KeyAt(0));
  // Now recipient can recieve moved_pair
  recipient->CopyLastFrom(moved_pair, buffer_pool_manager);
  // TODO(q): timing of unpin parent_page
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
  // *this points to a node whose size is larger than GetMaxSize() / 2. So it can borrow pairs to its right sibling,
  // i.e. recipient
  auto size = GetSize();
  auto moved_pair = std::make_pair(KeyAt(size - 1), ValueAt(size - 1));
  // Move middle_key down to be the first key of recipient which is invalid before
  recipient->SetKeyAt(0, middle_key);
  // Move recipient's pairs right to leave space for moved_pair
  auto reci_items = recipient->GetItems();
  std::memmove(reci_items + 1, reci_items, sizeof(MappingType));
  // Update middle_key in parent node
  auto parent_id = GetParentPageId();
  auto parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto value_index = ValueIndex(static_cast<ValueType>(recipient->GetPageId()));
  parent_node->SetKeyAt(value_index, moved_pair.first);
  recipient->CopyFirstFrom(moved_pair, buffer_pool_manager);
  IncreaseSize(-1);
  // TODO(q): timing of unpin parent_page
  buffer_pool_manager->UnpinPage(parent_id, true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // *this points to a underfull node attempting to borrow pairs from its left sibling
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
