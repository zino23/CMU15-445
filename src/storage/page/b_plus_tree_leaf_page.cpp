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
  SetMaxSize(max_size);
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
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // Binary search to find the smallest index i where KeyAt(i) >= {key}. This is equal to find the
  // left boundary of an interval x where all keys in x >= {key}
  int l = 0;
  int r = GetSize() - 1;

  // Key is larger than all the keys. This is used to get .end() iterator
  if (comparator(key, KeyAt(r)) > 0) {
    return r + 1;
  }

  while (l < r) {
    int mid = (l + r) >> 1;
    // middle_key >= key
    if (comparator(KeyAt(mid), key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  };
  return l;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
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
  // replace with your own code
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
  // Case 1: the node is empty
  if (GetSize() == 0) {
    SetItem(0, {key, value});
    IncreaseSize(1);
    return GetSize();
  }

  // Case 2: key is smaller than the smallest key, i.e. the first key of the node
  if (comparator(key, KeyAt(0)) < 0) {
    int moved_size = GetSize();
    std::memmove(GetItems() + 1, GetItems(), sizeof(MappingType) * moved_size);
    SetItem(0, {key, value});
    IncreaseSize(1);
    return GetSize();
  }

  // Case 3: key is in the middle or key is larger than the largest key in this node
  // Find the index to insert
  int key_index = KeyIndex(key, comparator);
  // Move items_[index:] right for one space
  // size: GetSize() - 1 - key_index + 1
  int moved_size = GetSize() - key_index;
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // Move the latter half to the start of recipient
  auto items = GetItems();
  auto size = GetSize();
  // Starting index of moved pairs
  auto st = size >> 1;
  auto num_copy_items = size - st;
  recipient->CopyNFrom(items + st, num_copy_items);
  IncreaseSize(-num_copy_items);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
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
  // If all keys are smaller than {key}, return false
  if (comparator(KeyAt(GetSize() - 1), key) < 0) {
    return false;
  }

  int l = 0;
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r) >> 1;
    auto mid_key = KeyAt(mid);
    if (comparator(mid_key, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }

  if (comparator(key, KeyAt(l)) == 0) {
    *value = ValueAt(l);
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
  ValueType value;
  if (!Lookup(key, &value, comparator)) {
    return GetSize();
  }

  // The key exists, we can safely use KeyIndex to find the key's corresponding index
  int key_index = KeyIndex(key, comparator);
  int st = key_index + 1;
  int size_moved = GetSize() - st + 1;
  auto items = GetItems();
  std::memmove(items + key_index, items + st, sizeof(MappingType) * size_moved);
  IncreaseSize(-1);
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // recipient is empty at this point
  auto items = GetItems();
  recipient->CopyNFrom(items, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  // this node will be deleted in Split
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  auto first_item = GetItem(0);
  recipient->CopyLastFrom(first_item);
  auto items = GetItems();
  std::memmove(items, items + 1, GetSize() - 1);
  IncreaseSize(-1);
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int size = GetSize();
  auto last_item = GetItem(size - 1);
  auto reci_items = recipient->GetItems();
  // Leave space for recipient
  std::memmove(reci_items + 1, reci_items, recipient->GetSize());
  CopyFirstFrom(last_item);
  IncreaseSize(-1);
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
