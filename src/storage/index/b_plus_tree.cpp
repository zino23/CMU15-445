//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <new>
#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // Find the leaf that contains the key
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(FindLeafPage(key)->GetData());
  // TODO(q): create a new value here in result?
  ValueType value;
  bool found = leaf_page->Lookup(key, &value, comparator_);
  if (found) {
    result->push_back(value);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // Root page unpinned in StartNewTree
  if (IsEmpty()) {
    StartNewTree(key, value);
    // Print(buffer_pool_manager_);
    return true;
  }

  // All internal pages and leaf page unpinned in InsertIntoLeaf
  InsertIntoLeaf(key, value);
  // TODO(silentroar): deal with duplicate key
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (root_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory!");
  }
  // At first, root is also leaf
  auto root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(root_page->GetData());
  root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId(true);
  // Note: root has no lower children limits
  root_node->Insert(key, value, comparator_);
  // Print(buffer_pool_manager_);
  // This method is only called when the tree is empty. The insert operation is completed in this method, so we need to
  // unpin it here
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto leaf_page = FindLeafPage(key, false);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  // Note: leaf node can hold up to { LEAF_PAGE_SIZE } key/value pairs, which will be larger than or equal to {
  // GetMaxSize() + 1 }. The plus 1 is reserved to hold an extra pair so after split, old_node and new_node will all be
  // at least half full
  if (!leaf_node->IsFull()) {
    leaf_node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return true;
  }

  // Leaf node is full:
  // 1. Insert key/value pair, since items_'s capacity is larger than or equal to {GetMaxSize() + 1}, this will succeed
  leaf_node->Insert(key, value, comparator_);
  // 2. Split the node
  B_PLUS_TREE_LEAF_PAGE_TYPE *new_node;
  new_node = Split(leaf_node);
  new_node->SetNextPageId(new_node->GetNextPageId());
  leaf_node->SetNextPageId(new_node->GetPageId());

  // 3. Insert new_node into leaf_node's parent. The separator key is new_node's first key
  InsertIntoParent(leaf_node, new_node->KeyAt(0), new_node);

  // Don't forget to unpin
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page.
 * Note:
 * 1. Split is an independent behavior without the need to consider where to insert the key/value pair
 * 2. After split, the old_node may be underfull, which is fine, cos we will redistribute it if need be
 * @param   node       the page to be splitted
 * @return  new_node   the newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 1. Allocate a new page, this new page will be pinned in NewPage
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Not enough memory to allocate a new page for spliting");
  }
  auto new_node = reinterpret_cast<N *>(new_page->GetData());
  // Do not forget to initialize the new node! Update new_node's parent id in InsertIntoParent
  if (node->IsLeafPage()) {
    new_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    new_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  }

  // 2. Move the later half key/value to new_node
  // TODO(q): internal and leaf node's MoveHalfTo have different number of arguments
  if (node->IsLeafPage()) {
    auto new_leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_node);
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
    leaf_node->MoveHalfTo(new_leaf_node);
  } else {
    auto new_internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_node);
    auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  }

  // TODO(IMPORTANT):
  // 1. *node's page is pinned inside FindLeafPage which is called inside InsertIntoLeaf, or inside InsertIntoParent
  // 2. It is InsertIntoLeaf or InsertIntoParent's Job to unpin old_node and new_node's pages
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key           the separator key between old_node and new_node
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 1. Check if old_node is root
  if (old_node->IsRootPage()) {
    // Allocate a new page as new root
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    // Note: the new root_node is internal
    auto root_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(root_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // Don't forget to set parent id
    // Note: here comes the beauty of making both internal and root node class of BPlusTreeInternalPage. We don't need
    // to change old_node to internal and change root_node to some "root" class
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // Update root page id
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  // For non-root node
  // 4. Find parent of old_node
  auto parent_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    // TODO(q): exception type of FetchPage failure
    throw Exception(ExceptionType::UNKNOWN_TYPE, "Cannot find parent node of old_node");
  }
  auto parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  // 5.1 if parent is full, split the parent
  if (parent_node->IsFull()) {
    // Split will always sccueed cos the tree grows at the root except memory issue, which will be handled in Split
    // items_[] is large enough to contain {internal_max_size_ + 1} pairs, so we can safely insert {key, value} into
    // parent, and split afterwards
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // Remember to set parent_id for new_node
    new_node->SetParentPageId(parent_id);
    auto parent_sibling = Split(parent_node);
    auto separator_key = parent_sibling->KeyAt(0);
    // Note:
    // 1. Remember to insert parent_sibling to parent's parent. The recursive takes place here.
    // 2. It is InsertIntoParen's job to unpin parent_node and parent_sibling, whereas old_node and new_node will be
    // unpinned inside InsertIntoLeaf
    InsertIntoParent(parent_node, separator_key, parent_sibling);
    // Unpin parent_node and parent_sibling
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_sibling->GetPageId(), true);
    return;
  }

  // 5.2 Parent not full, just insert
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // Update new_node's parent id
  new_node->SetParentPageId(parent_id);
  // unpin parent_node
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.  If not, User needs to first find the right leaf page as deletion
 * target, then delete entry from leaf page. Remember to deal with redistribute or merge if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  Page *leaf_page = FindLeafPage(key);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (!leaf_node->IsHalfFull()) {
    if (CoalesceOrRedistribute(leaf_node)) {
      // leaf_node should be deleted
      auto leaf_id = leaf_page->GetPageId();
      buffer_pool_manager_->DeletePage(leaf_id);
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf or internal page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // 1. If the node is root, call AdjustRoot
  // 4. If return value of AdjustRoot is true, return true to notify root page should be deleted
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // 1. If current node does not have right sibling, then borrow from left
  // 2. If left_sibling->GetSize() + node->GetSize() < node->GetMaxSize(), cannot redistribute, merge left_sibling to
  // node
  // 3. If current node has right_sibling, try to borrow from right
  // 4. If fail, merge node to right_sibling
  // 5. Check if parent is at least half full, if not, call CoalesceOrRedistribute on parent
  if (node->IsLeafPage()) {
    // For leaf node
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
    auto next_page_id = leaf_node->GetNextPageId();
    auto parent_id = leaf_node->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto parent_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    // *node is the last node, borrow from left
    if (next_page_id == INVALID_PAGE_ID) {
      // Locate the left sibling
      // Note: If the leaf node and internal node does not have a right_sibling, then it must have a left_sibling
      // cos *node cannot be the only node in its parent, otherwise the parent will be underfull
      auto left_sibling_idx = parent_node->ValueIndex(node->GetPageId()) - 1;
      auto left_sibling_id = parent_node->ValueAt(left_sibling_idx);
      auto left_sibling_page = buffer_pool_manager_->FetchPage(left_sibling_id);
      auto left_sibling = reinterpret_cast<N *>(left_sibling_page->GetData());
      if (left_sibling->GetSize() + node->GetSize() <= left_sibling->GetMaxSize()) {
        bool delete_parent = Coalesce(&left_sibling, &node, &parent_node, 1);
        if (delete_parent) {
          buffer_pool_manager_->DeletePage(parent_id);
        } else {
          // Does not need to delete parent, unpin it is enough
          buffer_pool_manager_->UnpinPage(parent_id, true);
        }
        // Note: merge left_sibling to node, left will be deleted in Coalesce so it does not need to be unpinned, need
        // not delete node
        return false;
      }
      Redistribute(left_sibling, node, 1);
      // Remember to unpin left_sibling!
      buffer_pool_manager_->UnpinPage(left_sibling_id, true);
      return false;
    }

    auto right_sibling_page = buffer_pool_manager_->FetchPage(next_page_id);
    auto right_sibling = reinterpret_cast<N *>(right_sibling_page->GetData());
    if (right_sibling->GetSize() + node->GetSize() <= right_sibling->GetMaxSize()) {
      bool delete_parent = Coalesce(&right_sibling, &node, &parent_node, 0);
      if (delete_parent) {
        buffer_pool_manager_->DeletePage(parent_id);
      } else {
        buffer_pool_manager_->UnpinPage(parent_id, true);
      }
      // Note: merge node to right_sibling, need to delete node
      // Remember to unpin right_sibling!
      buffer_pool_manager_->UnpinPage(next_page_id, true);
      return true;
    }
    Redistribute(right_sibling, node, 0);
    buffer_pool_manager_->UnpinPage(next_page_id, true);
    return false;
  }

  // For internal node
  auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
  // Find right sibling
  auto parent_id = internal_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  auto node_index = parent_node->ValueIndex(internal_node->GetPageId());
  if (node_index == parent_node->GetMaxSize() - 1) {
    // *node is the most right node, so it does not have right sibling
    auto left_sibling_id = parent_node->ValueAt(node_index - 1);
    auto left_sibling_page = buffer_pool_manager_->FetchPage(left_sibling_id);
    auto left_sibling = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(left_sibling_page);
    if (left_sibling->GetSize() + internal_node->GetSize() <= left_sibling->GetMaxSize()) {
      bool delete_parent = Coalesce(&left_sibling, &internal_node, &parent_node, 1);
      if (delete_parent) {
        buffer_pool_manager_->DeletePage(parent_id);
      } else {
        buffer_pool_manager_->UnpinPage(parent_id, true);
      }
      // Merge left_sibling to internal_node, left_sibling will be deleted in Coalesce, need not delete internal_node,
      // i.e. the input of CoalesceOrRedistribute
      return false;
    }
    // Redistribute is a lot easier! parent_node will not be involved in this process
    Redistribute(left_sibling, internal_node, 1);
    buffer_pool_manager_->UnpinPage(left_sibling_id, true);
    return false;
  }
  auto right_sibling_id = parent_node->ValueAt(node_index + 1);
  auto right_sibling_page = buffer_pool_manager_->FetchPage(right_sibling_id);
  auto right_sibling = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(right_sibling_page);
  if (right_sibling->GetSize() + internal_node->GetSize() <= right_sibling->GetMaxSize()) {
    bool delete_parent = Coalesce(&right_sibling, &internal_node, &parent_node, 0);
    if (delete_parent) {
      buffer_pool_manager_->DeletePage(parent_id);
    } else {
      buffer_pool_manager_->UnpinPage(parent_id, true);
    }
    // Merge internal_node to right_sibling, need to delete internal_node
    buffer_pool_manager_->UnpinPage(right_sibling_id, true);
    return true;
  }
  Redistribute(right_sibling, internal_node, 0);
  buffer_pool_manager_->UnpinPage(right_sibling_id, true);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              merge node to its right sibling if index == 0, merge left sibling to node otherwise
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  auto parent_node = *parent;
  if ((*node)->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(*node);
    auto leaf_neighbor_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(*neighbor_node);
    if (index == 0) {
      // Merge to right_sibling, i.e. leaf_neighbor_node
      leaf_node->MoveAllTo(leaf_neighbor_node);
      // Parent must remove the leaf_node
      auto node_index = parent_node->ValueIndex(leaf_node->GetPageId());
      parent_node->Remove(node_index);
      // Recursive may take place! parent_node may be underfull
      if (!parent_node->IsHalfFull()) {
        bool delete_parent = CoalesceOrRedistribute(parent_node);
        return delete_parent;
      }
    } else {
      // Merge left_sibling to node
      leaf_neighbor_node->MoveAllTo(leaf_node);
      // Parent must remove the left sibling
      auto node_index = parent_node->ValueIndex(leaf_neighbor_node->GetPageId());
      parent_node->Remove(node_index);
      if (!parent_node->IsHalfFull()) {
        bool delete_parent = CoalesceOrRedistribute(parent_node);
        return delete_parent;
      }
    }
  }

  // For internal node
  auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(*node);
  auto internal_neighbor_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(*neighbor_node);
  if (index == 0) {
    // Merge node to its right_sibling
    auto node_index = parent_node->ValueIndex(internal_neighbor_node->GetPageId());
    auto middle_key = parent_node->KeyAt(node_index);
    internal_node->MoveAllTo(internal_neighbor_node, middle_key, buffer_pool_manager_);
    // parent_node need to delete internal_node
    parent_node->Remove(node_index - 1);
    // Recursive may take place here
    if (!parent_node->IsHalfFull()) {
      bool delete_parent = CoalesceOrRedistribute(parent_node);
      return delete_parent;
    }
  } else {
    // Merge left_sibling to node
    auto node_index = parent_node->ValueIndex(internal_node->GetPageId());
    auto middle_key = parent_node->KeyAt(node_index);
    internal_neighbor_node->MoveAllTo(internal_node, middle_key, buffer_pool_manager_);
    // parent_node need to delete internal_neighbor_node
    parent_node->Remove(node_index - 1);
    if (!parent_node->IsHalfFull()) {
      bool delete_parent = CoalesceOrRedistribute(parent_node);
      return delete_parent;
    }
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   index              neighbor_node is node's right sibling if index == 0, left sibling otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
    auto leaf_neighbor_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(neighbor_node);
    // If index == 0, then neighbor_node is node's right sibling, left sibling otherwise
    if (index == 0) {
      leaf_neighbor_node->MoveFirstToEndOf(leaf_node);
    } else {
      leaf_neighbor_node->MoveLastToFrontOf(leaf_node);
    }
  } else {
    // For internal node
    auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    auto internal_neighbor_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);
    page_id_t parent_id = node->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
    auto parent_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    if (index == 0) {
      auto value_index = parent_node->ValueIndex(internal_neighbor_node->GetPageId());
      auto middle_key = parent_node->KeyAt(value_index);
      internal_neighbor_node->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
    } else {
      auto value_index = parent_node->ValueIndex(internal_node->GetPageId());
      auto middle_key = parent_node->KeyAt(value_index);
      internal_neighbor_node->MoveLastToFrontOf(internal_node, middle_key, buffer_pool_manager_);
    }
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size (i.e. at least half full) and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child. Root page is internal node, the root should be adjusted to its only child
 * case 2: when you delete the last element in whole b+ tree. Root page is leaf node
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    // The root is leaf, and it has no entries, so it should be deleted
    return old_root_node->GetSize() == 0;
  }

  if (old_root_node->GetSize() == 1) {
    auto old_internal_root_node =
        static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    root_page_id_ = static_cast<page_id_t>(old_internal_root_node->RemoveAndReturnOnlyChild());
    UpdateRootPageId(false);
    return true;
    // Note: type of the only child node of old_root_node does not need to be changed, i.e. if it is leaf, then stay
    // leaf; if it is internal, stay internal
  }
  // The root can stay the same
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if (IsEmpty()) {
    return nullptr;
  }

  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (root_page == nullptr) {
    return nullptr;
  }

  auto current_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(root_page->GetData());
  if (current_node->IsLeafPage()) {
    return root_page;
  }
  Page *child_page;

  // Find the left most leaf page
  // TODO(IMPORTANT): it is FindLeafPage's job to unpin the internal nodes cos they are only used to find the target
  // leaf page and will not be used later. Whereas the target leaf node will be unpinned inside InsertIntoLeaf cos it
  // will be used later (e.g. do the actual insert and Split)
  if (leftMost) {
    page_id_t first_child_ptr;
    while (!current_node->IsLeafPage()) {
      first_child_ptr = current_node->ValueAt(0);
      child_page = buffer_pool_manager_->FetchPage(first_child_ptr);
      current_node =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(child_page->GetData());
      buffer_pool_manager_->UnpinPage(first_child_ptr, false);
    }
  }

  while (!current_node->IsLeafPage()) {
    auto child_ptr = current_node->Lookup(key, comparator_);
    child_page = buffer_pool_manager_->FetchPage(child_ptr);
    current_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(child_page->GetData());
    buffer_pool_manager_->UnpinPage(child_ptr, false);
  }
  // Leaf page i.e. child_page will be unpinned inside InsertIntoLeaf
  return child_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    std::cout << "Page size: " << leaf->GetSize() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << "Key at index " << i << ": " << leaf->KeyAt(i) << std::endl;
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    std::cout << "Page size: " << internal->GetSize() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << "Index " << i << ": "
                << "{ " << internal->KeyAt(i) << ": " << internal->ValueAt(i) << " }" << std::endl;
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
