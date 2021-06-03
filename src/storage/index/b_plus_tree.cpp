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

#include <mutex>
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
  std::lock_guard<std::mutex> guard(root_page_mutex_);
  if (IsEmpty()) {
    return false;
  }

  // Find the leaf that contains the key
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, transaction, Operation::SEARCH)->GetData());
  ValueType value;
  bool found = leaf_page->Lookup(key, &value, comparator_);
  if (found) {
    result->push_back(value);
  }

  if (transaction != nullptr) {
    // Only target leaf page is latched at this point
    ReleaseLatchedPages(transaction, Operation::SEARCH, false);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }

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
  std::lock_guard<std::mutex> guard(root_page_mutex_);

  // Root page unpinned in StartNewTree
  if (IsEmpty()) {
    StartNewTree(key, value);
    // Print(buffer_pool_manager_);
    return true;
  }

  // All internal pages and leaf page unpinned in InsertIntoLeaf
  return InsertIntoLeaf(key, value);
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
  auto root_node = reinterpret_cast<LeafPage *>(root_page->GetData());
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
  auto leaf_page = FindLeafPage(key, transaction, Operation::INSERT);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  auto old_leaf_size = leaf_node->GetSize();
  // Note: leaf node can hold up to { LEAF_PAGE_SIZE } key/value pairs, which will be larger than or equal to {
  // GetMaxSize() + 1 }. The plus 1 is reserved to hold an extra pair so after split, old_node and new_node will all be
  // at least half full
  leaf_node->Insert(key, value, comparator_);
  auto new_leaf_size = leaf_node->GetSize();
  bool inserted = old_leaf_size != new_leaf_size;

  // Case 1: not full after insert
  if (!leaf_node->IsFull()) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return inserted;
  }

  // Case 2: full, need to split
  LeafPage *new_node = Split(leaf_node);
  // TODO(Q): timing of update next page id? here or MoveHalfTo
  // new_node->SetNextPageId(leaf_node->GetNextPageId());
  // leaf_node->SetNextPageId(new_node->GetPageId());

  // Insert new_node into leaf_node's parent. The separator key is new_node's first key
  InsertIntoParent(leaf_node, new_node->KeyAt(0), new_node);
  // TODO(IMPORTANT): timing of unpin the new splitted page
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  if (transaction != nullptr) {
    ReleaseLatchedPages(transaction, Operation::INSERT, inserted);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  return inserted;
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
  if (new_node->IsLeafPage()) {
    new_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    new_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  }

  // 2. Move the later half key/value to new_node
  node->MoveHalfTo(new_node, buffer_pool_manager_);

  // TODO(IMPORTANT):
  // 1. *node is pinned inside FindLeafPage which is called inside InsertIntoLeaf, or inside InsertIntoParent
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
  // old_node is root
  if (old_node->IsRootPage()) {
    // Allocate a new page as new root
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    // Note: the new root_node is internal
    auto root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
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

  // old_node is not root
  // 1. Find parent of old_node
  auto parent_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    // TODO(q): exception type of FetchPage failure
    throw Exception(ExceptionType::UNKNOWN_TYPE, "Cannot find parent node of old_node");
  }
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 2. Split will always sccueed cos the tree grows at the root except memory issue, which will be handled in Split
  // items_[] is large enough to contain {internal_max_size_ + 1} pairs, so we can safely insert {key, value} into
  // parent, and split afterwards
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // Remember to set parent_id for new_node
  new_node->SetParentPageId(parent_id);
  // 3.1 if parent is full, split the parent
  if (parent_node->IsFull()) {
    auto parent_sibling = Split(parent_node);
    auto separator_key = parent_sibling->KeyAt(0);
    // Note:
    // 1. Remember to insert parent_sibling to parent's parent. The recursive takes place here.
    // 2. It is InsertIntoParen's job to unpin parent_node and parent_sibling, whereas old_node and new_node will be
    // unpinned inside InsertIntoLeaf
    InsertIntoParent(parent_node, separator_key, parent_sibling);
    // Unpin parent_sibling
    buffer_pool_manager_->UnpinPage(parent_sibling->GetPageId(), true);
  }

  // 3.2 Parent not full, unpin parent_node
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
  std::lock_guard<std::mutex> guard(root_page_mutex_);
  if (IsEmpty()) {
    return;
  }

  Page *leaf_page = FindLeafPage(key, transaction, Operation::DELETE);
  auto leaf_id = leaf_page->GetPageId();
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  bool is_dirty = old_size != new_size;
  bool delete_leaf = !leaf_node->IsHalfFull() && CoalesceOrRedistribute(leaf_node);
  if (transaction != nullptr) {
    if (delete_leaf) {
      transaction->AddIntoDeletedPageSet(leaf_id);
    }
    ReleaseLatchedPages(transaction, Operation::DELETE, is_dirty);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_id, is_dirty);
    if (delete_leaf) {
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
  // Note: it is CoalesceOrRedistribute's job to delete parent if needed. Node will be unpinned or deleted
  // in Remove()

  // 1. If the node is root, call AdjustRoot
  // 2. If return value of AdjustRoot is true, return true to notify root page should be deleted
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  auto node_id = node->GetPageId();
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto node_index = parent_node->ValueIndex(node_id);
  // If current node does not have left sibling, take right sibling
  int sibling_index = node_index == 0 ? 1 : node_index - 1;
  page_id_t sibling_id = parent_node->ValueAt(sibling_index);
  auto sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
  auto sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  bool coalesce = sibling_node->GetSize() + node->GetSize() <= sibling_node->GetMaxSize();

  // Coalesce
  if (coalesce) {
    bool delete_parent = Coalesce(&sibling_node, &node, &parent_node, node_index);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    if (delete_parent) {
      if (transaction != nullptr) {
        transaction->AddIntoDeletedPageSet(parent_id);
      } else {
        buffer_pool_manager_->DeletePage(parent_id);
      }
    }
    buffer_pool_manager_->UnpinPage(sibling_id, true);

    // If merge sibling to node, delete sibling
    if (node_index == 0) {
      buffer_pool_manager_->DeletePage(sibling_id);
    }
  } else {
    // Redistribute
    Redistribute(sibling_node, node, node_index);
    buffer_pool_manager_->UnpinPage(sibling_id, true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }

  // This is tricky: if node_index == 0, the sibling page is deleted here cos Remove has no way to delete the sibling
  // and thus node should not be deleted
  return coalesce && node_index != 0;
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
 * @param   index              merge node to its right sibling if index == 0, merge node to its left sibling otherwise
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  auto parent_node = *parent;

  if (index == 0) {
    // Merge neighbor_node to node
    std::swap(*neighbor_node, *node);
  }

  auto node_index = parent_node->ValueIndex((*node)->GetPageId());
  auto separator_key = parent_node->KeyAt(node_index);
  (*node)->MoveAllTo(*neighbor_node, separator_key, buffer_pool_manager_);
  parent_node->Remove(node_index);

  if (!parent_node->IsHalfFull()) {
    return CoalesceOrRedistribute(parent_node);
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
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (index == 0) {
    int value_index = parent->ValueIndex(neighbor_node->GetPageId());
    auto separator_key = parent->KeyAt(value_index);
    neighbor_node->MoveFirstToEndOf(node, separator_key, buffer_pool_manager_);
  } else {
    int value_index = parent->ValueIndex(node->GetPageId());
    auto separator_key = parent->KeyAt(value_index);
    neighbor_node->MoveLastToFrontOf(node, separator_key, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
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
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    // The root is leaf, and it has no entries, so it should be deleted
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    return true;
  }

  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto old_internal_root_node = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = static_cast<page_id_t>(old_internal_root_node->RemoveAndReturnOnlyChild());
    auto new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    // Update root page id in header page
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
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  // Find the leftmost leaf page
  KeyType key;
  auto leaf_page = FindLeafPage(key, nullptr, Operation::SEARCH, true);
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  auto leaf_id = leaf->GetPageId();
  return INDEXITERATOR_TYPE(leaf_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf_page = FindLeafPage(key, nullptr, Operation::SEARCH);
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  auto leaf_id = leaf->GetPageId();
  int index = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_id, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  KeyType key;
  auto leaf_page = FindLeafPage(key, nullptr, Operation::SEARCH, true);
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    auto next_id = leaf->GetNextPageId();
    leaf_page = buffer_pool_manager_->FetchPage(next_id);
    if (leaf_page == nullptr) {
      throw Exception(ExceptionType::INVALID, "During compute end(): cannot fetch page!");
    }
    leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  }
  auto leaf_id = leaf->GetPageId();
  int index = leaf->GetSize();
  return INDEXITERATOR_TYPE(leaf_id, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AcquireLatchOnPage(Page *page, Transaction *transaction, Operation op) {
  if (op == Operation::SEARCH) {
    page->RLatch();
    // This will only release parent
    ReleaseLatchedPages(transaction, op, false);
  } else if (op == Operation::INSERT) {
    page->WLatch();
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // The node is safe after insert one item
    if (node->GetSize() < node->GetMaxSize()) {
      // Release all the way up to root
      ReleaseLatchedPages(transaction, op, false);
    }
  } else {
    page->WLatch();
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->GetSize() > node->GetMinSize()) {
      ReleaseLatchedPages(transaction, op, false);
    }
  }
  transaction->AddIntoPageSet(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchedPages(Transaction *transaction, Operation op, bool is_dirty) {
  auto locked_pages = transaction->GetPageSet();
  while (!locked_pages->empty()) {
    // Release from top to bottom to increase concurrency efficiency
    Page *page = locked_pages->front();
    if (op == Operation::SEARCH) {
      page->RUnlatch();
    } else {
      page->WUnlatch();
    }
    locked_pages->pop_front();

    // Pages from parent all the way up to root will not be changed, so is not dirty
    buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
  }

  auto deleted_pages = transaction->GetDeletedPageSet();
  for (int page_id : *deleted_pages) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  deleted_pages->clear();
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction, Operation op, bool leftMost) {
  if (IsEmpty()) {
    return nullptr;
  }

  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (root_page == nullptr) {
    return nullptr;
  }

  if (transaction != nullptr) {
    AcquireLatchOnPage(root_page, transaction, op);
  }

  auto current_node = reinterpret_cast<InternalPage *>(root_page->GetData());
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
      current_node = reinterpret_cast<InternalPage *>(child_page->GetData());
      if (transaction != nullptr) {
        AcquireLatchOnPage(child_page, transaction, op);
      } else {
        buffer_pool_manager_->UnpinPage(first_child_ptr, false);
      }
    }
  }

  // leftmost == false
  while (!current_node->IsLeafPage()) {
    auto child_ptr = current_node->Lookup(key, comparator_);
    child_page = buffer_pool_manager_->FetchPage(child_ptr);
    current_node = reinterpret_cast<InternalPage *>(child_page->GetData());
    if (transaction != nullptr) {
      AcquireLatchOnPage(child_page, transaction, op);
    } else {
      buffer_pool_manager_->UnpinPage(child_ptr, false);
    }
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
/*******************************************************************************************************************************************************************
 * The commented one is detailed version which is more helpful for debugging. to use b_plus_tree_print_test to generate
 *graph of the tree, use the default version *
 *******************************************************************************************************************************************************************/
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

// void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
//   if (page->IsLeafPage()) {
//     LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
//     std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
//               << " next: " << leaf->GetNextPageId() << std::endl;
//     for (int i = 0; i < leaf->GetSize(); i++) {
//       std::cout << leaf->KeyAt(i) << ",";
//     }
//     std::cout << std::endl;
//     std::cout << std::endl;
//   } else {
//     InternalPage *internal = reinterpret_cast<InternalPage *>(page);
//     std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() <<
//     std::endl; for (int i = 0; i < internal->GetSize(); i++) {
//       std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
//     }
//     std::cout << std::endl;
//     std::cout << std::endl;
//     for (int i = 0; i < internal->GetSize(); i++) {
//       ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
//     }
//   }
//   bpm->UnpinPage(page->GetPageId(), false);
// }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
