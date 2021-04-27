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
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out ofmemory!");
  }
  // At first, root is also leaf
  auto root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(root_page->GetData());
  root_node->Init(root_page_id_, INVALID_PAGE_ID);
  UpdateRootPageId(true);
  root_node->Insert(key, value, comparator_);
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
  if (!leaf_node->IsFull()) {
    leaf_node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *new_node;
  new_node = Split(leaf_node);
  new_node->SetNextPageId(new_node->GetNextPageId());
  leaf_node->SetNextPageId(new_node->GetPageId());
  KeyType separator_key = new_node->KeyAt(0);
  InsertIntoParent(leaf_node, key, new_node);

  // Don't forget to unpin
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page.
 * Note: Split is an independent behavior without the need to consider where to insert the key/value pair
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
    throw std::bad_alloc();
    std::cout << "Out of memory, cannot split!" << std::endl;
  }
  auto new_node = reinterpret_cast<N *>(new_page->GetData());
  // Do not forget to initialize the new node! Update new_node's parent id in InsertIntoParent
  new_node->Init(new_page_id, INVALID_PAGE_ID);

  // 2. Move the later half key/value to new_node
  // TODO(q): internal and leaf node's MoveHalfTo have different number of arguments
  if (node->IsLeafPage()) {
    auto new_leaf_node = dynamic_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_node);
    auto leaf_node = dynamic_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
    leaf_node->MoveHalfTo(new_leaf_node);
  } else {
    auto new_internal_node = dynamic_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_node);
    auto internal_node = dynamic_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  }

  // TODO(IMPORTANT):
  // 1. node is already pinned
  // 2. don't need to unpin the new page, cos after Split, we must call InsertIntoParent to insert the new_node into
  // node's parent node
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
    auto root_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(root_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID);
    root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // Don't forget to set parent id
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // Update root page id
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    // 4. Find parent of old_node
    auto parent_id = old_node->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
    if (parent_page == nullptr) {
      throw std::runtime_error("Cannot find parent page!");
    }
    auto parent_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    // 5.1 if parent is full, split the parent
    if (parent_node->IsFull()) {
      // split will always sccueed cos the tree grows at the root except memory issue, which will be handled in Split
      auto parent_sibling = Split(parent_node);
      // 6. finally, actual insert
      // now parent_node and parent_sibling both have space, during split, old_node may be in parent_node or
      // parent_sibling, we need to insert new_node to the one that contains old_node
      // TODO(IMPORTANT): although the KeyAt(0) is invalid, it should be the separator key
      auto separator_key = parent_sibling->KeyAt(0);
      if (comparator_(key, separator_key)) {
        parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      } else {
        parent_sibling->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      }
      // The recursive takes place here! Don't forget to insert parent_sibling to parent's parent and unpin the parent
      // node!
      InsertIntoParent(parent_node, separator_key, parent_sibling);
      // Unpin parent_node and parent_sibling
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_sibling->GetPageId(), true);
    } else {
      // 5.2 Parent not full, just insert
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      // unpin parent_node
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
  }
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // 1. If current node does not have right sibling, then borrow from left
  // 2. If left_sibling->GetSize() + node->GetSize() < node->GetMaxSize(), cannot redistribute, merge left_sibling to
  // node
  // 3. If current node has right_sibling, try to borrow from right
  // 4. If fail, merge node to right_sibling
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
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
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
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // If index == 0, then neighbor_node is node's right sibling, left sibling otherwise
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

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
  Page *child_page;

  // Find the left most leaf page
  // TODO(q): when to unpin these pages
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
    auto child_page = buffer_pool_manager_->FetchPage(child_ptr);
    current_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(child_page->GetData());
    buffer_pool_manager_->UnpinPage(child_ptr, false);
  }
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
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
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
