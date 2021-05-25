/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *buffer_pool_manager) {
  leaf_id_ = page_id;
  index_ = index;
  buffer_pool_manager_ = buffer_pool_manager;
  // TODO(IMPORTANT): unpin this leaf page
  auto leaf_page = buffer_pool_manager_->FetchPage(leaf_id_);
  if (leaf_page == nullptr) {
    throw Exception(ExceptionType::INVALID, "IndexIterator constructor: cannot fetch page");
  }
  leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  buffer_pool_manager_->UnpinPage(leaf_id_, false);
  buffer_pool_manager_ = nullptr;
  leaf_ = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "IndexIterator: out of range");
  }
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (++index_ >= leaf_->GetSize()) {
    auto next_id = leaf_->GetNextPageId();
    if (next_id != INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(leaf_id_, false);
      auto leaf_page = buffer_pool_manager_->FetchPage(next_id);
      if (leaf_page == nullptr) {
        throw Exception(ExceptionType::INVALID, "INDEXITERATOR_TYPE::operator++: cannot fetch page");
      }
      leaf_id_ = next_id;
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
      index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
