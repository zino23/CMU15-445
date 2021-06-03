//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <mutex>
#include <unordered_map>
#include "common/config.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the
  // free list or the replacer. Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then
  // return a pointer to P.

  // 1. Check is the page is in the memory
  std::lock_guard<std::mutex> guard(latch_);
  auto page_it = page_table_.find(page_id);

  if (page_it != page_table_.end()) {
    // 1.1 Pin it and return
    frame_id_t frame_id = page_it->second;
    auto page = static_cast<Page *>(GetPages() + frame_id);
    IncrementPinCount(frame_id);
    replacer_->Pin(frame_id);
    return page;
  }

  // 1.2 Not exist, find a free slot in the free_list_
  if (!free_list_.empty()) {
    // just fetch the first free slot
    auto frame_id = free_list_.front();
    auto target_page = static_cast<Page *>(GetPages() + frame_id);
    auto target_page_data = target_page->GetData();
    // read from disk according to page_id
    disk_manager_->ReadPage(page_id, target_page_data);
    // update page table
    page_table_[page_id] = frame_id;
    // increment pin count, since the frame is chosen from free list, it cannot be in replacer
    IncrementPinCount(frame_id);
    return target_page;
  }

  // 1.2 free_list_ does not have free slots, find a replacement from replacer_
  if (replacer_->Size() > 0) {
    frame_id_t victim_frame_id;
    if (replacer_->Victim(&victim_frame_id)) {
      auto victim = static_cast<Page *>(GetPages() + victim_frame_id);
      auto victim_page_id = victim->GetPageId();
      page_table_.erase(victim_page_id);
      // if victim is dirty, flush it
      if (victim->IsDirty()) {
        FlushPageImpl(victim_page_id);
      }
      victim->ResetMemory();
      ResetMetadata(victim_frame_id);
      disk_manager_->ReadPage(page_id, victim->GetData());
      page_table_[page_id] = victim_frame_id;
      IncrementPinCount(victim_frame_id);
      replacer_->Pin(victim_frame_id);
      victim->SetPageId(page_id);
      return victim;
    }
  }
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // does not need to check if page id is valid cause if it is not valid it will not be stored on page table anyway
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    auto page = static_cast<Page *>(GetPages() + frame_id);
    if (page->GetPinCount() <= 0) {
      return false;
    }
    // decrement pin count
    DecrementPinCount(frame_id);
    if (page->GetPinCount() == 0) {
      replacer_->Unpin(frame_id);
    }
    // the dirty bit will be told when unpinning
    page->SetDirty(is_dirty);
    return true;
  }
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  // TODO(IMPORTANT): FlushPageImpl may be called in FetchPageImpl, so latch_ is already acquired
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = static_cast<Page *>(GetPages() + frame_id);
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always
  // pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // *page_id is the page id of the newly created page, which starts at 0 which
  // the disk manager tracks. With this new page, we can read and write data to
  // and from it

  std::lock_guard<std::mutex> guard(latch_);
  auto page_iter = GetPages();
  bool all_pinned = true;
  // if there is free slot in the free list, put the newly created page in the
  // free slot
  if (!free_list_.empty()) {
    auto frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = disk_manager_->AllocatePage();
    // update metadata and page table
    page_table_[*page_id] = frame_id;
    auto new_page = static_cast<Page *>(GetPages() + frame_id);
    new_page->ResetMemory();
    new_page->SetPageId(*page_id);
    ResetMetadata(frame_id);
    IncrementPinCount(frame_id);
    replacer_->Pin(frame_id);
    return new_page;
  }

  // check if all the pages are pinned
  auto pool_size = GetPoolSize();
  for (size_t i = 0; i < pool_size; i++) {
    if (page_iter->GetPinCount() == 0) {
      all_pinned = false;
      break;
    }
    page_iter++;
  }

  // all pages are pinned
  if (all_pinned) {
    return nullptr;
  }

  frame_id_t victim_frame_id;
  if (replacer_->Victim(&victim_frame_id)) {
    auto victim_page = static_cast<Page *>(pages_ + victim_frame_id);
    auto victim_page_id = victim_page->GetPageId();
    if (victim_page->IsDirty()) {
      FlushPageImpl(victim_page_id);
    }
    // remove fron page table
    page_table_.erase(victim_page_id);

    // allocate new page
    *page_id = disk_manager_->AllocatePage();
    victim_page->SetPageId(*page_id);
    page_table_[*page_id] = victim_frame_id;
    victim_page->ResetMemory();
    ResetMetadata(victim_frame_id);
    IncrementPinCount(victim_frame_id);
    replacer_->Pin(victim_frame_id);
    return victim_page;
  }
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is
  // using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its
  // metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);

  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return true;
  }

  auto frame_id = page_table_[page_id];
  auto page = static_cast<Page *>(pages_ + frame_id);
  if (page->GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  ResetMetadata(frame_id);
  free_list_.emplace_back(frame_id);
  // no need to call replacer_->Pin() cause when a page is pinned to a frame in free_list_, Pin() will be called then
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // TODO(question): what to do with pinned pages, loop or don't touch it
  std::lock_guard<std::mutex> guard(latch_);

  // loop through page table
  for (auto x : page_table_) {
    auto page_id = x.first;
    auto frame_id = x.second;
    auto page = static_cast<Page *>(GetPages() + frame_id);
    // if the page is not pinned and is dirty, flush it, otherwise leave it be
    if (page->GetPinCount() == 0 && page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->GetData());
    }
  }
}

void BufferPoolManager::ResetMetadata(frame_id_t frame_id) {
  auto page = static_cast<Page *>(GetPages() + frame_id);
  page->pin_count_ = 0;
  page->is_dirty_ = false;
}

void BufferPoolManager::IncrementPinCount(frame_id_t frame_id) {
  auto page = static_cast<Page *>(GetPages() + frame_id);
  page->pin_count_++;
}

void BufferPoolManager::DecrementPinCount(frame_id_t frame_id) {
  auto page = static_cast<Page *>(GetPages() + frame_id);
  page->pin_count_--;
}
}  // namespace bustub
