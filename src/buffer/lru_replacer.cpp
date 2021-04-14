//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

#include <limits>

#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // check if there are possile victims
  if (replacement_pool_.empty()) {
    return false;
  }

  // implement the clock policy
  auto pool_size = Size();
  for (size_t i = 0; i <= pool_size; i++) {
    // check reference bit
    auto victim = replacement_pool_[pointer_];
    if (is_referenced[victim]) {
      is_referenced[victim] = false;
    } else {
      *frame_id = victim;
      // call pin to remove the frame from replacement pool
      Pin(*frame_id);
      return true;
    }
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // the replacer does not have the frame, just return
  if (is_referenced.find(frame_id) == is_referenced.end()) {
    return;
  }

  // remove the frame from replacer
  is_referenced.erase(frame_id);
  for (auto iter = replacement_pool_.begin(); iter != replacement_pool_.end(); iter++) {
    if (*iter == frame_id) {
      replacement_pool_.erase(iter);
      break;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // if the unpinned frame is already in the replacer, simply return
  if (is_referenced.find(frame_id) != is_referenced.end()) {
    return;
  }
  // add the unpinned frame into replacement_pool_
  replacement_pool_.push_back(frame_id);
  is_referenced[frame_id] = true;
}

size_t LRUReplacer::Size() { return replacement_pool_.size(); }
}  // namespace bustub
