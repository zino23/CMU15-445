//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <climits>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "common/macros.h"

namespace bustub {

/**
 * Reader-Writer latch which prioritizes on writer backed by std::mutex and
 * std::condition_variable
 */
class ReaderWriterLatch {
  using mutex_t = std::mutex;
  using cond_t = std::condition_variable;
  static const uint32_t MAX_READERS = UINT_MAX;

 public:
  ReaderWriterLatch() = default;
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }

  DISALLOW_COPY(ReaderWriterLatch);

  /**
   * Acquire a write latch.
   */
  void WLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    // Put the checking of condition predicate inside while loop in case another writer thread enters and set
    // writer_entered_ to true
    while (writer_entered_) {
      // This is tricky, we should put this thread in reader_ instead of writer_ cos when releasing writer lock, we will
      // wake up all threads in reader_
      // Note: the effect of this is there is at most 1 writer thread in writer_
      reader_.wait(latch);
    }
    writer_entered_ = true;
    while (reader_count_ > 0) {
      // Block subsequent readers and writers
      writer_.wait(latch);
    }
  }

  /**
   * Release a write latch.
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;
    // Because we put blocked read and write threads in reader_ when a writer enters, we can and should simply wake up
    // all threads in reader_ and let them compete according to the scheduler
    // Note: if we use reader_.notify_one() and wake up a reader, writers waiting on reader_ will never be woken up cos
    // RUnlock wakes up threads waiting on writer_
    reader_.notify_all();
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_ || reader_count_ == MAX_READERS) {
      reader_.wait(latch);
    }
    reader_count_++;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
    if (writer_entered_) {
      if (reader_count_ == 0) {
        writer_.notify_one();
      }
    } else {
      // Notify the reader thread blocked cos reader_count_ == MAX_READERS
      if (reader_count_ == MAX_READERS - 1) {
        reader_.notify_one();
      }
    }
  }

 private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_{0};
  bool writer_entered_{false};
};

class ReaderWriterLatchPreferReader {
 public:
  ReaderWriterLatchPreferReader() = default;

  void RLock() {
    std::lock_guard<mutex_t> guard(reader_lock_);
    reader_count_++;
    // First reader, acquire writer lock to block subsequent writer
    if (reader_count_ == 1) {
      WLock();
    }
  }

  void RUnlock() {
    std::lock_guard<mutex_t> guard(reader_lock_);
    reader_count_--;
    // No reader, release writer lock
    if (reader_count_ == 0) {
      WUnlock();
    }
  }

  void WLock() { writer_lock_.lock(); }
  void WUnlock() { writer_lock_.unlock(); }

 private:
  uint32_t reader_count_{0};
  using mutex_t = std::mutex;
  mutex_t reader_lock_;
  mutex_t writer_lock_;
};

}  // namespace bustub
