//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/config.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // std::unique_lock is movable and can work with condition variable well
  std::unique_lock<std::mutex> latch(latch_);

  // Check if txn is in the right state first
  if (!IsTxnValidToLock(txn)) {
    return false;
  }

  // Grant locks according to the txn's isolation level
  auto isolation_level = txn->GetIsolationLevel();
  txn_id_t txn_id = txn->GetTransactionId();

  // For isolation level READ_UNCOMMITTED, there is no notion of S-locks. No need to keep track of S-lock
  // information on this rid. Read whatever you want
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    return true;
  }

  // When a lock request arrives, add a record to the end of LockRequestQueue on
  // bucket of lock table if it exists, otherwise create a new LockRequestQueue
  // containing only the record of the request
  auto lock_rqst_queue_iter = lock_table_.find(rid);
  // Case 1: No txns hold S/X locks on this tuple. Make a new list, put request
  // in a new list and grant it
  if (lock_rqst_queue_iter == lock_table_.end()) {
    lock_table_.emplace(rid, std::make_unique<LockRequestQueue>());
    lock_table_[rid]->request_queue_.emplace_back(LockRequest(txn_id, LockMode::SHARED, true));
    txn->GetSharedLockSet()->emplace(rid);
    if (isolation_level == IsolationLevel::READ_COMMITTED) {
      // Release S-lock immediately
      Unlock(txn, rid);
    }
    return true;
  }

  // Case 2: Wait or grant immediately according to lock request queue
  auto lock_rqst_queue = lock_table_[rid].get();
  lock_rqst_queue->request_queue_.emplace_back(LockRequest(txn_id, LockMode::SHARED));
  /**
   * S-lock request on rid is granted until following conditions meet:
   * 1. No txns holding a lock (of course they made requests before current txn) on rid in mode EXCLUSIVE;
   * 2. No txns waiting for rid and made its request before current txn (prevent starvation)
   */
  // Grant lock with this pointer later (or make lock_rqst_queue stores
  // pointer?)
  LockRequest *lock_rqst = nullptr;
  // predicate to represent the above conditions
  auto predicate = [&]() {
    for (auto t : lock_rqst_queue->request_queue_) {
      // Conditions meet
      if (t.txn_id_ == txn_id) {
        lock_rqst = &t;
        return true;
      }

      // For txns made requests before current txn
      if (t.granted_ == false || t.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return false;
  };

  lock_rqst_queue->cv_.wait(latch, predicate);

  // Finally wake up!
  lock_rqst->granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  // Wake up my fellows!
  lock_rqst_queue->cv_.notify_all();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  if (!IsTxnValidToLock(txn)) {
    return false;
  }

  // For X-locks, REPEATABLE_READ, READ_COMMITTED and READ_UNCOMMITTED implementation is the same
  txn_id_t txn_id = txn->GetTransactionId();

  // No txns hold locks on rid
  auto lock_rqst_queue_iter = lock_table_.find(rid);
  if (lock_rqst_queue_iter == lock_table_.end()) {
    lock_table_.emplace(rid, std::make_unique<LockRequestQueue>());
    auto lock_rqst_queue = lock_table_[rid].get();
    lock_rqst_queue->request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE, true);
    txn->GetExclusiveLockSet()->emplace(rid);
    return true;
  }

  auto lock_rqst_queue = lock_table_[rid].get();
  lock_rqst_queue->request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);
  // X-lock conflicts with both S- and X-locks
  // Grant X-lock until current request is the first in request queue
  auto predicate = [&]() {
    auto i = lock_rqst_queue->request_queue_.begin();
    return i->txn_id_ == txn_id;
  };
  lock_rqst_queue->cv_.wait(latch, predicate);

  // Finally wake up
  lock_rqst_queue->request_queue_.front().granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_rqst_queue->cv_.notify_all();
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  if (!IsTxnValidToLock(txn)) {
    return false;
  }

  assert(lock_table_.find(rid) != lock_table_.end());
  auto lock_rqst_queue = lock_table_[rid].get();
  assert(!lock_rqst_queue->request_queue_.empty());

  txn_id_t txn_id = txn->GetTransactionId();
  for (auto iter = lock_rqst_queue->request_queue_.begin(); iter != lock_rqst_queue->request_queue_.end();
       iter++) {
    if (iter->txn_id_ == txn_id) {
      assert(iter->granted_ == true && iter->lock_mode_ == LockMode::SHARED);
      // If another txn is upgrading their lock, abort this txn
      if (lock_rqst_queue->upgrading_ == true) {
        txn->SetState(TransactionState::ABORTED);
        // throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      // Upgrade lock mode and ungrant it
      iter->lock_mode_ = LockMode::EXCLUSIVE;
      iter->granted_ = false;
      lock_rqst_queue->upgrading_ = true;
      // Remember to remove record from txn's lock set
      txn->GetSharedLockSet()->erase(rid);
      // Move this request to the end of request queue
      // Note: do not unlock it and make a X-lock request which will introduce a lot of work
      // TODO(silentroar): it seems that I really need to make LockRequestQueue holds a unique_ptr on request
      // queue
      lock_rqst_queue->request_queue_.splice(lock_rqst_queue->request_queue_.end(),
                                             lock_rqst_queue->request_queue_, iter);
      break;
    }
  }

  latch.unlock();
  bool upgrade = LockExclusive(txn, rid);
  // Whether upgrade succeeds, set upgrading_ to false
  lock_rqst_queue->upgrading_ = false;
  return upgrade;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> latch(latch_);
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }

  // Release S-locks immediately if the txn's isolation level is READ_COMMITTED
  // Note: txn does not go into SHRINKING state
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->IsSharedLocked(rid)) {
    assert(txn->GetState() == TransactionState::GROWING);
    txn->GetSharedLockSet()->erase(rid);
    // Erase lock request from lock_rqst_queue
    auto lock_rqst_queue = lock_table_[rid].get();
    lock_rqst_queue->request_queue_.remove_if(
        [&](LockRequest lock_rqst) { return txn->GetTransactionId() == lock_rqst.txn_id_; });
    // Notify txns blocked waiting on rid, either release S- or X-lock
    lock_rqst_queue->cv_.notify_all();
    return true;
  }

  // Lock manager only implements 2PL, Strict 2PL can be achieved when Unlock is called in
  // TransactionManager::Commit
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // Erase lock from txn
  if (txn->IsSharedLocked(rid)) {
    txn->GetSharedLockSet()->erase(rid);
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
  }

  // Erase lock request from lock_rqst_queue
  auto lock_rqst_queue = lock_table_[rid].get();
  lock_rqst_queue->request_queue_.remove_if(
      [&](LockRequest lock_rqst) { return txn->GetTransactionId() == lock_rqst.txn_id_; });
  // Notify txns blocked waiting on rid, either release S- or X-lock
  lock_rqst_queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> latch(latch_);
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::vector<txn_id_t>{t2});
  } else {
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> latch(latch_);
  assert(waits_for_.find(t1) != waits_for_.end());
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); ++it) {
    if (*it == t2) {
      waits_for_[t1].erase(it);
      break;
    }
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  /**
   * 1. Build the graph
   * 2. Detect cycle. In order to make cycle detection deterministic, choose the first txn and its neighbors
   * in sorted order from lowest to highest
   * 3. Abort the youngest txn assuming it has completed the least work and thus does not waste too much
   * computation
   * 4. Notify threads waiting on the the same rid
   *
   * Consider the following request_queue_ on rid_a
   * t1 <- t2 <- t3 <- t4
   * S     X     S     S
   * when t2 aborts, t2 should notify threads waiting on rid_a, otherwise t3 and t4 will sleep forever
   */

  /**
   * Two scenarios:
   * 1. txn A with S-lock request is waiting on txn B holding a X-lock. Request B is the first request in
   * request_queue_
   * 2. txn A with X-lock request is waiting on
   * 2.1 one or multiple txns holding S-locks
   * 2.2 one txn holding X-lock
   */
  for (auto &i : lock_table_) {
    auto request_queue = (i.second)->request_queue_;
    std::vector<txn_id_t> wait_for_vec;
    txn_id_t txn_id = -1;
    for (auto lock_rqst : request_queue) {
      if (lock_rqst.granted_ == true) {
        /**
         * Simply add txns with granted locks into wait_for_vec cos there will not be conflictions
         * Possible order of granted locks:
         * 1. S <- S <- ... <- S <- {waiting-X}
         * 2. X <- {waiting-X / waiting-S} (the granted X-lock must be the first in request_queue_ and has
         * no following granted locks)
         */
        wait_for_vec.emplace_back(lock_rqst.txn_id_);
      } else {
        txn_id = lock_rqst.txn_id_;
      }
    }

    // It is possible all locks are granted and no txns is waiting on this rid
    if (txn_id != -1) {
      waits_for_.emplace(txn_id, wait_for_vec);
    }

    wait_for_vec.clear();
  }

  // No wait-for graph
  if (waits_for_.empty()) {
    return false;
  }

  // Make sure we traverse all the txns
  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> in_cycle;
  std::vector<txn_id_t> cycle;
  bool has_cycle = false;
  while (visited.size() < waits_for_.size()) {
    in_cycle.clear();
    cycle.clear();
    // Start with txn with the lowest id
    txn_id_t min_txn_id = MinUnvisitedTxn(visited);
    visited.emplace(min_txn_id);
    in_cycle.emplace(min_txn_id);
    cycle.emplace_back(min_txn_id);
    has_cycle = dfs(min_txn_id, txn_id, visited, cycle, in_cycle);
    if (has_cycle == true) {
      return has_cycle;
    }
  }
  return has_cycle;
}

bool LockManager::dfs(txn_id_t txn_id, txn_id_t *aborted_txn_id, std::unordered_set<txn_id_t> &visited,
                      std::vector<txn_id_t> &cycle, std::unordered_set<txn_id_t> &in_cycle) {
  if (waits_for_.find(txn_id) == waits_for_.end()) {
    return false;
  }

  for (auto neighbor_txn_id : waits_for_[txn_id]) {
    // Cycle detected
    if (in_cycle.find(neighbor_txn_id) != in_cycle.end()) {
      sort(cycle.begin(), cycle.end());
      *aborted_txn_id = cycle[0];
      return true;
    }

    // Neighbor has not been visited
    if (visited.find(neighbor_txn_id) == visited.end()) {
      visited.emplace(neighbor_txn_id);
      in_cycle.emplace(neighbor_txn_id);
      cycle.emplace_back(neighbor_txn_id);
      if (dfs(neighbor_txn_id, aborted_txn_id, visited, cycle, in_cycle)) {
        return true;
      }
      in_cycle.erase(neighbor_txn_id);
      removeFromVector(cycle, neighbor_txn_id);
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::unique_lock<std::mutex> latch(latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edge_list;
  for (const auto &i : waits_for_) {
    auto t1 = i.first;
    auto vec = i.second;
    for (auto t2 : vec) {
      edge_list.emplace_back(t1, t2);
    }
  }
  return edge_list;
}

// void LockManager::RunCycleDetection() {}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and
      // abort code here
      try {
        txn_id_t aborted_txn_id = -1;
        if (HasCycle(&aborted_txn_id)) {
          auto aborted_txn = TransactionManager::GetTransaction(aborted_txn_id);
          aborted_txn->SetState(TransactionState::ABORTED);
          // throw TransactionAbortException(aborted_txn->GetTransactionId(), AbortReason::DEADLOCK);
        }
      } catch (TransactionAbortException &e) {
      }
    }
  }
}

bool LockManager::IsTxnValidToLock(Transaction *txn) {
  if (txn->GetState() == TransactionState::GROWING) {
    return true;
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  return false;
}

}  // namespace bustub
