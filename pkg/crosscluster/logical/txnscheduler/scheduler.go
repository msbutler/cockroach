// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

func NewScheduler(lockCount int32) *Scheduler {
	scheduler := &Scheduler{
		lockTable: makeLockTable(lockCount),
		lockMap:   make(map[LockHash]lockList, lockCount),
	}
	return scheduler
}

type Scheduler struct {
	// lockMap tracks the holders of each lock. A holder is a txn. The lockList is
	// logically a list of transactions, but the data is stored in a pre-allocated
	// lock table. For a given row, we only need to keep track of the latest txn
	// that wants a write lock; therefore, the txn with the write lock will be
	// stored at the head of the list. Here's why:
	// - given a txn at t5 that wants to write at key A, it will depend on all
	// write on key before it, but we don't need to know all write times, just the
	// latest one. i.e. hold apply the txn at t5 until t4 has committed.
	// - so let's consider a world with just write locks: txn t5 comes in and
	// modifes A and B. Before scheduling there's a lock on A for t4 and a lock on
	// B for t3. Schedule() should return t4 and t3. This logic happens in
	// addWriteDependency(), after which, we add the incoming txn into the lock
	// map.
	//
	// Reads can happen in parallel (i.e if
	// ts4 and ts3 only have read locks on the same key, they can happen in
	// parallel), so when a write on the key comes in at t5, it needs to wait on
	// both t3 and t4. Therefore, the lockMap needs to track _multiple_ read txn's
	// for a given key, unlike the write lock.
	//
	//
	lockMap      map[LockHash]lockList
	lockTable    lockTable
	eventHorizon hlc.Timestamp

	// TODO(jeffswenson): how do I get rid of the Transaction allocations? I
	// guess I could flatten them into a timestamp array and a lock array.
	transactions []Transaction
}

type LockHash uint64

type lockEntryIndex int32

func (l lockEntryIndex) valid() bool {
	return 0 <= l
}

// Goals:
// 1. No allocations in the steady state.
// 2. Scheduling cost is O(txn.Size()) amortized.
func (s *Scheduler) Schedule(
	transaction Transaction, scratch []hlc.Timestamp,
) ([]hlc.Timestamp, hlc.Timestamp) {
	// TODO handle the edge case where the transaction is larger than the entire
	// scheduler.
	for s.lockTable.availableCapacity() < len(transaction.Locks) {
		s.pushEventHorizion()
	}

	dependencies := scratch[:0]
	s.transactions = append(s.transactions, transaction)

	for _, lock := range transaction.Locks {
		entryIndex, exists := s.lockMap[lock.Hash]
		switch {
		case !exists:
			entryIndex = s.lockTable.allocateList()
		case lock.IsRead:
			dependencies = s.lockTable.addReadDependency(entryIndex, dependencies)
		default:
			dependencies = s.lockTable.addWriteDependency(entryIndex, dependencies)
		}
		if lock.IsRead {
			s.lockMap[lock.Hash] = s.lockTable.addReadLock(entryIndex, transaction.CommitTime)
		} else {
			s.lockMap[lock.Hash] = s.lockTable.setWriteLock(entryIndex, transaction.CommitTime)
		}
	}

	return dependencies, s.eventHorizon
}

func (s *Scheduler) pushEventHorizion() {
	txn := s.transactions[0]
	s.transactions = s.transactions[1:]
	s.eventHorizon = txn.CommitTime
	for _, lock := range txn.Locks {
		locks, exists := s.lockMap[lock.Hash]
		if !exists {
			continue
		}

		if lock.IsRead {
			locks, exists = s.lockTable.removeReadLock(locks, txn.CommitTime)
		} else {
			locks, exists = s.lockTable.removeWriteLock(locks, txn.CommitTime)
		}

		if exists {
			s.lockMap[lock.Hash] = locks
		} else {
			delete(s.lockMap, lock.Hash)
		}
	}
}
