# Fix-MVCC-Visibility Branch — Complete ✅

## What Was Done

### 1. MVCC Visibility Wired (CRITICAL)
- btree.go 3 visibility sites now use `isVisible()`:
  - Get() line ~434
  - Delete() line ~498  
  - iterator.Next() line ~662 (uses per-scan snapshot via it.visCheck)
- Iterator struct has visCheck field; Scan() captures snapshot-based checker

### 2. Snapshot-Based Reads (kvstore.go)
- Get() and Scan(): use txnMgr.BeginTxn() for snapshot, stored in readSnaps sync.Map
- VisibilityChecker: reads from readSnaps, uses snap.IsVisible() with frozen ActiveXIDs
- Result: TestIsolation_WriteBatchAtomicity 20/20 PASS ✅

### 3. Commit Ordering Fixed
- Put(), Delete(), WriteBatch.Commit(): WAL write BEFORE txnMgr.Commit()
- WAL entry built manually (Type 7 = RecordTxnCommit)
- On WAL failure: txnMgr.Abort(xid) — prevents ghost entries

### 4. Quick Wins
- t.closed: was already atomic.Bool ✅
- Checkpoint fsync before rename: added ✅
- Root creation race: bootstrapMu.Lock() around new root creation ✅

## Test Results
- go test ./... -race -count=1 -timeout 180s: 11/11 packages PASS, race-clean
- TestIsolation_WriteBatchAtomicity: 20/20 PASS

## Commit
- 1b1130d fix: MVCC snapshot isolation + commit ordering + quick wins
- 4 files, 200 insertions(+), 50 deletions(-)

## Remaining from Audit
- Vacuum without page locks (CRITICAL): vacuum.go reads/writes without locking
- Iterator after Close (HIGH): reads pages after segment files close  
- goroutineID fragility (MEDIUM): parsing runtime.Stack for goroutine ID
- Delete across same-key splits (MEDIUM): needs verification