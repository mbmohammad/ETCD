package replica

import (
	"strconv"
	"testing"
)

func TestNewReplicaInitialization(t *testing.T) {
	rep := NewReplica(2, 3, Follower)
	if rep.NodeId != 2 || rep.PartitionId != 3 || rep.Mode != Follower {
		t.Errorf("Replica initialized incorrectly: %+v", rep)
	}
	if rep.lsm == nil || rep.data == nil {
		t.Error("Replica should initialize LSM and data map")
	}
}
func TestLeaderSetGetDelete(t *testing.T) {
	rep := NewReplica(2, 3, Leader)

	// Set
	log, err := rep.Set("foo", "bar", -1)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if log.Value != "bar" {
		t.Error("Set log value mismatch")
	}

	// Get
	getLog, err := rep.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if getLog.Value != "bar" {
		t.Error("Get returned wrong value")
	}

	// Delete
	delLog, err := rep.Delete("foo", -1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if delLog.Action != ReplicaActionDelete {
		t.Error("Delete log action incorrect")
	}

	_, err = rep.Get("foo")
	if err == nil {
		t.Error("Expected error when getting deleted key")
	}
}
func TestFollowerSetConflict(t *testing.T) {
	rep := NewReplica(2, 3, Follower)

	// First set
	_, err := rep.Set("x", "val1", 100)
	if err != nil {
		t.Fatalf("First set failed: %v", err)
	}

	// Older timestamp set
	_, err = rep.Set("x", "val2", 90)
	if err == nil {
		t.Error("Expected conflict error on older timestamp set")
	}
}
func TestFollowerDeleteConflict(t *testing.T) {
	rep := NewReplica(2, 3, Follower)

	// Initial set
	_, err := rep.Set("y", "val1", 100)
	if err != nil {
		t.Fatal(err)
	}

	// Conflict delete
	_, err = rep.Delete("y", 90)
	if err == nil {
		t.Error("Expected error when deleting with old timestamp")
	}
}
func TestReplicaModeTransition(t *testing.T) {
	rep := NewReplica(1, 1, Follower)

	rep.ConvertToLeader()
	if rep.Mode != Leader {
		t.Error("Replica should be Leader after ConvertToLeader")
	}

	rep.ConvertToFollower()
	if rep.Mode != Follower {
		t.Error("Replica should be Follower after ConvertToFollower")
	}
}
func TestSnapshotRoundTrip(t *testing.T) {
	leader := NewReplica(1, 1, Leader)
	leader.Set("a", "apple", -1)
	leader.Set("b", "banana", -1)
	snapshot := leader.GetSnapshot()

	// New follower receives snapshot
	follower := NewReplica(1, 1, Follower)
	follower.ReceiveSnapshot(snapshot)

	logEntry, err := follower.Get("a")
	if err != nil || logEntry.Value != "apple" {
		t.Error("Snapshot did not apply correctly for key 'a'")
	}
}
func TestApplyLogEntryRespectsTimestamps(t *testing.T) {
	rep := NewReplica(1, 1, Follower)

	// Apply newer first
	rep.applyLogEntry(ReplicaLog{Key: "k", Value: "new", Timestamp: 100, Action: ReplicaActionSet})

	// Attempt older
	rep.applyLogEntry(ReplicaLog{Key: "k", Value: "old", Timestamp: 50, Action: ReplicaActionSet})

	val, _ := rep.Get("k")
	if val.Value != "new" {
		t.Error("ApplyLogEntry incorrectly overwrote with older timestamp")
	}
}

//

func TestGetNonexistentKey(t *testing.T) {
	rep := NewReplica(1, 1, Follower)

	_, err := rep.Get("missing")
	if err == nil {
		t.Error("Expected error when getting nonexistent key")
	}
}

func TestDeleteNonexistentKey(t *testing.T) {
	rep := NewReplica(1, 1, Leader)

	_, err := rep.Delete("ghost", -1)
	if err == nil {
		t.Error("Expected error when deleting nonexistent key")
	}
}
func TestSnapshotEmptyReplica(t *testing.T) {
	rep := NewReplica(1, 1, Leader)

	snap := rep.GetSnapshot()
	if len(snap.Tables) != 0 {
		t.Errorf("Expected 0 tables in snapshot, got %d", len(snap.Tables))
	}

	newRep := NewReplica(1, 1, Follower)
	newRep.ReceiveSnapshot(snap)

	if len(newRep.data) != 0 {
		t.Error("Expected no data applied from empty snapshot")
	}
}
func TestSnapshotWithMixedLogEntries(t *testing.T) {
	leader := NewReplica(1, 1, Leader)
	leader.Set("x", "123", -1)
	leader.Set("y", "456", -1)
	leader.Delete("x", -1)

	snap := leader.GetSnapshot()
	follower := NewReplica(1, 1, Follower)
	follower.ReceiveSnapshot(snap)

	_, err := follower.Get("x")
	if err == nil {
		t.Error("Deleted key 'x' should not exist after snapshot")
	}
	log, err := follower.Get("y")
	if err != nil || log.Value != "456" {
		t.Error("Key 'y' missing or incorrect after snapshot")
	}
}
func TestFollowerSetSameTimestamp(t *testing.T) {
	rep := NewReplica(1, 1, Follower)

	_, err := rep.Set("dup", "one", 10)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rep.Set("dup", "two", 10)
	if err == nil {
		t.Error("Expected error for same timestamp set")
	}
}
func TestDoubleConvertToLeader(t *testing.T) {
	rep := NewReplica(1, 1, Follower)
	rep.Set("a", "alpha", 100)

	rep.ConvertToLeader()
	rep.ConvertToLeader() // should be no-op

	_, err := rep.Get("a")
	if err != nil {
		t.Error("Data should persist after ConvertToLeader")
	}
}
func TestConcurrentAccess(t *testing.T) {
	rep := NewReplica(1, 1, Leader)

	done := make(chan bool)

	go func() {
		for i := 0; i < 100; i++ {
			rep.Set("key", "value", -1)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			rep.Get("key")
		}
		done <- true
	}()

	<-done
	<-done
}
func TestLSMFlush(t *testing.T) {
	lsm := NewLSM()
	for i := 0; i < maxLen; i++ {
		lsm.AddLogEntry(ReplicaLog{Key: "k" + strconv.Itoa(i)})
	}

	if len(lsm.immutables) != 1 {
		t.Errorf("Expected 1 immutable table after flush, got %d", len(lsm.immutables))
	}
}
