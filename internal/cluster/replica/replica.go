package replica

import (
	"log"
	"strconv"
	"sync"
)

type ReplicaType int

const (
	Follower ReplicaType = iota
	Leader
)

type Replica struct {
	NodeId      int
	PartitionId int
	Mode        ReplicaType

	data      map[string]ReplicaData
	lsm       *LSM
	timestamp int64 // only used in Leader type
	mu        sync.RWMutex
}

func NewReplica(nodeId, partitionId int, mode ReplicaType) *Replica {
	return &Replica{
		NodeId:      nodeId,
		PartitionId: partitionId,
		Mode:        mode,
		data:        make(map[string]ReplicaData),
		lsm:         NewLSM(),
		timestamp:   0,
		mu:          sync.RWMutex{},
	}
}

// timestamp doesn't matter for leader, pass -1.
func (r *Replica) Set(key, value string, timestamp int64) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var ts int64
	if r.Mode == Leader {
		ts = r.timestamp
		r.timestamp++
	} else {
		ts = timestamp
		// Reject if data is newer
		// TODO: check if timestamp collision can happen
		if currentData, exists := r.data[key]; exists && currentData.Timestamp >= ts {
			err := &OldSetRequestError{
				key:              key,
				oldTimestamp:     currentData.Timestamp,
				oldValue:         currentData.Value,
				requestTimestamp: ts,
				newValue:         value,
			}
			log.Println("[replica.Set]", err.Error())
			return ReplicaLog{}, err
		}
	}

	r.data[key] = ReplicaData{Value: value, Timestamp: ts}

	logEntry := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		Action:      ReplicaActionSet,
		Timestamp:   ts,
		Key:         key,
		Value:       value,
	}

	if r.Mode == Leader {
		r.lsm.AddLogEntry(logEntry)
	}

	log.Println("[replica.Set]", r.modePrefix()+"SetKey:", logEntry.String())
	return logEntry, nil
}

func (r *Replica) Get(key string) (ReplicaLog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, ok := r.data[key]
	if !ok {
		err := &KeyNotFoundError{key}
		log.Println("[replica.Get]", err.Error())
		return ReplicaLog{}, err
	}

	logEntry := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		Action:      ReplicaActionGet,
		Timestamp:   -1,
		Key:         key,
		Value:       data.Value,
	}
	log.Println("[replica.Get]", r.modePrefix()+"GetKey:", logEntry.String())
	return logEntry, nil
}

// timestamp doesn't matter for leader, pass -1.
func (r *Replica) Delete(key string, timestamp int64) (ReplicaLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, ok := r.data[key]
	if !ok {
		err := &KeyNotFoundError{key}
		log.Println("[replica.Delete]", err.Error())
		return ReplicaLog{}, err
	}

	var ts int64
	if r.Mode == Leader {
		ts = r.timestamp
		r.timestamp++
	} else {
		ts = timestamp
		// TODO: check if timestamp collision can happen
		if data.Timestamp >= ts {
			err := &OldDeleteRequestError{
				key:              key,
				oldValue:         data.Value,
				oldTimestamp:     data.Timestamp,
				requestTimestamp: timestamp,
			}
			log.Println("[replica.Delete]", err.Error())
			return ReplicaLog{}, err
		}
	}

	delete(r.data, key)

	logEntry := ReplicaLog{
		PartitionId: r.PartitionId,
		NodeId:      r.NodeId,
		Action:      ReplicaActionDelete,
		Timestamp:   ts,
		Key:         key,
		Value:       "",
	}

	if r.Mode == Leader {
		r.lsm.AddLogEntry(logEntry)
	}

	log.Println("[replica.Delete]", r.modePrefix()+"DeleteKey:", logEntry.String())
	return logEntry, nil
}

// Mode transition
func (r *Replica) ConvertToLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Mode != Leader {
		r.Mode = Leader
		r.timestamp = 0

		r.lsm = NewLSM()
		r.copyDataToLSM()
		r.lsm.flush()
	}
}

func (r *Replica) ConvertToFollower() {
	r.mu.Lock()
	r.Mode = Follower
	r.timestamp = 0
	r.mu.Unlock()

	r.lsm.mu.Lock()
	defer r.lsm.mu.Unlock()
	r.lsm = NewLSM() // reset LSM, lock is held for previous LSM
}

func (r *Replica) modePrefix() string {
	if r.Mode == Leader {
		return "Leader"
	}
	return "Follower"
}

func (r *Replica) ReceiveSnapshot(snapshot *Snapshot) {
	if r.Mode == Follower {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.lsm.mu.Lock()
		defer r.lsm.mu.Unlock()

		r.lsm.immutables = []*MemTable{}

		// apply data from snapshot
		r.data = make(map[string]ReplicaData)
		for _, table := range snapshot.Tables {
			for _, logEntry := range table.Data {
				r.applyLogEntry(logEntry)
			}
		}

		// log number of data entries after applying snapshot
		log.Printf("[replica.ReceiveSnapshot] Node %d, partition %d applied snapshot resulting in %d data entries\n", r.NodeId, r.PartitionId, len(r.data))

		r.lsm.mem = NewMemTable()
	}
}

func (r *Replica) GetSnapshot() *Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.lsm.mem.Data) > 0 {
		r.lsm.flush()
	}

	r.lsm.mu.RLock()
	defer r.lsm.mu.RUnlock()

	snapshot := &Snapshot{
		Tables: make([]MemTable, len(r.lsm.immutables)),
	}

	for i, table := range r.lsm.immutables {
		snapshot.Tables[i] = *table
	}

	log.Println("[replica.ReceiveSnapshot]", "Snapshot created with", strconv.FormatInt(int64(len(snapshot.Tables)), 10), "tables")

	return snapshot
}

func (r *Replica) copyDataToLSM() {
	for key, data := range r.data {
		logEntry := ReplicaLog{
			PartitionId: r.PartitionId,
			NodeId:      r.NodeId,
			Action:      ReplicaActionSet,
			Timestamp:   data.Timestamp,
			Key:         key,
			Value:       data.Value,
		}
		r.lsm.AddLogEntry(logEntry)
	}
}

// does NOT acquire lock. assumes caller has already acquired the lock.
func (r *Replica) applyLogEntry(logEntry ReplicaLog) {
	if logEntry.Action == ReplicaActionSet {
		if currentData, exists := r.data[logEntry.Key]; !exists || currentData.Timestamp < logEntry.Timestamp {
			data := ReplicaData{
				Value:     logEntry.Value,
				Timestamp: logEntry.Timestamp,
			}
			r.data[logEntry.Key] = data
		} else {
			log.Println("[replica.applyLogEntry]", "Key", logEntry.Key, "already exists with a newer timestamp, ignoring snapshot entry")
		}
	} else if logEntry.Action == ReplicaActionDelete {
		if _, exists := r.data[logEntry.Key]; exists {
			delete(r.data, logEntry.Key)
		} else {
			log.Println("[replica.applyLogEntry]", "Key", logEntry.Key, "not found for deletion in snapshot")
		}
	} else {
		log.Println("[replica.applyLogEntry]", "Unknown action in snapshot entry:", logEntry.String())
	}
}
