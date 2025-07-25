package replica

import (
	"sort"
	"sync"
)

const maxLen int = 1e4

type MemTable struct {
	Data []ReplicaLog
}

type Snapshot struct {
	Tables []MemTable
}

type LSM struct {
	mu         sync.RWMutex
	mem        *MemTable
	immutables []*MemTable
	wal        []ReplicaLog
}

func NewMemTable() *MemTable {
	return &MemTable{
		Data: []ReplicaLog{},
	}
}

func NewLSM() *LSM {
	return &LSM{
		mu:         sync.RWMutex{},
		mem:        NewMemTable(),
		immutables: []*MemTable{},
		wal:        []ReplicaLog{},
	}
}

func (l *LSM) AddLogEntry(logEntry ReplicaLog) {
	l.mu.Lock()
	l.mem.Data = append(l.mem.Data, logEntry)
	l.wal = append(l.wal, logEntry)
	l.mu.Unlock()

	if len(l.mem.Data) >= maxLen {
		l.flush()
	}
}

func (l *LSM) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.mem.Data) == 0 {
		return
	}

	l.immutables = append(l.immutables, l.mem)
	l.mem = NewMemTable()
}

func (l *LSM) WALAfter(ts int64) []ReplicaLog {
	l.mu.RLock()
	defer l.mu.RUnlock()

	idx := sort.Search(len(l.wal), func(i int) bool {
		return !(l.wal[i].Timestamp < ts)
	})
	return l.wal[idx:] // return a copy
}

// sortMemTableByKey sorts the entries of a MemTable by key (Data field in this example).
func sortMemTableByKey(mt *MemTable) {
	sort.Slice(mt.Data, func(i, j int) bool {
		return mt.Data[i].Key < mt.Data[j].Key
	})
}

// mergeOldest merges two sorted MemTables, keeping only the oldest ReplicaLog for each key.
func mergeOldest(a, b *MemTable) *MemTable {
	result := &MemTable{Data: make([]ReplicaLog, 0, len(a.Data)+len(b.Data))}
	i, j := 0, 0

	for i < len(a.Data) && j < len(b.Data) {
		if a.Data[i].Key < b.Data[j].Key {
			result.Data = append(result.Data, a.Data[i])
			i++
		} else if a.Data[i].Key > b.Data[j].Key {
			result.Data = append(result.Data, b.Data[j])
			j++
		} else {
			// Same key, pick the older one
			if a.Data[i].Timestamp < (b.Data[j].Timestamp) {
				result.Data = append(result.Data, a.Data[i])
			} else {
				result.Data = append(result.Data, b.Data[j])
			}
			i++
			j++
		}
	}

	for i < len(a.Data) {
		result.Data = append(result.Data, a.Data[i])
		i++
	}
	for j < len(b.Data) {
		result.Data = append(result.Data, b.Data[j])
		j++
	}

	return result
}

// CompactImmutables merges the first two immutable MemTables, deduplicating by key with oldest entry.
func (l *LSM) CompactImmutables() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.immutables) < 2 {
		return // Nothing to compact
	}

	// Sort first two memtables
	sortMemTableByKey(l.immutables[0])
	sortMemTableByKey(l.immutables[1])

	// Merge them
	merged := mergeOldest(l.immutables[0], l.immutables[1])

	// Replace first two with the merged table
	l.immutables = append([]*MemTable{merged}, l.immutables[2:]...)
}
