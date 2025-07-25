package replica

import "strconv"

type ReplicaAction int

const (
	ReplicaActionSet = iota
	ReplicaActionGet
	ReplicaActionDelete
)

type ReplicaLog struct {
	PartitionId int
	NodeId      int
	Action      ReplicaAction
	Timestamp   int64
	Key         string
	Value       string
}

type ReplicaData struct {
	Value     string
	Timestamp int64
}

func (l *ReplicaLog) String() string {
	action := ""
	switch l.Action {
	case ReplicaActionSet:
		action = "SET"
	case ReplicaActionGet:
		action = "GET"
	case ReplicaActionDelete:
		action = "DELETE"
	}
	return "ReplicaLog{" +
		"PartitionId: " + strconv.FormatInt(int64(l.PartitionId), 10) +
		", NodeId: " + strconv.FormatInt(int64(l.NodeId), 10) +
		", Action: " + action +
		", Timestamp: " + strconv.FormatInt(l.Timestamp, 10) +
		", Key: " + l.Key +
		", Value: " + l.Value +
		"}"
}

type OldSetRequestError struct {
	key              string
	oldTimestamp     int64
	oldValue         string
	requestTimestamp int64
	newValue         string
}

type OldDeleteRequestError struct {
	key              string
	oldValue         string
	oldTimestamp     int64
	requestTimestamp int64
}

type KeyNotFoundError struct {
	key string
}

func (e *OldSetRequestError) Error() string {
	return "OldSetRequestError: the key \"" + e.key + "\" has a newer timestamp (" +
		strconv.FormatInt(e.oldTimestamp, 10) + ") and value \"" + e.oldValue +
		"\" than the set request value \"" + e.newValue + "\" with timestamp " +
		strconv.FormatInt(e.requestTimestamp, 10)
}

func (e *OldDeleteRequestError) Error() string {
	return "OldDeleteRequestError: the key \"" + e.key + "\" has a newer timestamp (" +
		strconv.FormatInt(e.oldTimestamp, 10) + ") and value \"" + e.oldValue +
		"\" than the delete request with timestamp " +
		strconv.FormatInt(e.requestTimestamp, 10)
}

func (e *KeyNotFoundError) Error() string {
	return "KeyNotFoundError: the key \"" + e.key + "\" was not found"
}
