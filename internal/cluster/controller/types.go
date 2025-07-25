package controller

import "time"

type NodeStatus string

const (
	Creating NodeStatus = "creating"
	Syncing  NodeStatus = "syncing"
	Alive    NodeStatus = "alive"
	Dead     NodeStatus = "dead"
)

type NodeMetadata struct {
	ID          int        `json:"id"`
	HttpAddress string     `json:"http"`
	TcpAddress  string     `json:"tcp"`
	Status      NodeStatus `json:"status"`
	lastSeen    time.Time
	partitions  []int
}

type PartitionMetadata struct {
	PartitionID int   `json:"partitionId"`
	Leader      int   `json:"leader"`
	Replicas    []int `json:"replicas"`
}
