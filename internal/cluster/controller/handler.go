package controller

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func (c *Controller) setupRoutes() {
	c.ginEngine.GET("/metadata", c.handleGetMetadata)
	c.ginEngine.GET("/node-metadata/:partitionID", c.handleGetNodeMetadata)

	c.ginEngine.POST("/partition/move-replica", c.handleMoveReplica)
	c.ginEngine.POST("/partition/set-leader", c.handleSetLeader)
	c.ginEngine.POST("/partitions/add", c.handleAddPartitions)
}

func (c *Controller) handleAddPartitions(ctx *gin.Context) {
	var req struct {
		Count int `json:"count"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.Count <= 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition count"})
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < req.Count; i++ {
		c.partitions = append(c.partitions, &PartitionMetadata{
			PartitionID: len(c.partitions),
			Leader:      -1,
			Replicas:    make([]int, 0),
		})
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Partitions added successfully"})
	log.Printf("controller::handleAddPartitions: Added %d new partitions\n", req.Count)
}

func (c *Controller) handleGetMetadata(ctx *gin.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	metadata := struct {
		NodeAddresses map[int]string       `json:"nodes"`
		Partitions    []*PartitionMetadata `json:"partitions"`
	}{}
	metadata.NodeAddresses = make(map[int]string)
	for id, node := range c.nodes {
		if node.Status == Alive {
			metadata.NodeAddresses[id] = node.HttpAddress
		}
	}
	metadata.Partitions = c.partitions

	ctx.JSON(http.StatusOK, metadata)
}

func (c *Controller) handleGetNodeMetadata(ctx *gin.Context) {
	partitionID, err := strconv.Atoi(ctx.Param("partitionID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	metadata := struct {
		Addresses []string `json:"addresses"`
	}{}
	c.mu.Lock()
	metadata.Addresses = make([]string, len(c.partitions[partitionID].Replicas))
	for i, replica := range c.partitions[partitionID].Replicas {
		if c.nodes[replica].Status == Alive {
			metadata.Addresses[i] = c.nodes[replica].TcpAddress
		}
	}
	c.mu.Unlock()

	ctx.JSON(http.StatusOK, metadata)
}


func (c *Controller) handleSetLeader(ctx *gin.Context) {
	var req struct {
		PartitionID int `json:"partition_id"`
		NodeID      int `json:"node_id"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	c.mu.Lock()
	if req.PartitionID < 0 || req.PartitionID >= len(c.partitions) {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	if c.partitions[req.PartitionID].Leader == req.NodeID {
		c.mu.Unlock()
		ctx.JSON(http.StatusOK, gin.H{"message": "Node is already the leader"})
		return
	}

	// Check if the new leader is already a replica, if not error
	partition := c.partitions[req.PartitionID]
	exists := false
	for _, replica := range partition.Replicas {
		if replica == req.NodeID {
			exists = true
			break
		}
	}
	if !exists {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Node is not a replica of the partition"})
		return
	}
	c.mu.Unlock()

	c.changeLeader(req.PartitionID, req.NodeID)

	ctx.JSON(http.StatusOK, gin.H{"message": "Leader set successfully"})
	log.Printf("controller::handleSetLeader: Node %d is now the leader for partition %d\n", req.NodeID, req.PartitionID)
}

func (c *Controller) handleRebalance(ctx *gin.Context) {

}

func (c *Controller) handleMoveReplica(ctx *gin.Context) {
	var req struct {
		PartitionID int `json:"partition_id"`
		From        int `json:"from"`
		To          int `json:"to"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	c.mu.RLock()
	if req.PartitionID < 0 || req.PartitionID >= len(c.partitions) {
		c.mu.RUnlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	partition := c.partitions[req.PartitionID]

	isLeader := false
	if partition.Leader == req.From {
		isLeader = true
	} else {
		exists := false
		for _, replica := range partition.Replicas {
			if replica == req.From {
				exists = true
				break
			}
		}
		if !exists {
			c.mu.RUnlock()
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Replica not found in partition"})
			return
		}
	}
	c.mu.RUnlock()

	log.Printf("controller::handleMoveReplica: Moving replica from node %d to node %d for partition %d\n", req.From, req.To, req.PartitionID)
	c.replicate(req.PartitionID, req.To)
	if isLeader {
		c.changeLeader(req.PartitionID, req.To)
		log.Printf("controller::handleMoveReplica: Node %d is now the leader for partition %d after moving replica\n", req.To, req.PartitionID)
	}
	c.removePartitionReplica(req.PartitionID, req.From)

	log.Printf("controller::handleMoveReplica: Replica moved from node %d to node %d for partition %d\n", req.From, req.To, req.PartitionID)
	ctx.JSON(http.StatusOK, gin.H{"message": "Replica moved successfully"})
}

func (c *Controller) handleReadyCheck(ctx *gin.Context) {

}
