package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
	"github.com/gin-gonic/gin"
)

func (n *Node) setupRoutes() {
	n.ginEngine.GET("/:partition_id/:key", n.handleGet)
	n.ginEngine.POST("/:partition_id/:key/:value", n.handleSet)
	n.ginEngine.DELETE("/:partition_id/:key", n.handleDelete)
	n.ginEngine.POST("/add-partition/:partition_id", n.handleAddPartition)
	n.ginEngine.POST("/send-partition/:partition_id/:node_addr", n.handleSendPartition)
	n.ginEngine.POST("/set-leader/:partition_id", n.handleSetLeader)
	n.ginEngine.POST("/set-follower/:partition_id", n.handleSetFollower)
	n.ginEngine.POST("/delete-partition/:partition_id", n.handleDeletePartition)
}

func (n *Node) handleGet(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}
	key := c.Param("key")

	value, err := n.get(partitionID, key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"value": value})
}

func (n *Node) handleSet(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}
	key := c.Param("key")
	value := c.Param("value")

	err = n.set(partitionID, time.Now().UnixNano(), key, value, replica.Leader)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) handleDelete(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}
	key := c.Param("key")

	err = n.delete(partitionID, time.Now().UnixNano(), key, replica.Leader)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) handleAddPartition(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	n.replicasMapMutex.Lock()
	defer n.replicasMapMutex.Unlock()
	if _, ok := n.replicas[partitionID]; !ok {
		n.replicas[partitionID] = replica.NewReplica(n.Id, partitionID, replica.Follower)
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) handleSendPartition(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}
	nodeAddr := c.Param("node_addr")

	err = n.sendSnapshotToNode(partitionID, nodeAddr)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) handleSetLeader(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	n.replicasMapMutex.Lock()
	defer n.replicasMapMutex.Unlock()
	if r, ok := n.replicas[partitionID]; ok {
		r.Mode = replica.Leader
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Partition not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) handleSetFollower(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	n.replicasMapMutex.Lock()
	defer n.replicasMapMutex.Unlock()
	if r, ok := n.replicas[partitionID]; ok {
		r.Mode = replica.Follower
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Partition not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) handleDeletePartition(c *gin.Context) {
	partitionID, err := strconv.Atoi(c.Param("partition_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	n.replicasMapMutex.Lock()
	defer n.replicasMapMutex.Unlock()
	delete(n.replicas, partitionID)

	c.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (n *Node) getNodesContainingPartition(partitionId int) ([]string, error) {
	resp, err := n.etcdClient.Get(context.Background(), "/leader")
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("no leader found")
	}
	controllerAddr := string(resp.Kvs[0].Value)

	req, err := http.NewRequest("GET", "http://"+controllerAddr+"/node-metadata/"+strconv.Itoa(partitionId), nil)
	if err != nil {
		return nil, err
	}

	metaResp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer metaResp.Body.Close()

	if metaResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get metadata: %s", metaResp.Status)
	}

	var metadata struct {
		Addresses []string `json:"addresses"`
	}
	if err := json.NewDecoder(metaResp.Body).Decode(&metadata); err != nil {
		return nil, err
	}

	return metadata.Addresses, nil
}
