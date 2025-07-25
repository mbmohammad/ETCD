package loadbalancer

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (lb *LoadBalancer) setupRoutes() {
	lb.ginEngine.GET("/ping", lb.handleHealthCheck)
	lb.ginEngine.GET("/client/:key", lb.handleGet)
	lb.ginEngine.POST("/client/:key/:value", lb.handleSet)
	lb.ginEngine.DELETE("/client/:key", lb.handleDelete)

	lb.ginEngine.NoRoute(func(c *gin.Context) {
		log.Printf("loadbalancer::NoRoute: Unmatched request: %s %s", c.Request.Method, c.Request.URL.Path)
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
	})
}

func (lb *LoadBalancer) Run(addr string) error {
	return lb.ginEngine.Run(addr)
}

func (lb *LoadBalancer) handleHealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (lb *LoadBalancer) handleGet(c *gin.Context) {
	key := c.Param("key")
	partitionID := lb.getPartitionID(key)

	nodeAddr, err := lb.getReplicaForRead(partitionID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	var resp *http.Response
	var responseBody []byte

	for i := 0; i < lb.maxRetries; i++ {
		resp, err = lb.doNodeRequest(http.MethodGet, nodeAddr, strconv.Itoa(partitionID), key, "")
		if err == nil {
			defer resp.Body.Close()
			responseBody, err = io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
				break
			}
		}

		log.Printf("Error during request to %s for partition %d: %v", nodeAddr, partitionID, err)
		if i < lb.maxRetries-1 {
			lb.refreshMetadata(c.Request.Context())
			nodeAddr, _ = lb.getLeaderForPartition(partitionID)
			time.Sleep(200 * time.Millisecond)
		}
		log.Printf("Retrying request to %s for partition %d, attempt %d", nodeAddr, partitionID, i+1)
	}

	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	// Forward the response
	log.Printf("loadbalancer::handleGet: Response from node: %s, status: %d", string(responseBody), resp.StatusCode)
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), responseBody)
}

func (lb *LoadBalancer) handleSet(c *gin.Context) {
	key := c.Param("key")
	value := c.Param("value")

	partitionID := lb.getPartitionID(key)
	if partitionID == -1 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No partitions available"})
		return
	}
	leaderAddr, err := lb.getLeaderForPartition(partitionID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	var resp *http.Response
	var responseBody []byte

	for i := 0; i < lb.maxRetries; i++ {
		resp, err = lb.doNodeRequest(c.Request.Method, leaderAddr, strconv.Itoa(partitionID), key, value)
		if err == nil {
			defer resp.Body.Close()
			responseBody, err = io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
				break
			}
		}

		log.Printf("Error during request to %s for partition %d: %v", leaderAddr, partitionID, err)
		if i < lb.maxRetries-1 {
			lb.refreshMetadata(c.Request.Context())
			leaderAddr, _ = lb.getLeaderForPartition(partitionID)
			time.Sleep(200 * time.Millisecond)
		}
		log.Printf("Retrying request to %s for partition %d, attempt %d", leaderAddr, partitionID, i+1)
	}

	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), responseBody)
}

func (lb *LoadBalancer) handleDelete(c *gin.Context) {
	key := c.Param("key")
	partitionID := lb.getPartitionID(key)
	leaderAddr, err := lb.getLeaderForPartition(partitionID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	var resp *http.Response
	var responseBody []byte

	for i := 0; i < lb.maxRetries; i++ {
		resp, err = lb.doNodeRequest(http.MethodDelete, leaderAddr, strconv.Itoa(partitionID), key, "")
		if err == nil {
			defer resp.Body.Close()
			responseBody, err = io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
				break
			}
		}

		log.Printf("Error during request to %s for partition %d: %v", leaderAddr, partitionID, err)
		if i < lb.maxRetries-1 {
			lb.refreshMetadata(c.Request.Context())
			leaderAddr, _ = lb.getLeaderForPartition(partitionID)
			time.Sleep(200 * time.Millisecond)
		}
		log.Printf("Retrying request to %s for partition %d, attempt %d", leaderAddr, partitionID, i+1)
	}

	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), responseBody)
}

func (lb *LoadBalancer) doNodeRequest(method, nodeAddr, partitionID, key, value string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), lb.requestTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/%s/%s/%s", nodeAddr, partitionID, key, value)
	log.Printf("loadbalancer::doNodeRequest: Sending request to %s", url)
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}

	return lb.httpClient.Do(req)
}
