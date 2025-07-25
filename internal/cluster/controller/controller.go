package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Controller struct {
	ginEngine         *gin.Engine
	etcdClient        *clientv3.Client
	mu                sync.RWMutex
	partitionCount    int
	replicationFactor int
	nodes             map[int]*NodeMetadata
	partitions        []*PartitionMetadata
	httpClient        *http.Client
	networkName       string
	nodeImage         string
}

func NewEtcdClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
}

func NewController(partitionCount, replicationFactor int, networkName, nodeImage string, etcdClient *clientv3.Client) *Controller {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	c := &Controller{
		ginEngine:         router,
		etcdClient:        etcdClient,
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		nodes:             make(map[int]*NodeMetadata),
		partitions:        make([]*PartitionMetadata, partitionCount),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		networkName: networkName,
		nodeImage:   nodeImage,
	}

	for i := 0; i < partitionCount; i++ {
		c.partitions[i] = &PartitionMetadata{
			PartitionID: i,
			Leader:      -1, // Will be set when first node joins
			Replicas:    make([]int, 0),
		}
	}

	c.setupRoutes()
	// go c.eventHandler()

	return c
}

func (c *Controller) Start(addr, controllerID string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpServer := &http.Server{
		Addr:    addr,
		Handler: c.ginEngine,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("controller::Start: Failed to run http server %v", err)
		}
	}()

	go c.startLeaderElection(ctx, controllerID)

	<-ctx.Done()
	log.Printf("controller::Start: Shutting down gracefully...")

	// Shutdown the http server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("controller::Start: Failed to shutdown http server: %v", err)
	}
}

func (c *Controller) startLeaderElection(ctx context.Context, controllerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			s, err := concurrency.NewSession(c.etcdClient, concurrency.WithTTL(10))
			if err != nil {
				log.Printf("controller::startLeaderElection: Failed to create etcd session: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			e := concurrency.NewElection(s, "/leader")
			if err := e.Campaign(ctx, controllerID); err != nil {
				log.Printf("controller::startLeaderElection: Failed to campaign for leadership: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			log.Printf("controller::startLeaderElection: Controller %s became the leader", controllerID)

			// The new leader should write its address to etcd
			_, err = c.etcdClient.Put(ctx, "/leader", controllerID)
			if err != nil {
				log.Printf("controller::startLeaderElection: Failed to write leader address to etcd: %v", err)
			}

			c.watchNodes(ctx)

			log.Printf("controller::startLeaderElection: Controller %s lost leadership", controllerID)
		}
	}
}

func (c *Controller) watchNodes(ctx context.Context) {
	rch := c.etcdClient.Watch(ctx, "/nodes/", clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			if ev.Type == clientv3.EventTypePut {
				var node NodeMetadata
				if err := json.Unmarshal(ev.Kv.Value, &node); err != nil {
					log.Printf("Failed to unmarshal node metadata: %v", err)
					continue
				}
				c.mu.Lock()
				if _, ok := c.nodes[node.ID]; !ok {
					c.nodes[node.ID] = &node
					go c.makeNodeReady(node.ID)
				}
				c.mu.Unlock()
			}
		}
	}
}

func (c *Controller) makeNodeReady(nodeID int) {
	partitionsToAssign := make([]int, 0)
	c.mu.Lock()
	for _, partition := range c.partitions {
		if len(partition.Replicas) < c.replicationFactor {
			partitionsToAssign = append(partitionsToAssign, partition.PartitionID)
		}
	}
	c.mu.Unlock()

	c.replicatePartitions(partitionsToAssign, nodeID)

	c.mu.Lock()
	c.nodes[nodeID].Status = Alive
	c.mu.Unlock()
	log.Printf("controller::makeNodeReady: Node %d is now ready\n", nodeID)
}

func (c *Controller) replicatePartitions(partitionsToAssign []int, nodeID int) {
	for _, partitionID := range partitionsToAssign {
		partition := c.partitions[partitionID]
		for i := 0; i < 3; i++ {
			err := c.replicate(partitionID, nodeID)
			if err == nil {
				c.mu.Lock()
				if partition.Leader == -1 {
					partition.Leader = nodeID
				} else {
					partition.Replicas = append(partition.Replicas, nodeID)
				}
				c.nodes[nodeID].partitions = append(c.nodes[nodeID].partitions, partitionID)
				c.mu.Unlock()
				log.Printf("controller::makeNodeReady: replicate successfully partition %d to node %d\n", partitionID, nodeID)
				break
			} else {
				log.Printf("controller::makeNodeReady: Failed to replicate %d in node %d: %v\n", partitionID, nodeID, err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Controller) replicate(partitionID, nodeID int) error {
	var addr string
	c.mu.RLock()
	if c.partitions[partitionID].Leader == -1 {
		addr = fmt.Sprintf("%s/add-partition/%d", c.nodes[nodeID].HttpAddress, partitionID)
	} else {
		addr = fmt.Sprintf("%s/send-partition/%d/%s", c.nodes[c.partitions[partitionID].Leader].HttpAddress, partitionID, c.nodes[nodeID].TcpAddress)
	}
	c.mu.RUnlock()

	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("resp status not OK: " + resp.Status)
	}

	return nil
}

func (c *Controller) doNodeRequest(method, addr string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, addr, nil)
	if err != nil {
		return nil, err
	}

	return c.httpClient.Do(req)
}

func (c *Controller) monitorHeartbeat() {
	for {
		c.mu.Lock()
		for _, node := range c.nodes {
			if time.Since(node.lastSeen) > 5*time.Second {
				if node.Status == Alive {
					log.Printf("controller::monitorHeartbeat: Node %d is not responding\n", node.ID)
					node.Status = Dead

					go c.handleFailover(node.ID)
				}
			}
		}
		c.mu.Unlock()

		time.Sleep(2 * time.Second)
	}
}

func (c *Controller) handleFailover(nodeID int) {
	log.Printf("controller::handleFailover: Handling failover for node %d\n", nodeID)

	c.mu.Lock()
	defer c.mu.Unlock()
	node, exists := c.nodes[nodeID]
	if !exists || node.Status != Dead {
		return
	}

	for _, partitionID := range node.partitions {
		if c.partitions[partitionID].Leader == nodeID {
			// set another node as leader
			if len(c.partitions[partitionID].Replicas) > 0 {
				newLeader := c.partitions[partitionID].Replicas[0]
				c.partitions[partitionID].Leader = newLeader
				log.Printf("controller::handleFailover: Node %d is now the leader for partition %d\n", newLeader, partitionID)
				// notify the new leader to take over
				addr := fmt.Sprintf("%s/set-leader/%d", c.nodes[newLeader].HttpAddress, partitionID)
				resp, err := c.doNodeRequest("POST", addr)
				if err != nil {
					log.Printf("controller::handleFailover: Failed to notify new leader for partition %d: %v\n", partitionID, err)
					continue
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					log.Printf("controller::handleFailover: Failed to set new leader for partition %d: %s\n", partitionID, resp.Status)
				} else {
					log.Printf("controller::handleFailover: New leader for partition %d is node %d\n", partitionID, newLeader)
					// remove replica[0]
					c.partitions[partitionID].Replicas = c.partitions[partitionID].Replicas[1:]
				}
			}
		} else {
			// remove this node from replicas
			for i, replica := range c.partitions[partitionID].Replicas {
				if replica == nodeID {
					c.partitions[partitionID].Replicas = append(c.partitions[partitionID].Replicas[:i], c.partitions[partitionID].Replicas[i+1:]...)
					break
				}
			}
		}
	}
}

func (c *Controller) reviveNode(nodeID int) {
	c.mu.Lock()
	node, exists := c.nodes[nodeID]
	if !exists || node.Status != Dead {
		c.mu.Unlock()
		return
	}
	node.Status = Syncing
	c.mu.Unlock()

	log.Printf("controller::reviveNode: Reviving node %d\n", nodeID)

	partitionsToReplicate := make([]int, 0)
	c.mu.Lock()
	for _, partition := range node.partitions {
		partitionsToReplicate = append(partitionsToReplicate, partition)
	}
	node.partitions = make([]int, 0)
	c.mu.Unlock()
	c.replicatePartitions(partitionsToReplicate, nodeID)

	c.mu.Lock()
	c.nodes[nodeID].Status = Alive
	c.mu.Unlock()
	log.Printf("controller::reviveNode: Node %d revived successfully\n", nodeID)
}

func (c *Controller) changeLeader(partitionID, newLeaderID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	partition := c.partitions[partitionID]

	// notify the old leader to become follower
	oldLeaderID := partition.Leader
	if oldLeaderID != -1 {
		addr := fmt.Sprintf("%s/set-follower/%d", c.nodes[oldLeaderID].HttpAddress, partitionID)
		resp, err := c.doNodeRequest("POST", addr)
		if err != nil {
			log.Printf("controller::changeLeader: Failed to notify old leader %d for partition %d: %v\n", oldLeaderID, partitionID, err)
			return fmt.Errorf("failed to notify old leader")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("controller::changeLeader: Failed to set old leader %d as follower for partition %d: %s\n", oldLeaderID, partitionID, resp.Status)
			return fmt.Errorf("failed to set old leader as follower for partition %d: %v", partitionID, resp.Status)
		}
		log.Printf("controller::changeLeader: Node %d is now a follower for partition %d\n", oldLeaderID, partitionID)
	}

	// notify the new leader to take over
	addr := fmt.Sprintf("%s/set-leader/%d", c.nodes[newLeaderID].HttpAddress, partitionID)
	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		log.Printf("controller::changeLeader: Failed to notify new leader for partition %d: %v\n", partitionID, err)
		return fmt.Errorf("failed to notify new leader")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("controller::changeLeader: Failed to set new leader for partition %d: %s\n", partitionID, resp.Status)
		return fmt.Errorf("failed to set new leader for partition %d: %v", partitionID, resp.Status)
	}

	// remove new leader from replicas
	for i, replica := range partition.Replicas {
		if replica == newLeaderID {
			partition.Replicas = append(partition.Replicas[:i], partition.Replicas[i+1:]...)
			log.Printf("controller::changeLeader: Removed node %d from replicas of partition %d\n", newLeaderID, partitionID)
			break
		}
	}

	// add old leader to replicas
	if oldLeaderID != -1 && oldLeaderID != newLeaderID {
		partition.Replicas = append(partition.Replicas, oldLeaderID)
		log.Printf("controller::changeLeader: Added old leader %d to replicas of partition %d\n", oldLeaderID, partitionID)
	}

	partition.Leader = newLeaderID
	log.Printf("controller::changeLeader: Partition %d leader changed to node %d\n", partitionID, newLeaderID)

	return nil
}

func (c *Controller) removePartitionReplica(partitionID, nodeID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	partition := c.partitions[partitionID]
	if partition == nil {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	for i, replica := range partition.Replicas {
		if replica == nodeID {
			partition.Replicas = append(partition.Replicas[:i], partition.Replicas[i+1:]...)
			log.Printf("controller::removePartitionReplica: Removed node %d from replicas of partition %d\n", nodeID, partitionID)
			return nil
		}
	}

	addr := fmt.Sprintf("%s/delete-partition/%d", c.nodes[nodeID].HttpAddress, partitionID)

	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("resp status not OK: " + resp.Status)
	}

	log.Printf("controller::removePartitionReplica: Node %d removed from partition %d successfully\n", nodeID, partitionID)
	return nil
}
