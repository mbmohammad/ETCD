package loadbalancer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type LoadBalancer struct {
	etcdClient       *clientv3.Client
	httpClient       *http.Client
	ginEngine        *gin.Engine
	metadataLock     sync.RWMutex
	requestTimeout   time.Duration
	maxRetries       int
	partitionCache   []*controller.PartitionMetadata
	nodeAddressCache map[int]string
}

func NewLoadBalancer(etcdClient *clientv3.Client) *LoadBalancer {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	lb := &LoadBalancer{
		etcdClient: etcdClient,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		ginEngine:      router,
		requestTimeout: 2 * time.Second,
		maxRetries:     3,
	}

	lb.setupRoutes()
	go lb.watchMetadata(context.Background())

	return lb
}

func (lb *LoadBalancer) getPartitionID(key string) int {
	h := sha256.Sum256([]byte(key))
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()
	if len(lb.partitionCache) == 0 {
		return -1
	}
	return int(h[0]) % len(lb.partitionCache)
}

func (lb *LoadBalancer) getLeaderForPartition(partitionID int) (string, error) {
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()

	if len(lb.partitionCache) == 0 {
		return "", errors.New("metadata not available")
	}

	if partitionID >= len(lb.partitionCache) {
		return "", fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	return lb.nodeAddressCache[lb.partitionCache[partitionID].Leader], nil
}

func (lb *LoadBalancer) getReplicaForRead(partitionID int) (string, error) {
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()

	if len(lb.partitionCache) == 0 {
		return "", errors.New("metadata not available")
	}

	if partitionID >= len(lb.partitionCache) {
		return "", fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	replicas := lb.partitionCache[partitionID].Replicas
	if len(replicas) == 0 {
		return lb.nodeAddressCache[lb.partitionCache[partitionID].Leader], nil
	}

	return lb.nodeAddressCache[replicas[rand.Intn(len(replicas))]], nil
}

func (lb *LoadBalancer) watchMetadata(ctx context.Context) {
	leaderCh := lb.etcdClient.Watch(ctx, "/leader")

	for {
		select {
		case wresp := <-leaderCh:
			for _, ev := range wresp.Events {
				fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
			lb.refreshMetadata(ctx)
		}
	}
}

func (lb *LoadBalancer) refreshMetadata(ctx context.Context) error {
	resp, err := lb.etcdClient.Get(ctx, "/leader")
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return errors.New("no leader found")
	}
	controllerAddr := string(resp.Kvs[0].Value)

	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+controllerAddr+"/metadata", nil)
	if err != nil {
		log.Printf("loadbalancer::updateMetadata: Failed to create request: %v\n", err)
		return err
	}

	metaResp, err := lb.httpClient.Do(req)
	if err != nil {
		log.Printf("loadbalancer::updateMetadata: Failed to get metadata: %v\n", err)
		return err
	}
	defer metaResp.Body.Close()

	if metaResp.StatusCode != http.StatusOK {
		log.Printf("loadbalancer::updateMetadata: Failed to get metadata: %s\n", metaResp.Status)
		return err
	}

	var metadata struct {
		NodeAddresses map[int]string       `json:"nodes"`
		Partitions    []*controller.PartitionMetadata `json:"partitions"`
	}
	if err := json.NewDecoder(metaResp.Body).Decode(&metadata); err != nil {
		log.Printf("loadbalancer::updateMetadata: Failed to decode metadata: %v\n", err)
		return err
	}

	lb.metadataLock.Lock()
	defer lb.metadataLock.Unlock()

	lb.partitionCache = metadata.Partitions
	lb.nodeAddressCache = metadata.NodeAddresses

	return nil

	partitions := make([]*controller.PartitionMetadata, 0)
	for _, ev := range resp.Kvs {
		var partition controller.PartitionMetadata
		if err := json.Unmarshal(ev.Value, &partition); err != nil {
			log.Printf("Failed to unmarshal partition metadata: %v", err)
			continue
		}
		partitions = append(partitions, &partition)
	}

	resp, err = lb.etcdClient.Get(ctx, "/nodes/", clientv3.WithPrefix())
	if err != nil {
		return err
	}

	nodes := make(map[int]string)
	for _, ev := range resp.Kvs {
		var node controller.NodeMetadata
		if err := json.Unmarshal(ev.Value, &node); err != nil {
			log.Printf("Failed to unmarshal node metadata: %v", err)
			continue
		}
		nodes[node.ID] = node.HttpAddress
	}

	lb.metadataLock.Lock()
	defer lb.metadataLock.Unlock()

	lb.partitionCache = partitions
	lb.nodeAddressCache = nodes

	return nil
}
