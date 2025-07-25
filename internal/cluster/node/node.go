package node

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	REQUEST_TIMEOUT = 2 * time.Second
	HEARTBEAT_TIMER = 2 * time.Second
	HTTP_ADDRESS    = "0.0.0.0:8000"
	TCP_ADDRESS     = "0.0.0.0:9000"
)

type MessageType int

const (
	Set MessageType = iota
	Get
	Delete
	Snapshot
)

type Message struct {
	Type        MessageType
	PartitionId int
	Timestamp   int64
	Key         string
	Value       string
	Snapshot    replica.Snapshot
}

type Response struct {
	Value string
	Error error
}

type Node struct {
	Id               int
	replicas         map[int]*replica.Replica // partitionId, replica of that partiotionId
	etcdClient       *clientv3.Client
	replicasMapMutex sync.RWMutex
	ginEngine        *gin.Engine
	httpClient       *http.Client
}

func NewNode(id int, etcdClient *clientv3.Client) *Node {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	return &Node{
		Id:         id,
		replicas:   make(map[int]*replica.Replica),
		etcdClient: etcdClient,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		replicasMapMutex: sync.RWMutex{},
		ginEngine:        router,
	}
}

func (n *Node) Start() {
	go n.registerAndMaintainLease(context.Background())
	go n.tcpListener(n.nodeConnectionHandler)

	n.setupRoutes()
	n.ginEngine.Run(HTTP_ADDRESS)
}

func (n *Node) tcpListener(handler func(Message) Response) {
	ln, err := net.Listen("tcp", TCP_ADDRESS)
	if err != nil {
		log.Fatalf("[node.tcpListener] Node failed to listen on address %s: %v", TCP_ADDRESS, err)
	}
	log.Printf("[node.tcpListener] Listening on address %s", TCP_ADDRESS)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[node.tcpListener] Connection accept error: %v", err)
			continue
		}
		go func(conn net.Conn) {
			defer conn.Close()
			decoder := gob.NewDecoder(conn)
			encoder := gob.NewEncoder(conn)

			var msg Message
			if err := decoder.Decode(&msg); err != nil {
				log.Printf("[node.tcpListener] Failed to decode message: %v", err)
				return
			}

			resp := handler(msg)
			if err := encoder.Encode(resp); err != nil {
				log.Printf("[node.tcpListener] Failed to send response: %v", err)
				return
			}
		}(conn)
	}
}

// This function handels requests of some node containing a leader with some follower replica in this node
func (n *Node) nodeConnectionHandler(msg Message) Response {
	switch msg.Type {
	case Set:
		err := n.set(msg.PartitionId, msg.Timestamp, msg.Key, msg.Value, replica.Follower)
		if err != nil {
			log.Printf("[node.nodeConnectionHandler] failed to set key '%s' in partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{}
	case Delete:
		err := n.delete(msg.PartitionId, msg.Timestamp, msg.Key, replica.Follower)
		if err != nil {
			log.Printf("[node.nodeConnectionHandler] failed to delete key '%s' from partition %d: %v", msg.Key, msg.PartitionId, err)
			return Response{Error: err}
		}
		return Response{}
	case Snapshot:
		r, exists := n.replicas[msg.PartitionId]
		if !exists {
			n.replicasMapMutex.Lock()
			r = replica.NewReplica(n.Id, msg.PartitionId, replica.Follower)
			n.replicas[msg.PartitionId] = r
			n.replicasMapMutex.Unlock()
		}
		r.ReceiveSnapshot(&msg.Snapshot)
		log.Printf("[nodeConnectionHandler] Received snapshot for partition %d", msg.PartitionId)
		return Response{}
	default:
		return Response{Error: fmt.Errorf("unknown message type")}
	}
}

func (n *Node) set(partitionId int, timestamp int64, key string, value string, replicaType replica.ReplicaType) error {
	n.replicasMapMutex.RLock()
	r, ok := n.replicas[partitionId]
	n.replicasMapMutex.RUnlock()
	if !ok {
		return fmt.Errorf("[node.set] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode != replicaType {
		return fmt.Errorf("[node.set] node id: %v contains no %v replica for partition %v", n.Id, replicaType, partitionId)
	}

	replicaLog, err := r.Set(key, value, timestamp)
	if err != nil {
		return fmt.Errorf("[node.set] failed to set(%v, %v) to partitionId: %v in nodeId: %v | err: %v", key, value, partitionId, n.Id, err)
	}

	if replicaType == replica.Leader {
		go n.broadcastToFollowers(replicaLog)
	}
	return nil
}

func (n *Node) get(partitionId int, key string) (string, error) {
	n.replicasMapMutex.RLock()
	r, ok := n.replicas[partitionId]
	n.replicasMapMutex.RUnlock()
	if !ok {
		return "", fmt.Errorf("[node.get] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	replicaLog, err := r.Get(key)
	if err != nil {
		return "", fmt.Errorf("[node.get] failed to get(%v) to partitionId: %v in nodeId: %v | err: %v", key, partitionId, n.Id, err)
	}

	return replicaLog.Value, nil
}

func (n *Node) delete(partitionId int, timestamp int64, key string, replicaType replica.ReplicaType) error {
	n.replicasMapMutex.RLock()
	r, ok := n.replicas[partitionId]
	n.replicasMapMutex.RUnlock()
	if !ok {
		return fmt.Errorf("[node.delete] node id: %v contains no partition containing key:%s", n.Id, key)
	}

	if r.Mode != replicaType {
		return fmt.Errorf("[node.delete] node id: %v contains no %v replica for partition %v", n.Id, replicaType, partitionId)
	}

	replicaLog, err := r.Delete(key, timestamp)
	if err != nil {
		return fmt.Errorf("[node.delete] failed to delete(%v) from partitionId: %v in nodeId: %v | err: %v", key, partitionId, n.Id, err)
	}

	if replicaType == replica.Leader {
		go n.broadcastToFollowers(replicaLog)
	}
	return nil
}

// This function broadcase set/delete requests to all follower replicas
func (n *Node) broadcastToFollowers(replicaLog replica.ReplicaLog) {
	msg := Message{
		PartitionId: replicaLog.PartitionId,
		Timestamp:   replicaLog.Timestamp,
		Key:         replicaLog.Key,
		Value:       replicaLog.Value,
	}

	if replicaLog.Action == replica.ReplicaActionSet {
		msg.Type = Set
	} else if replicaLog.Action == replica.ReplicaActionDelete {
		msg.Type = Delete
	}

	followersNodesAddresses, err := n.getNodesContainingPartition(replicaLog.PartitionId)
	if err != nil {
		log.Printf("[node.broadcastToFollowers] failed get nodes containing partitionId: %d from controller: %v", replicaLog.PartitionId, err)
	}

	for _, address := range followersNodesAddresses {
		go n.replicateToFollower(address, msg)
	}
}

func (n *Node) replicateToFollower(address string, msg Message) error {
	maxRetries := 3
	retryDelay := 100 * time.Millisecond
	for i := 0; i < maxRetries; i++ {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Printf("[node.replicateToFollower] failed to connect to %s: %v", address, err)
			time.Sleep(retryDelay)
			continue
		}

		encoder := gob.NewEncoder(conn)
		decoder := gob.NewDecoder(conn)

		if err := encoder.Encode(msg); err != nil {
			log.Printf("[node.replicateToFollower] failed to send message: %v", err)
			conn.Close()
			time.Sleep(retryDelay)
			continue
		}

		var resp Response
		if err := decoder.Decode(&resp); err != nil {
			log.Printf("[node.replicateToFollower] failed to decode response: %v", err)
			conn.Close()
			time.Sleep(retryDelay)
			continue
		}

		conn.Close()

		if resp.Error != nil {
			log.Printf("[node.replicateToFollower] follower at %s responded with error: %s", address, resp.Error)
			time.Sleep(retryDelay)
			continue
		}

		log.Printf("[node.replicateToFollower] successfully replicated to %s", address)
		return nil
	}

	log.Printf("[node.replicateToFollower] failed to replicate to follower at %s after %d retries", address, maxRetries)
	return fmt.Errorf("failed to replicate to follower at %s after %d retries", address, maxRetries)
}

func (n *Node) registerAndMaintainLease(ctx context.Context) {
	httpAddr := os.Getenv("HTTP_ADDRESS")
	if httpAddr == "" {
		httpAddr = HTTP_ADDRESS
	}
	tcpAddr := os.Getenv("TCP_ADDRESS")
	if tcpAddr == "" {
		tcpAddr = TCP_ADDRESS
	}

	nodeInfo := struct {
		ID          int
		HttpAddress string
		TcpAddress  string
	}{
		ID:          n.Id,
		HttpAddress: fmt.Sprintf("http://%s", httpAddr),
		TcpAddress:  tcpAddr,
	}

	nodeInfoJSON, err := json.Marshal(nodeInfo)
	if err != nil {
		log.Fatalf("Failed to marshal node info: %v", err)
	}

	for {
		lease, err := n.etcdClient.Grant(ctx, 10)
		if err != nil {
			log.Printf("Failed to grant lease: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		_, err = n.etcdClient.Put(ctx, "/nodes/"+strconv.Itoa(n.Id), string(nodeInfoJSON), clientv3.WithLease(lease.ID))
		if err != nil {
			log.Printf("Failed to register node: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Node %d registered with lease %v", n.Id, lease.ID)

		keepAlive, err := n.etcdClient.KeepAlive(ctx, lease.ID)
		if err != nil {
			log.Printf("Failed to keep lease alive: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Block until lease expires or context is canceled
		<-keepAlive
		log.Printf("Node %d lease expired", n.Id)
	}
}

func (n *Node) sendSnapshotToNode(partitionId int, address string) error {
	n.replicasMapMutex.RLock()
	replica, exists := n.replicas[partitionId]
	if !exists {
		return fmt.Errorf("[node.sendSnapshotToNode] partition %d not found", partitionId)
	}

	snapshot := replica.GetSnapshot()
	if snapshot == nil {
		return fmt.Errorf("[node.sendSnapshotToNode] failed to get snapshot for partition %d", partitionId)
	}
	n.replicasMapMutex.RUnlock()

	msg := Message{
		Type:        Snapshot,
		PartitionId: replica.PartitionId,
		Snapshot:    *snapshot,
	}

	err := n.replicateToFollower(address, msg)

	if err != nil {
		return err
	}
	log.Printf("[node.SendSnapshotToNode] successfully sent snapshot of partition %d to %s", partitionId, address)
	return nil
}
