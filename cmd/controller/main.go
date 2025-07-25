package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
)

func main() {
	etcdEndpoints := os.Getenv("ETCD_ENDPOINTS")
	if etcdEndpoints == "" {
		etcdEndpoints = "localhost:2379"
	}

	etcdClient, err := controller.NewEtcdClient(strings.Split(etcdEndpoints, ","))
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}
	controllerAddr := hostname + ":8080"

	partitionCountStr := os.Getenv("PARTITION_COUNT")
	if partitionCountStr == "" {
		partitionCountStr = "4"
	}
	partitionCount, err := strconv.Atoi(partitionCountStr)
	if err != nil {
		log.Fatalf("Failed to parse PARTITION_COUNT: %v", err)
	}

	replicationFactorStr := os.Getenv("REPLICATION_FACTOR")
	if replicationFactorStr == "" {
		replicationFactorStr = "3"
	}
	replicationFactor, err := strconv.Atoi(replicationFactorStr)
	if err != nil {
		log.Fatalf("Failed to parse REPLICATION_FACTOR: %v", err)
	}

	c := controller.NewController(partitionCount, replicationFactor, "", "", etcdClient)
	c.Start(":8080", controllerAddr)
}
