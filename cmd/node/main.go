package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/node"
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

	id, err := strconv.Atoi(os.Getenv("NODE_ID"))
	if err != nil {
		log.Fatalf("[main (node)] failed to get 'NODE_ID' env variable: %v", err)
	}

	n := node.NewNode(id, etcdClient)
	n.Start()
}
