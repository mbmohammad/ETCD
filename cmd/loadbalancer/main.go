package main

import (
	"log"
	"os"
	"strings"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/loadbalancer"
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

	lb := loadbalancer.NewLoadBalancer(etcdClient)

	if err := lb.Run(":9001"); err != nil {
		log.Fatalf("Failed to start load balancer: %v", err)
	}
}
