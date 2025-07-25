# CT_HW3
Computer Technology Course - Homework 3

---

# Distributed In-Memory Database

A distributed in-memory key-value store written in Go. The system follows a single-leader architecture and is designed to be scalable, modular, and easy to deploy using Docker.

## Features

* In-memory key-value storage with distributed architecture.
* Single-leader replication model.
* A central controller manages node and partition assignment.
* Key partitioning is determined by `Hash(key) % number_of_partitions`.
* Leader replicas use LSM trees to persist changes and initialize new replicas.
* Load balancers use the partition and node information to route requests.
* Clients can send requests and manage the cluster by adding or removing nodes and partitions.
* Dockerized deployment.

## Project Structure

* **Controller**: Assigns keys to partitions and manages the overall cluster configuration.
* **Nodes (Replicas)**: Store and manage data for specific partitions; leaders persist logs using LSM trees.
* **Load Balancer**: Uses metadata to forward requests to the appropriate nodes.
* **Client**: Interfaces with the system to send key-value operations and manage nodes/partitions.

## Running with Docker
Run in project's root:
```
docker compose --profile "*" build
docker compose up
```

## Contributors

* [Mohammad Barkatin](https://github.com/mammedbrk)
* [Mehdi Taheri](https://github.com/Mefi22)
* [Parisa Sadat Mousavi](https://github.com/parisam83)
