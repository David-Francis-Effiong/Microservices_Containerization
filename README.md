# Distributed Key-Value Store Simulation

This repository contains a Python simulation of a basic distributed key-value storage system. The project demonstrates core distributed system concepts such as scalability, elasticity, fault tolerance, and transparency.

## Features

1. **Consistent Hashing**: Dynamically assigns keys to nodes using a hash ring (with virtual nodes to ensure even distribution).
2. **Minimal Data Movement**: Simulates elastic scaling (node join/leave) and observes how consistent hashing prevents total data rehashing.
3. **Caching Layer**: Includes a custom Least Recently Used (LRU) cache to improve read performance and demonstrate memory eviction policies.
4. **Fault Tolerance**: Simulates sudden node failures and showcases how the system gracefully falls back to available nodes to provide limited availability.
5. **Transparency**: Employs the Facade pattern (`DistributedKVStore`) to completely hide the node-level routing, hashing, and caching complexity from the end-user.

## Experimental Data Used
```json
user:101 -> {"name": "Alice"}
user:102 -> {"name": "Bob"}
user:103 -> {"name": "Charlie"}
user:104 -> {"name": "Diana"}
user:105 -> {"name": "Eve"}
user:106 -> {"name": "Frank"}
```

## How to Run

Ensure you have Python installed. The project relies on built-in standard libraries (`hashlib`, `bisect`, `collections`).

```bash
python distributed_kv_store.py
```

The console output will visually walk you through:
1. Cluster Initialization
2. Data Insertion & Distribution
3. Cache Hits and Evictions
4. Adding a Node (Scale Out)
5. Simulating a Node Crash and Failover
6. Graceful Node Departure
