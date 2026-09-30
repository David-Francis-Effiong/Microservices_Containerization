import hashlib
import bisect
import collections

class LRUCache:
    """
    A simple Least Recently Used (LRU) Cache implementation using OrderedDict.
    This acts as our caching layer to improve read performance and reduce load on storage nodes.
    """
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def get(self, key):
        """Retrieve an item from cache. If found, mark it as most recently used."""
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        """Add an item to cache. If capacity is exceeded, evict the least recently used item."""
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
            
    def remove(self, key):
        """Explicitly remove an item from cache."""
        if key in self.cache:
            del self.cache[key]

class StorageNode:
    """
    Represents a single physical or virtual storage node in our distributed system.
    Holds a portion of the total key-value data.
    """
    def __init__(self, name):
        self.name = name
        self.data = {}
        # is_active flag allows us to simulate node crashes/failures easily.
        self.is_active = True

    def put(self, key, value):
        """Store data in this node. Raises exception if the node is simulated as failed."""
        if not self.is_active:
            raise Exception(f"Node {self.name} is down!")
        self.data[key] = value

    def get(self, key):
        """Retrieve data from this node. Raises exception if the node is simulated as failed."""
        if not self.is_active:
            raise Exception(f"Node {self.name} is down!")
        return self.data.get(key)
        
    def remove(self, key):
        """Remove data from this node. Raises exception if the node is simulated as failed."""
        if not self.is_active:
            raise Exception(f"Node {self.name} is down!")
        if key in self.data:
            del self.data[key]

class ConsistentHashRing:
    """
    Implements Consistent Hashing to dynamically assign keys to storage nodes.
    Uses virtual nodes (replicas) to ensure even data distribution across the ring.
    """
    def __init__(self, num_replicas=3):
        self.num_replicas = num_replicas
        self.ring = {} # Maps hash values to node names
        self.sorted_keys = [] # Keeps hash values sorted for binary search
        self.nodes = {} # Maps node names to StorageNode instances

    def _hash(self, key):
        """Helper to generate a consistent integer hash for a given string key using MD5."""
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return int(m.hexdigest(), 16)

    def add_node(self, node: StorageNode):
        """Add a new storage node to the ring, creating multiple virtual nodes for it."""
        self.nodes[node.name] = node
        for i in range(self.num_replicas):
            vnode_key = f"{node.name}:{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node.name
            bisect.insort(self.sorted_keys, h) # Keep the ring sorted
            
    def remove_node(self, node_name):
        """Remove a storage node and all its virtual nodes from the ring."""
        if node_name in self.nodes:
            del self.nodes[node_name]
            for i in range(self.num_replicas):
                vnode_key = f"{node_name}:{i}"
                h = self._hash(vnode_key)
                if h in self.ring:
                    del self.ring[h]
                    self.sorted_keys.remove(h)

    def get_node(self, key):
        """
        Find the appropriate storage node for a given key.
        Searches clockwise on the ring for the next available node.
        """
        if not self.ring:
            return None
        h = self._hash(key)
        # Find the first node on the ring with a hash >= the key's hash
        idx = bisect.bisect(self.sorted_keys, h)
        # If we reach the end of the ring, wrap around to the first node
        if idx == len(self.sorted_keys):
            idx = 0
        node_name = self.ring[self.sorted_keys[idx]]
        return self.nodes[node_name]
        
    def get_all_nodes(self):
        """Return all physical nodes registered in the ring."""
        return self.nodes.values()

class DistributedKVStore:
    """
    The facade for our distributed system. It hides the complexity of consistent hashing,
    nodes, routing, and caching from the end-user (transparency).
    """
    def __init__(self, cache_capacity=3):
        self.ring = ConsistentHashRing(num_replicas=3)
        self.cache = LRUCache(capacity=cache_capacity)
        
    def add_node(self, node_name):
        """Scale out the system by adding a node. Only minimal data needs to be moved."""
        new_node = StorageNode(node_name)
        old_nodes = list(self.ring.get_all_nodes())
        self.ring.add_node(new_node)
        
        print(f"\n[System] Node '{node_name}' joined. Rebalancing data...")
        keys_moved = 0
        
        # Re-evaluate the placement of all keys in active nodes
        # In a real system, we would only check data on nodes adjacent to the new node on the ring.
        for node in old_nodes:
            if not node.is_active: continue
            keys_to_move = []
            for k, v in node.data.items():
                target_node = self.ring.get_node(k)
                if target_node.name != node.name:
                    keys_to_move.append((k, v, target_node))
            
            # Perform the minimal data movement
            for k, v, target in keys_to_move:
                target.put(k, v)
                node.remove(k)
                keys_moved += 1
        print(f"[System] Rebalancing complete. {keys_moved} keys moved.")

    def remove_node(self, node_name):
        """Gracefully remove a node from the system and redistribute its data."""
        node_to_remove = self.ring.nodes.get(node_name)
        if not node_to_remove: return
        
        print(f"\n[System] Node '{node_name}' leaving gracefully. Redistributing data...")
        # Save a copy of the departing node's data
        data_to_redistribute = dict(node_to_remove.data)
        # Remove it from the hash ring so it no longer receives traffic
        self.ring.remove_node(node_name)
        
        # Push its data to the next available nodes in the ring
        keys_moved = 0
        for k, v in data_to_redistribute.items():
            target = self.ring.get_node(k)
            if target:
                target.put(k, v)
                keys_moved += 1
        print(f"[System] Redistribution complete. {keys_moved} keys moved.")

    def simulate_node_failure(self, node_name):
        """Force a node failure to test system availability."""
        node = self.ring.nodes.get(node_name)
        if node:
            node.is_active = False
            print(f"\n[System] CRITICAL: Node '{node_name}' has crashed!")

    def put(self, key, value):
        """
        Client interface to store data.
        Transparently routes the request to the correct node and updates the cache.
        """
        node = self.ring.get_node(key)
        if not node:
            print("[Error] No nodes available.")
            return

        try:
            node.put(key, value)
            self.cache.put(key, value)
            print(f"-> PUT: '{key}' saved to node '{node.name}'")
        except Exception as e:
            # Handle node failure dynamically (Limited Availability/Fault Tolerance)
            print(f"-> PUT ERROR: Failed to write '{key}' to node '{node.name}': {e}")
            self._handle_node_failure(key, value)

    def _handle_node_failure(self, key, value=None, is_put=True):
        """
        Fallback mechanism. If the primary node for a key is down, it traverses 
        the hash ring to find the next active node to handle the request.
        """
        print(f"-> [Fallback] Finding next available node in ring for '{key}'...")
        h = self.ring._hash(key)
        idx = bisect.bisect(self.ring.sorted_keys, h)
        start_idx = idx
        
        while True:
            if idx == len(self.ring.sorted_keys):
                idx = 0
            
            node_name = self.ring.ring[self.ring.sorted_keys[idx]]
            node = self.ring.nodes[node_name]
            
            if node.is_active:
                if is_put:
                    node.put(key, value)
                    self.cache.put(key, value)
                    print(f"-> [Fallback PUT]: '{key}' saved to alternate node '{node.name}'")
                    return
                else:
                    val = node.get(key)
                    if val:
                        self.cache.put(key, val) # Repopulate cache
                    return val
                    
            idx = (idx + 1) % len(self.ring.sorted_keys)
            # Stop if we've checked the entire ring and no nodes are active
            if idx == start_idx:
                print("-> [System Error] All nodes are down!")
                return None

    def get(self, key):
        """
        Client interface to retrieve data.
        Checks the high-speed cache first. If it's a cache miss, transparently retrieves it 
        from the appropriate storage node.
        """
        # 1. Check Cache first
        cached_val = self.cache.get(key)
        if cached_val:
            print(f"<- GET: '{key}' found in CACHE -> {cached_val}")
            return cached_val

        # 2. Not in cache, check Storage Node
        node = self.ring.get_node(key)
        if not node:
            print("[Error] No nodes available.")
            return None

        try:
            val = node.get(key)
            if val:
                self.cache.put(key, val) # Populate cache on read
                print(f"<- GET: '{key}' retrieved from node '{node.name}' -> {val}")
            else:
                print(f"<- GET: '{key}' not found in node '{node.name}'")
            return val
        except Exception as e:
            # Handle node failure gracefully
            print(f"<- GET ERROR: Failed to read '{key}' from node '{node.name}': {e}")
            val = self._handle_node_failure(key, is_put=False)
            if val:
                print(f"<- [Fallback GET]: '{key}' retrieved from alternate node -> {val}")
            return val
            
    def print_distribution(self):
        """Utility function to visualize which nodes hold which data."""
        print("\n--- Current Data Distribution ---")
        for name, node in self.ring.nodes.items():
            status = "ACTIVE" if node.is_active else "FAILED"
            print(f"Node '{name}' ({status}): {node.data}")
        print("---------------------------------")

def run_simulation():
    """Executes the simulation to demonstrate the system's capabilities."""
    
    # Initialize the system. Capacity is set to 2 to easily demonstrate LRU cache eviction.
    store = DistributedKVStore(cache_capacity=2)
    
    # 1. Start with a 3-node cluster
    print("=== 1. Initialize Cluster ===")
    store.add_node("Node-A")
    store.add_node("Node-B")
    store.add_node("Node-C")
    
    # 2. Add Experimental Data
    print("\n=== 2. Inserting Experimental Data ===")
    data = {
        "user:101": {"name": "Alice"},
        "user:102": {"name": "Bob"},
        "user:103": {"name": "Charlie"},
        "user:104": {"name": "Diana"},
        "user:105": {"name": "Eve"},
        "user:106": {"name": "Frank"}
    }
    for k, v in data.items():
        store.put(k, v)
        
    store.print_distribution()
    
    # 3. Test LRU Cache
    print("\n=== 3. Testing Cache Layer ===")
    store.get("user:101") # Cache Miss -> Reads from node, loads into cache
    store.get("user:101") # Cache Hit -> Fast retrieval from cache
    store.get("user:102") # Cache Miss -> Reads from node, loads into cache
    store.get("user:103") # Cache Miss -> Load into cache, evicts user:101 (since capacity=2)
    store.get("user:101") # Cache Miss -> Was evicted, reads from node again
    
    # 4. Node Join & Consistent Hashing (Minimal Data Movement)
    print("\n=== 4. Node Join (Scaling Out) ===")
    store.add_node("Node-D")
    store.print_distribution()
    
    # 5. Node Failure & System Availability
    print("\n=== 5. Simulating Node Failure ===")
    # Intentionally fail the node holding user:102 to test fault tolerance
    target_node = store.ring.get_node("user:102").name
    store.simulate_node_failure(target_node)
    
    # Remove from cache first to ensure it hits the failed node during the test
    store.cache.remove("user:102") 
    print(f"\nAttempting to read user:102 (was on {target_node})")
    # Will fail and trigger the fallback mechanism to search for the next available node
    store.get("user:102")
    
    print("\nAttempting to write a new key to the failed node's hash space...")
    store.put("user:102", {"name": "Bob (Recovered)"})
    store.get("user:102")
    
    store.print_distribution()
    
    # 6. Graceful Node Departure
    print("\n=== 6. Graceful Node Departure (Node-A) ===")
    # Node-A leaves, its data is safely redistributed to remaining active nodes
    store.remove_node("Node-A")
    store.print_distribution()

if __name__ == "__main__":
    run_simulation()
