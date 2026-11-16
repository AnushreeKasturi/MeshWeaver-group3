
## Week 1 – DHT Foundation

Completed the initial implementation of the Distributed Hash Table (DHT).

### Tasks Completed

- Implemented 160-bit Node ID generation using SHA-1.
- Implemented XOR distance calculation between node IDs.
- Implemented bucket index calculation.
- Created K-Buckets with a maximum capacity of 20 peers.
- Implemented peer addition and ordering.
- Implemented peer eviction when buckets are full.
- Created the Routing Table with 160 buckets.
- Executed test cases successfully.

### Week 1 Status

All Week 1 implementation tasks and test cases have been completed successfully.

## Week 1 - DHT Foundation (Member C)

### Implemented Features

1. Generated unique 160-bit Node IDs using SHA-1 hashing.
2. Implemented XOR distance calculation between nodes.
3. Created K-Buckets with a maximum capacity of 20 peers.
4. Implemented peer addition, ordering, and eviction.
5. Developed a Routing Table containing 160 K-Buckets.

### Testing

Tested the Node ID generation, XOR distance,
K-Bucket operations, and Routing Table functionality.

All implemented Week 1 test cases passed successfully.

### Technologies Used
- Python
- SHA-1
- Pytest
- VS Code
  ## Running Tests

MeshWeaver uses pytest for testing.

### Run Node ID Tests

```bash
python -m pytest dht/testcase_node_id.py -v
# DHT Module - MeshWeaver

## Overview

The DHT (Distributed Hash Table) module manages node identification,
peer distance calculation, and routing information.

## Files

### 1. node_id.py
- Generates 160-bit node IDs using SHA-1.
- Calculates XOR distance between two node IDs.
- Determines the appropriate K-Bucket index.

