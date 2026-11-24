# MeshWeaver – Distributed Hash Table (DHT)

**Project:** MeshWeaver  
**Module:** Distributed Hash Table (DHT)  
**Team Role:** Member C  
**Implementation:** Week 1  
**Status:** Completed

---

## 1. Project Overview

MeshWeaver is a distributed system project. As Member C, my responsibility is to develop the Distributed Hash Table (DHT) module.

The DHT module is responsible for generating node identifiers, calculating distances between nodes, managing peers, and maintaining routing information.

During Week 1, I implemented the fundamental DHT components using Python, including SHA-1-based node ID generation, XOR distance calculation, K-Buckets, and the routing table.

## 2. Week 1 Objectives

The main objectives of Week 1 were:

- Generate 160-bit node IDs using SHA-1 hashing.
- Implement XOR distance calculation between nodes.
- Develop K-Buckets with a maximum capacity of K = 20.
- Implement peer addition and ordering.
- Handle peer eviction when a bucket reaches capacity.
- Build a routing table containing 160 K-Buckets.
- Write and execute test cases to verify the implementation.

## 3. Project Folder Structure

```text
meshweaver/
│
├── .gitignore
├── README.md
│
├── dht/
│   ├── __init__.py
│   ├── bucket.py
│   ├── node_id.py
│   ├── routing_table.py
│   ├── README.md
│   │
│   ├── testcase_node_id.py
│   ├── testcase_bucket.py
│   ├── testcase_routing_table.py
│   │
│   ├── __pycache__/
│   └── .pytest_cache/
│
├── .pytest_cache/
│
└── venv/
```

**Note:** `__pycache__`, `.pytest_cache`, and `venv` are generated or local environment folders, not application source files.

## 4. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| SHA-1 | 160-bit node ID generation |
| XOR | Distance calculation |
| OrderedDict | Maintaining peer ordering |
| Pytest | Test case execution |
| VS Code | Development environment |

## 5. Implementation Details

### 5.1 Node ID Generation – `node_id.py`

Implemented node ID generation using the SHA-1 hashing algorithm.

**Features:**
- Generates 160-bit node identifiers.
- Converts input values into SHA-1 hash values.
- Generates consistent IDs for identical inputs.
- Calculates XOR distance between two node IDs.
- Determines the corresponding K-Bucket index.

### 5.2 K-Bucket Implementation – `bucket.py`

Implemented K-Bucket functionality to manage peer information.

**Features:**
- Maximum capacity of 20 peers.
- Adds new peers.
- Maintains peer ordering.
- Handles duplicate peers.
- Removes existing peers.
- Handles peer eviction when the bucket is full.

### 5.3 Routing Table – `routing_table.py`

Implemented the routing table using 160 K-Buckets.

**Features:**
- Maintains 160 individual buckets.
- Organizes peers based on XOR distance.
- Supports peer addition and removal.
- Retrieves peers from buckets.
- Maintains the peer count.

## 6. Test Cases

Three test files were created inside the `dht` folder.

| Test File | Testing Purpose |
|---|---|
| `testcase_node_id.py` | Node ID generation, XOR distance, and bucket index |
| `testcase_bucket.py` | Peer addition, ordering, removal, and eviction |
| `testcase_routing_table.py` | Routing table creation and peer management |

### Running the Tests

Open the VS Code terminal from the main project folder.

Activate the virtual environment:

```powershell
.\venv\Scripts\Activate.ps1
```

Run Node ID tests:

```powershell
python -m pytest dht/testcase_node_id.py -v
```

Run K-Bucket tests:

```powershell
python -m pytest dht/testcase_bucket.py -v
```

Run Routing Table tests:

```powershell
python -m pytest dht/testcase_routing_table.py -v
```

Run all three test files together:

```powershell
python -m pytest dht/testcase_node_id.py dht/testcase_bucket.py dht/testcase_routing_table.py -v
```

**Testing Result:** All implemented Week 1 test cases were executed successfully.

## 7. Week 1 Deliverables

| Task | Status |
|---|---|
| SHA-1-based 160-bit Node ID | Completed |
| XOR distance calculation | Completed |
| K-Bucket implementation (K = 20) | Completed |
| Peer addition and ordering | Completed |
| Peer eviction | Completed |
| Routing Table with 160 buckets | Completed |
| Node ID test cases | Passed |
| K-Bucket test cases | Passed |
| Routing Table test cases | Passed |

## 8. Challenges and Learning

During Week 1, I gained practical experience in:

- Understanding SHA-1 hashing.
- Working with 160-bit node identifiers.
- Understanding XOR distance in distributed systems.
- Managing peers using OrderedDict.
- Implementing K-Bucket capacity management.
- Organizing Python modules and packages.
- Running test cases using Pytest.
- Debugging Python imports and execution errors.

## 9. Week 1 Outcome

Successfully implemented the foundational components of the MeshWeaver DHT module.

The Node ID generation, XOR distance calculation, K-Bucket management, and Routing Table functionalities were implemented and tested.

All implemented Week 1 test cases passed successfully.

## 10. Conclusion

Week 1 established the foundation for the Distributed Hash Table module of MeshWeaver.

The completed implementation provides the basic node identification, peer management, and routing functionalities required for the next development phase.

**Project:** MeshWeaver  
**Role:** Member C – DHT  
**Week:** 1  
**Status:** Completed
