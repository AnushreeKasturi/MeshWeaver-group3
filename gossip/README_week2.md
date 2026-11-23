# Week 2 – Gossip Protocol

## Overview
Week 2 focuses on implementing Gossip broadcasting and receiving functionality in the MeshWeaver distributed system.

## Files and Description

- **peer_register.py** – Selects the nearest connected peers using XOR distance.
- **peer_store.py** – Stores peer load information and manages fresh and stale peer records.
- **broadcast.py** – Implements the GossipModule to broadcast and receive CPU and RAM load information.

## Key Features

- Selects the nearest neighbours using XOR distance.
- Broadcasts load reports to configured neighbours.
- Receives and stores peer load information.
- Validates incoming gossip messages.
- Detects and removes stale peer records.
- Integrates with the shared MeshWeaver node runtime.

## Configuration

- `gossip_interval` – Time between gossip broadcasts.
- `gossip_fanout` – Number of neighbours to contact.
- `stale_after` – Timeout for peer load information.

## Testing

Week 2 tests cover:
- Nearest-neighbour selection.
- Peer load storage and validation.
- Stale peer detection and removal.
- Gossip message receiving.

Run tests using:

```bash
py -m pytest tests/gossip/ -v
```

## Conclusion

Week 2 implements peer selection, load information storage, and Gossip message broadcasting and receiving for communication between MeshWeaver nodes.
