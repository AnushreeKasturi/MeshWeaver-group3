# MeshWeaver

A pure-Python, serverless distributed task queue. Nodes discover each other
through a Kademlia DHT, share CPU/RAM load by gossip, and route each task to the
least-loaded peer, re-routing it if that peer dies.

See [docs/architecture.md](docs/architecture.md) for the design and the shared
message protocol.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows  (source .venv/bin/activate on Linux/macOS)
pip install -r requirements.txt
python -m pytest
```

Python 3.10+ is required.

## Layout

| Path | Owner | Purpose |
|---|---|---|
| `meshweaver/protocol.py` | A | Shared message format and dispatcher |
| `meshweaver/node.py`, `config.py`, `cli.py`, `dashboard.py` | A | Runtime, CLI, live view |
| `meshweaver/net/` | B | UDP/TCP transport, TLS |
| `meshweaver/dht/` | C | Node IDs, k-buckets, lookups, bootstrap |
| `meshweaver/gossip/`, `meshweaver/scheduler/` | D | Load gossip and task routing |
| `meshweaver/tasks/` | E | Serialization, execution, heartbeats |
| `tests/unit/`, `tests/integration/` | All, A | Tests |
| `scripts/launch_mesh.py` | A | 10-node local launcher |

## Contributing

* Push only to your own branch: `member-a` … `member-e`. The Team Lead merges into `main`.
* Put your code inside your module folder under `meshweaver/` and your tests in
  `tests/unit/test_<module>_*.py`.
* Import message classes from `meshweaver.protocol`; do not define a separate wire format.
* Commit small working pieces daily with clear messages, e.g. `dht: add FIND_NODE iterative lookup`.

## Status

| Week | A (lead) |
|---|---|
| 1 | Architecture doc, `protocol.py` message format and dispatcher, repo skeleton ✅ |
| 2 | Node runtime, config, 10-node launcher, structured logging |
| 3 | End-to-end task path and integration tests |
| 4 | CLI, dashboard, packaging, report |
