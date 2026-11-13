# MeshWeaver

**A pure-Python, serverless distributed task queue.**

MeshWeaver has no central server. Every machine runs the same node program.
Nodes find each other through a **Kademlia DHT**, share their CPU and RAM load
by **gossip**, and send each task to the **least-loaded peer**. If that peer
dies, its missing **heartbeats** trigger a re-route, so the task still finishes.

> Infotact Solutions internship · Group 3 · Project 1 (Month 1) · 5 Nov – 4 Dec 2026

---

## Contents

- [How it works](#how-it-works)
- [Team and module ownership](#team-and-module-ownership)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [The shared protocol](#the-shared-protocol)
- [Timeline and week-wise plan](#timeline-and-week-wise-plan)
- [Reviews](#reviews)
- [GitHub rules](#github-rules)
- [Progress](#progress)

---

## How it works

A task passes through every module:

1. **Tasks (E)** packages a Python function and its arguments with `cloudpickle`.
2. **Scheduler (D)** picks the target node from the gossip load table, skipping
   busy and stale peers, and checks with an offer/accept handshake.
3. **Transport (B)** carries the task over UDP/TCP (TLS in Week 4) to a peer
   that the **DHT (C)** discovered.
4. **Tasks (E)** runs it in a process pool on the remote node and sends
   heartbeats every 2 s, then the result.
5. **Node runtime and dashboard (A)** wire all of this together on one asyncio
   event loop and show peers, load and task states live.

All modules talk through one shared message format and dispatcher in
`meshweaver/protocol.py`. Full design: **[docs/architecture.md](docs/architecture.md)**.

## Team and module ownership

| Member | Role | Code owned | Branch |
|---|---|---|---|
| **A**: Sai Anushree Kasturi (Team Lead) | System architect: protocol, node runtime, integration, CLI, dashboard | `protocol.py`, `node.py`, `config.py`, `cli.py`, `dashboard.py`, `scripts/`, `tests/integration/` | `member-a` |
| **B** | Networking core and TLS security | `meshweaver/net/` | `member-b` |
| **C** | Kademlia DHT and peer discovery | `meshweaver/dht/` | `member--c` |
| **D** | Gossip protocol and task routing | `meshweaver/gossip/`, `meshweaver/scheduler/` | `member---d` |
| **E** | Task serialization, execution and fault tolerance | `meshweaver/tasks/` | `member--e` |

## Repository layout

```
MeshWeaver-group3/
├── meshweaver/
│   ├── protocol.py      # A: shared Message format + Dispatcher
│   ├── node.py          # A: node runtime (Week 2)
│   ├── config.py        # A: ports, intervals, timeouts (Week 2)
│   ├── cli.py           # A: `meshweaver start` / `submit` (Week 4)
│   ├── dashboard.py     # A: live rich dashboard (Week 4)
│   ├── net/             # B: UDP/TCP transport, TLS
│   ├── dht/             # C: node IDs, k-buckets, lookups, bootstrap
│   ├── gossip/          # D: load collector and gossip
│   ├── scheduler/       # D: least-loaded routing
│   └── tasks/           # E: serializer, executor, heartbeats
├── tests/
│   ├── unit/            # one test file per module
│   └── integration/     # A: end-to-end task path
├── scripts/
│   └── launch_mesh.py   # A: start 10 nodes on one machine
├── docs/
│   └── architecture.md
├── requirements.txt
└── pytest.ini
```

## Getting started

Requires **Python 3.10+**.

```bash
git clone https://github.com/AnushreeKasturi/MeshWeaver-group3.git
cd MeshWeaver-group3
python -m venv .venv
.venv\Scripts\activate            # Windows
# source .venv/bin/activate       # Linux / macOS
pip install -r requirements.txt
python -m pytest                  # run all tests
```

Starting nodes (`python scripts/launch_mesh.py`) and the `meshweaver` CLI
arrive in Weeks 2 and 4.

## The shared protocol

Every message is a `Message(type, msg_id, sender_id, payload)` from
`meshweaver/protocol.py`, sent as JSON. Each message type has a payload
dataclass, and the `Dispatcher` sends each incoming message to the module that
registered for it.

```python
from meshweaver.protocol import Dispatcher, LoadReport, Message, MessageType

# Sending
msg = Message.create(MessageType.GOSSIP_LOAD, my_id, LoadReport(my_id, 23.5, 4.1))
transport.send(peer_addr, msg.encode())

# Receiving: each module registers its handlers once at startup
dispatcher = Dispatcher(my_id)

@dispatcher.on(MessageType.GOSSIP_LOAD)
async def on_load(msg, addr):
    load_table.update(msg.parsed())      # -> LoadReport
```

| Module | Message types it handles |
|---|---|
| net (B) | `PING`, `PONG` |
| dht (C) | `FIND_NODE`, `FIND_VALUE`, `STORE` (+ replies) |
| gossip (D) | `GOSSIP_LOAD` |
| scheduler (D) | `TASK_OFFER` (+ reply) |
| tasks (E) | `TASK_SUBMIT`, `TASK_RESULT`, `HEARTBEAT` |

**Rule for every pull request:** import message classes from
`meshweaver.protocol`. Do not define a separate wire format.

## Timeline and week-wise plan

| Week | Dates | A: architecture and integration | B: networking | C: DHT | D: gossip and routing | E: tasks |
|---|---|---|---|---|---|---|
| **1** | 5–11 Nov | Architecture doc, `protocol.py` message format and dispatcher, repo, branches, skeleton | UDP ping/pong with `asyncio.DatagramProtocol`, request/response matching by `msg_id` with timeouts | SHA-1 160-bit IDs, XOR distance, k-buckets (k = 20) with tests | CPU/RAM load collector, gossip message format | `cloudpickle` a function, send it, run it remotely, return the result |
| **2** | 12–18 Nov | `node.py` runtime on one event loop, `config.py`, `launch_mesh.py` (10 nodes), structured logging, merge all branches | TCP channel for large payloads, retries, max size, `send_rpc()` | `PING`/`STORE`/`FIND_NODE`/`FIND_VALUE`, iterative lookup (α = 3), broadcast bootstrap | Gossip to 3 nearest every 5 s, stale after 15 s | Process pool via `run_in_executor`, timeouts, Monte Carlo π check |
| **3** | 19–25 Nov | End-to-end task path, integration tests incl. node death mid-task | Malformed input and closed connections handled, lost-packet tests | Bucket refresh, dead-peer removal, task records in the DHT | Lowest-CPU router (RAM tie-break), offer/accept handshake | Heartbeats every 2 s, fail after 3 misses, re-route, task states |
| **4** | 26 Nov – 4 Dec | `meshweaver start` / `submit`, live dashboard, `pyproject.toml`, README, report, PPT | TLS with self-signed certs, signing helper | 10–20 node join/leave tests, discovery timing, peer list for dashboard | Routing-under-load tests, load data for dashboard | Signature check before running, chaos test killing nodes |

## Reviews

| Review | Window | What is checked | Demo |
|---|---|---|---|
| **Mid Review** | 20–27 Nov 2026 | Week 1 + Week 2 | 10-node launcher forming the mesh · two nodes pinging · every node found the others · live load tables · Monte Carlo π running remotely |
| **Final Review** | 5–10 Dec 2026 | Week 3 + Week 4 | Dashboard while nodes are killed and tasks re-route · encrypted connection and rejected unsigned request · task skipping a busy node |

Scoring weight: implementation 40% · GitHub commit discipline 30% · code
quality and documentation 20% · team collaboration 10%. Each review also has a
viva, attendance and a social media video.

## GitHub rules

From the Infotact Project Execution Handbook:

- **Own branch only.** Each member commits to their own branch. `main` holds
  the merged code and is updated only by the Team Lead after review.
- **Mid Review:** the team needs commits on **at least 10 different days** in
  the 14 days before the review, with each active member committing in their
  own branch.
- **Final Review:** the team needs commits **every day** of the 20 days before
  the review. A gap marks the project incomplete.
- **Dashboard:** connect your own branch, and only yours, in the Project
  Progress tab.
- **Communication:** reply in the team channel within 24 hours. A member silent
  for 48 hours is reported to Infotact and their work is reassigned.

| Valid commits | Invalid commits |
|---|---|
| Code changes, tests | Empty commits |
| Documentation and README updates | Single-character changes |
| Configuration changes | The same commit repeated across days |
| Assets (images, icons) | Auto-generated spam |
| Merge commits | Commits outside your own branch |

**Commit message style:** `<module>: <what changed>`, for example
`dht: add FIND_NODE iterative lookup` or `gossip: mark peers stale after 15 s`.
Commit small, working pieces every day.

## Progress

| Week | A | B | C | D | E |
|---|---|---|---|---|---|
| 1 | ✅ Protocol, dispatcher (30 tests), architecture doc, skeleton | ⬜ | ✅ Node IDs, k-buckets, routing table, tests | ✅ Load collector, gossip message, tests | ⬜ |
| 2 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| 3 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| 4 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
