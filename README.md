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
| **C**: Marimganti Lakshmi Sahana | Kademlia DHT and peer discovery | `meshweaver/dht/` | `member--c` |
| **D**: PadmaSrivalli Bathula | Gossip protocol and task routing | `gossip/`, `meshweaver/scheduler/` | `member---d` |
| **E** | Task serialization, execution and fault tolerance | `meshweaver/tasks/` | `member--e` |

B and E have not been confirmed on the roster and have made no commits.

## Repository layout

```
MeshWeaver-group3/
├── meshweaver/
│   ├── protocol.py      # A: shared Message format + Dispatcher
│   ├── node.py          # A: node runtime and Module interface
│   ├── config.py        # A: ports, intervals, timeouts
│   ├── logs.py          # A: structured event logging
│   ├── transport.py     # A: Transport interface + minimal UDP (until B's net/ lands)
│   ├── cli.py           # A: `meshweaver start` / `submit` (Week 4)
│   ├── dashboard.py     # A: live rich dashboard (Week 4)
│   ├── net/             # B: UDP/TCP transport, TLS
│   ├── dht/             # C: node IDs, k-buckets, RPCs, iterative lookup, bootstrap
│   ├── scheduler/       # D: least-loaded routing (Week 3)
│   └── tasks/           # E: serializer, executor, heartbeats
├── gossip/              # D: load collector, peer load store, GossipModule
├── tests/
│   ├── unit/            # A: protocol, config, logging, node runtime
│   ├── gossip/          # D: gossip tests
│   ├── testcase_*.py    # C: DHT tests
│   └── integration/     # A: 10-node mesh and (Week 3) end-to-end task path
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

### Run a mesh

```bash
python scripts/launch_mesh.py                       # 10 nodes on ports 9000-9009
python scripts/launch_mesh.py --nodes 20 --base-port 9100 --log-json
python scripts/launch_mesh.py --multicast           # no seed addresses: multicast discovery
```

Each node runs member C's DHT and member D's gossip. The launcher prints every
node's DHT peer count and the gossip load reports it holds until every node
knows all the others, e.g. `Mesh formed in 2.0s: every node knows all 9 others.`
Logs are in `logs/node-<i>.log`. Ctrl+C stops every node.

### Run a single node

```bash
python -m meshweaver.node --name node-0 --port 9000
python -m meshweaver.node --name node-1 --port 9001 --seed 127.0.0.1:9000
```

Any setting can also come from a JSON file (`--config node.json`) or an
environment variable such as `MESHWEAVER_GOSSIP_INTERVAL=5`. See
[docs/architecture.md](docs/architecture.md#6-node-runtime-meshweavernodepy) for
the settings table and the `Module` interface every member implements.

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

_Last checked: 25 Nov 2026 (end of Week 3). ✅ done · 🟡 partial · ⬜ not started_

| Week | A (`member-a`) | B (`member-b`) | C (`member--c`) | D (`member---d`) | E (`member--e`) |
|---|---|---|---|---|---|
| 1 | ✅ Protocol, dispatcher, architecture doc, skeleton | ⬜ No commits | ✅ Node IDs, XOR distance, k-buckets, routing table | ✅ Load collector, gossip message format | ⬜ No commits |
| 2 | ✅ Node runtime, config, structured logging, 10-node launcher, multicast settings, branches merged into `main` | ⬜ | ✅ PING / STORE / FIND_NODE / FIND_VALUE, iterative lookup (α = 3), seed bootstrap; multicast bootstrap in progress | ✅ `GossipModule`: load to 3 nearest every 5 s, peer load store, 15 s staleness | ⬜ |
| 3 | ⬜ End-to-end task path, integration tests | ⬜ | ⬜ | ⬜ Router, offer/accept handshake | ⬜ |
| 4 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

### `main` branch

`main` holds `member-a`, `member--c` and `member---d`, merged on 25 Nov (`8b67315`).

| Check | Result |
|---|---|
| All tests (`python -m pytest`) | 144 / 145 pass. The failure is `tests/testcase_node.py::test_find_node_handler`: the test reads `FindNodeReply.peers` as objects instead of calling `.peer_infos()`. |
| `python scripts/launch_mesh.py` | Mesh formed in 2.0 s: every node knows all 9 others |
| C's DHT in a live 10-node mesh | Every node finds all 9 others |
| D's gossip in a live 10-node mesh | 30 load reports delivered (10 nodes × 3); a stopped node's load expires everywhere |

**28 Nov:** C's and D's latest work is merged into `main` (151/151 tests).
On `member-a`, nodes now run C's `DHT` and D's `GossipModule` by default
instead of the placeholder, and the launcher shows both:
`python scripts/launch_mesh.py` (seed) and `--multicast` (no seeds) each form
a 10-node mesh in about 2 s with gossip load reports flowing.

### Commit days

| Member | Days with commits (Nov) | Last commit |
|---|---|---|
| A | 13, 21, 22, 25 | 25 Nov |
| B | none | none |
| C | 11–17, 21–24 | 24 Nov |
| D | 11, 12, 13, 15, 17, 21–24 | 24 Nov |
| E | none | none |
| **Team** | **11–17, 21–25 (12 days)** | |

**Mid Review (20–27 Nov) needs 10 commit days in the 14 days before it.**
A review on 25 or 26 Nov already meets it. For a review on 27 Nov the window is
14–27 Nov, which has 9 days so far, so at least one commit is needed on 26 or 27 Nov.
The Final Review needs a commit **every day** for the 20 days before it.

### Action items

- **A:** Wire C's DHT and D's gossip into `default_modules()`; start the Week 3 end-to-end task path and integration tests.
- **B:** No commits. The transport in `meshweaver/transport.py` is a placeholder until `net/` lands.
- **C:** Fix `test_find_node_handler` (use `.peer_infos()`); finish multicast bootstrap using the `multicast_*` settings in `NodeConfig`; Week 3 bucket refresh, dead-peer removal and task records.
- **D:** Week 3 router (lowest CPU, RAM tie-break, skip stale) and offer/accept handshake in `meshweaver/scheduler/`.
- **E:** No commits. Task execution blocks the Week 3 end-to-end path.
- **B and E:** Over 48 hours without a reply must be reported to Infotact with screenshots, and their work reassigned to active members.
