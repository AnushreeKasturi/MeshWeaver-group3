# MeshWeaver Architecture

MeshWeaver is a pure-Python task queue with no central server. Every machine
runs the same **node** program. Nodes find each other through a Kademlia DHT,
share CPU and RAM load by gossip, and send each task to the least-loaded peer.
If that peer dies, its missing heartbeats trigger a re-route.

## 1. Modules and owners

| Module | Path | Owner | Responsibility |
|---|---|---|---|
| Protocol | `meshweaver/protocol.py` | A | Message envelope, payload dataclasses, dispatcher |
| Node runtime | `meshweaver/node.py`, `config.py` | A | Starts and stops every module on one event loop |
| CLI / dashboard | `meshweaver/cli.py`, `dashboard.py` | A | `meshweaver start/submit`, live `rich` view |
| Transport | `meshweaver/net/` | B | UDP + TCP sockets, request/response matching, TLS, signing |
| DHT | `meshweaver/dht/` | C | 160-bit IDs, k-buckets, lookups, bootstrap, task records |
| Gossip | `meshweaver/gossip/` | D | CPU/RAM collector, periodic load gossip |
| Scheduler | `meshweaver/scheduler/` | D | Pick least-loaded peer, offer/accept handshake |
| Tasks | `meshweaver/tasks/` | E | cloudpickle, process-pool execution, heartbeats, retries |

## 2. Inside one node

```
                 +--------------------------------------------------+
                 |                 node.py (one asyncio loop)       |
                 |                                                  |
   UDP / TCP     |  +-----------+   bytes   +--------------------+  |
 <-------------->|  |  net (B)  |---------->|  Dispatcher        |  |
   other nodes   |  | transport |<----------|  (protocol.py, A)  |  |
                 |  +-----------+   reply   +---------+----------+  |
                 |        ^                  by msg.type|             |
                 |        | send_rpc()     +-----------+-----------+  |
                 |        |                v           v           v  |
                 |  +-----+-----+   +-----------+ +---------+ +--------+|
                 |  |  dht (C)  |   | gossip (D)| |sched (D)| |tasks(E)||
                 |  +-----------+   +-----------+ +---------+ +--------+|
                 |        ^              |  load table ^          |      |
                 |        +-- peers -----+-------------+          |      |
                 |                                    dashboard (A)<-----+
                 +--------------------------------------------------+
```

* **Only the transport touches sockets.** Every other module sends with
  `send_rpc(addr, Message)` (await a reply) or `send(addr, Message)` (one-way).
* **Every incoming message goes through the dispatcher.** The transport calls
  `Dispatcher.dispatch_bytes(data, addr)` for each datagram or TCP frame and
  sends back whatever bytes it returns.
* Modules never import each other's internals. They talk through messages, plus
  a few in-process calls the runtime wires up: the scheduler reads the DHT peer
  list and the gossip load table; tasks call the scheduler to re-route.

## 3. The message format (`meshweaver/protocol.py`)

Every message is a `Message` dataclass:

| Field | Type | Meaning |
|---|---|---|
| `type` | `MessageType` | What the message is (`PING`, `FIND_NODE`, `TASK_SUBMIT`, ...) |
| `msg_id` | `str` | Random UUID hex, unique per message |
| `sender_id` | `str` | Sender's 160-bit node ID as 40 hex characters |
| `payload` | `dict` | Type-specific body, built from a payload dataclass |
| `reply_to` | `str \| None` | For replies: the `msg_id` of the request |
| `timestamp` | `float` | Unix time the message was created |
| `version` | `int` | Protocol version (currently `1`) |

On the wire it is compact UTF-8 JSON. `bytes` inside a payload (pickled tasks,
signatures) are encoded automatically as `{"__b64__": "..."}`.

```json
{"version":1,"type":"GOSSIP_LOAD","msg_id":"9f2c...","sender_id":"3a7e...",
 "reply_to":null,"timestamp":1762841000.2,
 "payload":{"node_id":"3a7e...","cpu_usage":23.5,"free_ram_gb":4.1,"timestamp":1762841000.2}}
```

### Message types and payloads

| Type | Payload class | Handled by | Reply |
|---|---|---|---|
| `PING` / `PONG` | `Ping(port)` | net | `PONG` |
| `FIND_NODE` | `FindNode(target_id)` | dht | `FIND_NODE_REPLY` → `FindNodeReply(peers)` |
| `FIND_VALUE` | `FindValue(key)` | dht | `FIND_VALUE_REPLY` → `FindValueReply(found, value, peers)` |
| `STORE` | `Store(key, value)` | dht | `STORE_REPLY` → `StoreReply(ok)` |
| `GOSSIP_LOAD` | `LoadReport(node_id, cpu_usage, free_ram_gb, timestamp)` | gossip | none (one-way) |
| `TASK_OFFER` | `TaskOffer(task_id)` | scheduler | `TASK_OFFER_REPLY` → `TaskOfferReply(task_id, accepted, reason)` |
| `TASK_SUBMIT` | `TaskSubmit(task_id, blob, timeout, signature)` | tasks | `TASK_RESULT` → `TaskResult(task_id, status, result, error)` |
| `HEARTBEAT` | `Heartbeat(task_id, seq, status)` | tasks | none (one-way, every 2 s) |
| `ERROR` | `ErrorInfo(code, detail)` | — | sent by the dispatcher on failure |

`LoadReport` uses the same four fields as `gossip.protocol.GossipMessage`, so
D's existing code maps onto it directly. Node IDs are ints inside the DHT and
hex strings on the wire: use `node_id_to_hex()` / `node_id_from_hex()`.

### Using it

```python
from meshweaver.protocol import Message, MessageType, LoadReport

msg = Message.create(MessageType.GOSSIP_LOAD, my_id, LoadReport(my_id, 23.5, 4.1))
data = msg.encode()                     # bytes for the socket

received = Message.decode(data)         # raises ProtocolError on garbage
report = received.parsed()              # -> LoadReport
answer = received.reply(MessageType.PONG, my_id)   # reply_to set for you
```

## 4. The dispatcher

`Dispatcher` maps each `MessageType` to one handler. A handler is
`handler(msg, addr)`, sync or `async`, and returns a reply `Message` or `None`.

```python
dispatcher.register(MessageType.FIND_NODE, dht.handle_find_node)

@dispatcher.on(MessageType.GOSSIP_LOAD)
async def on_load(msg, addr):
    table.update(msg.parsed())
```

Rules it enforces, so one bad peer or bug can never take a node down:

1. **Malformed bytes** are logged and dropped (`dispatch_bytes` returns `None`).
2. **Replies** (`reply_to` set) go first to the reply hook the transport
   installs with `set_reply_hook()`; that hook wakes the coroutine awaiting the
   matching `msg_id`. Unclaimed replies fall through to a type handler if one
   exists, otherwise they are dropped.
3. **Unknown types** get an `ERROR` reply with code `NO_HANDLER`.
4. **Handler exceptions** become an `ERROR` reply (`BAD_PAYLOAD` for a
   `ProtocolError`, otherwise `HANDLER_FAILED`) and are logged.
5. Only one handler per type; registering twice raises unless `replace=True`.
6. `missing_handlers()` lists request types with no handler, so the runtime can
   check at startup that every module registered.

## 5. How a task moves through the mesh

```
 Node X (submitter)                                         Node Y (worker)
 ------------------                                         ---------------
 tasks(E): cloudpickle(func, args) -> blob
 scheduler(D): pick least-loaded fresh peer from
               gossip table + dht peer list  ---------------> Y
 net(B): TASK_OFFER --------------------------------------> scheduler(D) checks load
         <------------------------------ TASK_OFFER_REPLY(accepted=True)
 dht(C): STORE task_id -> {node: Y, status: assigned}
 net(B): TASK_SUBMIT(blob) -------------------------------> tasks(E): verify signature,
                                                            run in process pool
         <------------------------------------ HEARTBEAT every 2 s
         <------------------------------------ TASK_RESULT(status=done, result)
 dht(C): STORE task_id -> {status: done}
 dashboard(A): shows result
```

If X misses 3 heartbeats from Y, E marks the task `failed`, asks D's scheduler
for another peer (skipping Y) and resends it as `re-routed`. The task ID in the
DHT guarantees the task is never run twice.

## 6. Startup and shutdown order (node runtime, Week 2)

1. Load config (ports, gossip interval 5 s, stale after 15 s, heartbeat 2 s).
2. Generate the node ID, create the `Dispatcher`.
3. Start the transport (B) and install its reply hook.
4. Start the DHT (C) and bootstrap; register its handlers.
5. Start gossip (D) and the scheduler (D); register handlers.
6. Start tasks (E); register handlers.
7. Check `dispatcher.missing_handlers()` is empty.

Shutdown cancels the background tasks in reverse order and closes sockets last.

## 7. Repository and branches

```
meshweaver/
  protocol.py  node.py  config.py  cli.py  dashboard.py   (A)
  net/         (B)
  dht/         (C)
  gossip/      (D)
  scheduler/   (D)
  tasks/       (E)
tests/
  unit/        one test file per module, e.g. tests/unit/test_dht_bucket.py
  integration/ end-to-end tests (A)
scripts/launch_mesh.py   10-node launcher (A)
docs/architecture.md     this file
```

Each member pushes only to their own branch (`member-a` … `member-e`); the Team
Lead reviews and merges into `main`. Every pull request must use the message
classes in `protocol.py` rather than defining its own wire format.
