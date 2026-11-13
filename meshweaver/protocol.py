"""MeshWeaver wire protocol: the shared message format and the dispatcher.

Every module (net, dht, gossip, scheduler, tasks) imports from here, so this
file is the contract between team members. The rules are:

* Every message on the wire is a :class:`Message` with four core fields:
  ``type``, ``msg_id``, ``sender_id`` and ``payload``.
* ``payload`` is always a plain JSON-compatible ``dict``. Each message type has
  a payload dataclass below (``LoadReport``, ``TaskSubmit``, ...) that builds
  and parses that dict, so nobody hand-writes dictionary keys.
* Messages are encoded as UTF-8 JSON. ``bytes`` values (for example a
  cloudpickled task) are carried as ``{"__b64__": "..."}`` automatically.
* Replies set ``reply_to`` to the ``msg_id`` of the request, which is what the
  transport uses to match a response to the caller awaiting it.
* Incoming messages are routed to module handlers by :class:`Dispatcher`.
"""

from __future__ import annotations

import asyncio
import base64
import enum
import inspect
import json
import logging
import time
import uuid
from dataclasses import MISSING, asdict, dataclass, field, fields
from typing import Any, Awaitable, Callable, Optional, Type, TypeVar, Union

log = logging.getLogger(__name__)

PROTOCOL_VERSION = 1
NODE_ID_BITS = 160
NODE_ID_HEX_LEN = NODE_ID_BITS // 4  # 40 hex characters


class ProtocolError(Exception):
    """Raised when bytes on the wire are not a valid MeshWeaver message."""


class MessageType(str, enum.Enum):
    """Every message type in the mesh, grouped by the module that handles it."""

    # Transport (member B): liveness check.
    PING = "PING"
    PONG = "PONG"

    # Kademlia DHT (member C).
    FIND_NODE = "FIND_NODE"
    FIND_NODE_REPLY = "FIND_NODE_REPLY"
    FIND_VALUE = "FIND_VALUE"
    FIND_VALUE_REPLY = "FIND_VALUE_REPLY"
    STORE = "STORE"
    STORE_REPLY = "STORE_REPLY"

    # Gossip and routing (member D).
    GOSSIP_LOAD = "GOSSIP_LOAD"
    TASK_OFFER = "TASK_OFFER"
    TASK_OFFER_REPLY = "TASK_OFFER_REPLY"

    # Task execution and fault tolerance (member E).
    TASK_SUBMIT = "TASK_SUBMIT"
    TASK_RESULT = "TASK_RESULT"
    HEARTBEAT = "HEARTBEAT"

    # Generic failure reply, sent by the dispatcher.
    ERROR = "ERROR"


# Which module owns each message type. The node runtime uses this to check
# that every type has a handler once all modules are registered.
MODULE_OF: dict[MessageType, str] = {
    MessageType.PING: "net",
    MessageType.PONG: "net",
    MessageType.FIND_NODE: "dht",
    MessageType.FIND_NODE_REPLY: "dht",
    MessageType.FIND_VALUE: "dht",
    MessageType.FIND_VALUE_REPLY: "dht",
    MessageType.STORE: "dht",
    MessageType.STORE_REPLY: "dht",
    MessageType.GOSSIP_LOAD: "gossip",
    MessageType.TASK_OFFER: "scheduler",
    MessageType.TASK_OFFER_REPLY: "scheduler",
    MessageType.TASK_SUBMIT: "tasks",
    MessageType.TASK_RESULT: "tasks",
    MessageType.HEARTBEAT: "tasks",
    MessageType.ERROR: "node",
}


# --------------------------------------------------------------------------
# Node IDs
# --------------------------------------------------------------------------

def node_id_to_hex(node_id: int) -> str:
    """Encode a 160-bit integer node ID (as produced by dht.node_id) for the wire."""
    if not 0 <= node_id < (1 << NODE_ID_BITS):
        raise ValueError(f"node ID out of 160-bit range: {node_id}")
    return format(node_id, f"0{NODE_ID_HEX_LEN}x")


def node_id_from_hex(text: str) -> int:
    """Decode a wire node ID back into the integer form the DHT uses."""
    if len(text) != NODE_ID_HEX_LEN:
        raise ValueError(f"node ID must be {NODE_ID_HEX_LEN} hex chars, got {len(text)}")
    return int(text, 16)


def new_msg_id() -> str:
    """Return a fresh, globally unique message ID."""
    return uuid.uuid4().hex


# --------------------------------------------------------------------------
# JSON encoding with bytes support
# --------------------------------------------------------------------------

_B64_KEY = "__b64__"


def _to_wire(value: Any) -> Any:
    """Recursively convert a payload into JSON-safe values."""
    if isinstance(value, (bytes, bytearray)):
        return {_B64_KEY: base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): _to_wire(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_wire(v) for v in value]
    return value


def _from_wire(value: Any) -> Any:
    """Undo :func:`_to_wire`, turning base64 markers back into bytes."""
    if isinstance(value, dict):
        if set(value) == {_B64_KEY}:
            return base64.b64decode(value[_B64_KEY])
        return {k: _from_wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_from_wire(v) for v in value]
    return value


# --------------------------------------------------------------------------
# Payload dataclasses
# --------------------------------------------------------------------------

P = TypeVar("P", bound="Payload")


@dataclass
class Payload:
    """Base class for typed payloads. Subclasses are plain dataclasses."""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls: Type[P], data: dict) -> P:
        """Build the payload, ignoring unknown keys so newer senders stay compatible."""
        if not isinstance(data, dict):
            raise ProtocolError(f"{cls.__name__} payload must be a dict")
        known = {f.name for f in fields(cls)}
        missing = [
            f.name for f in fields(cls)
            if f.name not in data and f.default is MISSING and f.default_factory is MISSING
        ]
        if missing:
            raise ProtocolError(f"{cls.__name__} payload missing fields: {missing}")
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass
class PeerInfo(Payload):
    """How to reach a peer. ``node_id`` is the 40-char hex form."""

    node_id: str
    host: str
    port: int


@dataclass
class Ping(Payload):
    """PING and PONG payload. ``port`` lets the receiver learn our listen port."""

    port: int = 0


@dataclass
class FindNode(Payload):
    target_id: str


@dataclass
class FindNodeReply(Payload):
    peers: list = field(default_factory=list)  # list of PeerInfo dicts

    @classmethod
    def from_peers(cls, peers: list[PeerInfo]) -> "FindNodeReply":
        return cls(peers=[p.to_dict() for p in peers])

    def peer_infos(self) -> list[PeerInfo]:
        return [PeerInfo.from_dict(p) for p in self.peers]


@dataclass
class FindValue(Payload):
    key: str


@dataclass
class FindValueReply(Payload):
    """Either ``value`` is set (found) or ``peers`` holds closer nodes to ask."""

    found: bool
    value: Any = None
    peers: list = field(default_factory=list)


@dataclass
class Store(Payload):
    key: str
    value: Any


@dataclass
class StoreReply(Payload):
    ok: bool


@dataclass
class LoadReport(Payload):
    """GOSSIP_LOAD payload. Same fields as gossip.protocol.GossipMessage."""

    node_id: str
    cpu_usage: float
    free_ram_gb: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class TaskOffer(Payload):
    """Ask a peer whether it will take a task before sending the code."""

    task_id: str


@dataclass
class TaskOfferReply(Payload):
    task_id: str
    accepted: bool
    reason: str = ""


@dataclass
class TaskSubmit(Payload):
    """The task itself. ``blob`` is cloudpickle bytes of (func, args, kwargs)."""

    task_id: str
    blob: bytes
    timeout: float = 60.0
    signature: Optional[bytes] = None


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    REROUTED = "re-routed"


@dataclass
class TaskResult(Payload):
    """``result`` is cloudpickle bytes of the return value when status is done."""

    task_id: str
    status: str
    result: Optional[bytes] = None
    error: str = ""


@dataclass
class Heartbeat(Payload):
    task_id: str
    seq: int
    status: str = TaskStatus.RUNNING.value


@dataclass
class ErrorInfo(Payload):
    code: str
    detail: str = ""


# The payload class that goes with each message type.
PAYLOAD_OF: dict[MessageType, Type[Payload]] = {
    MessageType.PING: Ping,
    MessageType.PONG: Ping,
    MessageType.FIND_NODE: FindNode,
    MessageType.FIND_NODE_REPLY: FindNodeReply,
    MessageType.FIND_VALUE: FindValue,
    MessageType.FIND_VALUE_REPLY: FindValueReply,
    MessageType.STORE: Store,
    MessageType.STORE_REPLY: StoreReply,
    MessageType.GOSSIP_LOAD: LoadReport,
    MessageType.TASK_OFFER: TaskOffer,
    MessageType.TASK_OFFER_REPLY: TaskOfferReply,
    MessageType.TASK_SUBMIT: TaskSubmit,
    MessageType.TASK_RESULT: TaskResult,
    MessageType.HEARTBEAT: Heartbeat,
    MessageType.ERROR: ErrorInfo,
}


# --------------------------------------------------------------------------
# The message envelope
# --------------------------------------------------------------------------

@dataclass
class Message:
    """The envelope every MeshWeaver message travels in."""

    type: MessageType
    msg_id: str
    sender_id: str
    payload: dict = field(default_factory=dict)
    reply_to: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        try:
            self.type = MessageType(self.type)
        except ValueError:
            raise ProtocolError(f"unknown message type: {self.type!r}") from None
        if not isinstance(self.msg_id, str) or not self.msg_id:
            raise ProtocolError("msg_id must be a non-empty string")
        if not isinstance(self.sender_id, str) or not self.sender_id:
            raise ProtocolError("sender_id must be a non-empty string")
        if isinstance(self.payload, Payload):
            self.payload = self.payload.to_dict()
        if not isinstance(self.payload, dict):
            raise ProtocolError("payload must be a dict or Payload")

    # -- construction -------------------------------------------------------

    @classmethod
    def create(
        cls,
        msg_type: Union[MessageType, str],
        sender_id: str,
        payload: Union[Payload, dict, None] = None,
    ) -> "Message":
        """Build a new request or one-way message with a fresh msg_id."""
        return cls(
            type=msg_type,
            msg_id=new_msg_id(),
            sender_id=sender_id,
            payload=payload if payload is not None else {},
        )

    def reply(
        self,
        msg_type: Union[MessageType, str],
        sender_id: str,
        payload: Union[Payload, dict, None] = None,
    ) -> "Message":
        """Build the response to this message (``reply_to`` = our ``msg_id``)."""
        msg = Message.create(msg_type, sender_id, payload)
        msg.reply_to = self.msg_id
        return msg

    def error_reply(self, sender_id: str, code: str, detail: str = "") -> "Message":
        return self.reply(MessageType.ERROR, sender_id, ErrorInfo(code, detail))

    # -- payload access -----------------------------------------------------

    def parsed(self) -> Payload:
        """Return the payload as the dataclass registered for this type."""
        return PAYLOAD_OF[self.type].from_dict(self.payload)

    # -- wire format --------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "type": self.type.value,
            "msg_id": self.msg_id,
            "sender_id": self.sender_id,
            "reply_to": self.reply_to,
            "timestamp": self.timestamp,
            "payload": _to_wire(self.payload),
        }

    def encode(self) -> bytes:
        """Serialise to compact UTF-8 JSON bytes for a socket."""
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    @classmethod
    def decode(cls, data: Union[bytes, str]) -> "Message":
        """Parse bytes from a socket. Raises :class:`ProtocolError` on bad input."""
        try:
            raw = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError(f"not valid JSON: {exc}") from None
        if not isinstance(raw, dict):
            raise ProtocolError("message must be a JSON object")
        version = raw.get("version", PROTOCOL_VERSION)
        if version != PROTOCOL_VERSION:
            raise ProtocolError(f"unsupported protocol version {version}")
        for key in ("type", "msg_id", "sender_id"):
            if key not in raw:
                raise ProtocolError(f"missing field: {key}")
        try:
            payload = _from_wire(raw.get("payload") or {})
        except (ValueError, TypeError) as exc:
            raise ProtocolError(f"bad payload encoding: {exc}") from None
        return cls(
            type=raw["type"],
            msg_id=raw["msg_id"],
            sender_id=raw["sender_id"],
            payload=payload,
            reply_to=raw.get("reply_to"),
            timestamp=raw.get("timestamp", time.time()),
            version=version,
        )


# --------------------------------------------------------------------------
# Dispatcher
# --------------------------------------------------------------------------

Address = Any  # (host, port) tuple from asyncio, or None in tests
HandlerResult = Optional[Message]
Handler = Callable[[Message, Address], Union[HandlerResult, Awaitable[HandlerResult]]]


class Dispatcher:
    """Routes each incoming message to the handler its module registered.

    Handlers take ``(message, addr)`` and may be plain functions or coroutines.
    A handler returns a reply :class:`Message` or ``None`` for one-way messages.

    The dispatcher never lets a bad message or a buggy handler crash the node:
    malformed bytes are dropped, and unknown types or handler exceptions turn
    into an ERROR reply for the sender.

    Replies (messages with ``reply_to`` set) can be intercepted with
    :meth:`set_reply_hook`; the transport uses this to wake the coroutine that
    is awaiting that ``msg_id``. A reply is only passed to a type handler if no
    hook claims it.
    """

    def __init__(self, local_id: str) -> None:
        self.local_id = local_id
        self._handlers: dict[MessageType, Handler] = {}
        self._reply_hook: Optional[Callable[[Message], bool]] = None
        self.stats: dict[str, int] = {"dispatched": 0, "dropped": 0, "errors": 0}

    def register(
        self, msg_type: Union[MessageType, str], handler: Handler, *, replace: bool = False
    ) -> None:
        msg_type = MessageType(msg_type)
        if msg_type in self._handlers and not replace:
            raise ValueError(f"handler already registered for {msg_type.value}")
        self._handlers[msg_type] = handler

    def unregister(self, msg_type: Union[MessageType, str]) -> None:
        self._handlers.pop(MessageType(msg_type), None)

    def on(self, msg_type: Union[MessageType, str]) -> Callable[[Handler], Handler]:
        """Decorator form of :meth:`register`."""

        def deco(fn: Handler) -> Handler:
            self.register(msg_type, fn)
            return fn

        return deco

    def has_handler(self, msg_type: Union[MessageType, str]) -> bool:
        return MessageType(msg_type) in self._handlers

    def missing_handlers(self) -> list[MessageType]:
        """Request types with no handler yet (replies are handled by the hook)."""
        return [
            t for t in MessageType
            if t not in self._handlers and t is not MessageType.ERROR
            and not t.value.endswith("_REPLY") and t is not MessageType.PONG
        ]

    def set_reply_hook(self, hook: Optional[Callable[[Message], bool]]) -> None:
        """``hook(msg)`` returns True if it consumed the reply."""
        self._reply_hook = hook

    async def dispatch(self, msg: Message, addr: Address = None) -> HandlerResult:
        """Run the handler for ``msg`` and return its reply, if any."""
        self.stats["dispatched"] += 1

        if msg.reply_to is not None and self._reply_hook is not None:
            try:
                if self._reply_hook(msg):
                    return None
            except Exception:  # noqa: BLE001 - a hook bug must not kill the node
                log.exception("reply hook failed for %s", msg.msg_id)

        handler = self._handlers.get(msg.type)
        if handler is None:
            if msg.reply_to is not None or msg.type is MessageType.ERROR:
                # Unclaimed reply or error: nothing to answer, just drop it.
                self.stats["dropped"] += 1
                log.debug("dropping unclaimed %s %s", msg.type.value, msg.msg_id)
                return None
            self.stats["errors"] += 1
            log.warning("no handler for %s from %s", msg.type.value, msg.sender_id)
            return msg.error_reply(self.local_id, "NO_HANDLER", msg.type.value)

        try:
            result = handler(msg, addr)
            if inspect.isawaitable(result):
                result = await result
        except asyncio.CancelledError:
            raise
        except ProtocolError as exc:
            self.stats["errors"] += 1
            log.warning("bad %s payload from %s: %s", msg.type.value, msg.sender_id, exc)
            return msg.error_reply(self.local_id, "BAD_PAYLOAD", str(exc))
        except Exception as exc:  # noqa: BLE001
            self.stats["errors"] += 1
            log.exception("handler for %s failed", msg.type.value)
            return msg.error_reply(self.local_id, "HANDLER_FAILED", repr(exc))

        if result is not None and not isinstance(result, Message):
            self.stats["errors"] += 1
            log.error("handler for %s returned %r, not a Message", msg.type.value, result)
            return msg.error_reply(self.local_id, "HANDLER_FAILED", "bad return type")
        return result

    async def dispatch_bytes(self, data: bytes, addr: Address = None) -> Optional[bytes]:
        """Decode raw socket bytes, dispatch, and return the encoded reply (if any).

        This is the single entry point the transport calls for every datagram
        or TCP frame it receives.
        """
        try:
            msg = Message.decode(data)
        except ProtocolError as exc:
            self.stats["dropped"] += 1
            log.warning("dropping malformed message from %s: %s", addr, exc)
            return None
        reply = await self.dispatch(msg, addr)
        return reply.encode() if reply is not None else None


__all__ = [
    "PROTOCOL_VERSION", "ProtocolError", "MessageType", "MODULE_OF", "PAYLOAD_OF",
    "node_id_to_hex", "node_id_from_hex", "new_msg_id",
    "Payload", "PeerInfo", "Ping", "FindNode", "FindNodeReply", "FindValue",
    "FindValueReply", "Store", "StoreReply", "LoadReport", "TaskOffer",
    "TaskOfferReply", "TaskSubmit", "TaskStatus", "TaskResult", "Heartbeat",
    "ErrorInfo", "Message", "Dispatcher",
]
