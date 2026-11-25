"""Transport interface used by the node runtime, plus a minimal UDP version.

:class:`Transport` is the contract between the runtime (member A) and the
networking module (member B). :class:`UdpTransport` is a small working
implementation so the runtime, the launcher and the integration tests run
today. Member B's ``meshweaver/net/`` transport (TCP for large payloads,
retries, TLS) replaces it by implementing the same four methods.

How it works:

* One UDP socket per node, bound to ``config.host:config.port``. Requests and
  replies use that same socket, so a peer's source address is also its
  listening address.
* Every datagram is handed to ``Dispatcher.dispatch_bytes``; any bytes it
  returns are sent back to the sender.
* :meth:`request` stores a future under the request's ``msg_id``. The reply
  hook installed on the dispatcher resolves that future when a message with
  the matching ``reply_to`` arrives, and the caller's ``await`` returns.
"""

from __future__ import annotations

import abc
import asyncio
import socket
import sys
from typing import Optional

from .logs import NodeLogger
from .protocol import Dispatcher, Message

Address = tuple[str, int]


def _make_udp_socket(host: str, port: int) -> socket.socket:
    """Bound, non-blocking UDP socket.

    On Windows, a datagram sent to a closed port makes the next receive fail
    with WSAECONNRESET, and after several of these asyncio's proactor stops
    reading the socket entirely, so the node goes deaf as soon as a peer dies.
    Turning off SIO_UDP_CONNRESET makes Windows ignore those ICMP errors, like
    Linux and macOS already do.
    """
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_DGRAM)
    try:
        if sys.platform == "win32":
            _disable_udp_connreset(sock)
        sock.bind((host, port))
        sock.setblocking(False)
    except BaseException:
        sock.close()
        raise
    return sock


def _disable_udp_connreset(sock: socket.socket) -> None:
    """Windows only: WSAIoctl(SIO_UDP_CONNRESET, FALSE).

    Python's ``socket.ioctl`` does not accept this control code, so call
    Winsock directly.
    """
    import ctypes
    from ctypes import wintypes

    sio_udp_connreset = 0x9800000C
    wsa_ioctl = ctypes.windll.ws2_32.WSAIoctl
    wsa_ioctl.argtypes = [
        ctypes.c_size_t, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD,
        ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD),
        ctypes.c_void_p, ctypes.c_void_p,
    ]
    wsa_ioctl.restype = ctypes.c_int
    flag = wintypes.BOOL(False)
    returned = wintypes.DWORD(0)
    if wsa_ioctl(sock.fileno(), sio_udp_connreset, ctypes.byref(flag),
                 ctypes.sizeof(flag), None, 0, ctypes.byref(returned), None, None) != 0:
        raise OSError(ctypes.windll.ws2_32.WSAGetLastError(),
                      "WSAIoctl(SIO_UDP_CONNRESET) failed")


class TransportError(Exception):
    """Sending failed: message too large, transport closed, and so on."""


class RequestTimeout(TransportError, asyncio.TimeoutError):
    """No reply arrived within the timeout."""


class Transport(abc.ABC):
    """What the runtime and every module need from the network layer."""

    address: Optional[Address] = None

    @abc.abstractmethod
    async def start(self, host: str, port: int) -> Address:
        """Start listening; return the actual (host, port) bound."""

    @abc.abstractmethod
    def send(self, addr: Address, msg: Message) -> None:
        """Fire-and-forget one message."""

    @abc.abstractmethod
    async def request(self, addr: Address, msg: Message,
                      timeout: Optional[float] = None) -> Message:
        """Send ``msg`` and wait for the reply whose ``reply_to`` is ``msg.msg_id``."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Stop listening and fail any requests still waiting."""


class UdpTransport(Transport, asyncio.DatagramProtocol):
    def __init__(self, dispatcher: Dispatcher, log: NodeLogger,
                 max_message_size: int = 60_000, default_timeout: float = 2.0) -> None:
        self.dispatcher = dispatcher
        self.log = log
        self.max_message_size = max_message_size
        self.default_timeout = default_timeout
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._pending: dict[str, asyncio.Future] = {}
        self._inflight: set[asyncio.Task] = set()
        self._closed = False
        self.stats = {"sent": 0, "received": 0, "timeouts": 0}
        dispatcher.set_reply_hook(self._on_reply)

    # -- Transport API -------------------------------------------------------

    async def start(self, host: str, port: int) -> Address:
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(lambda: self, sock=_make_udp_socket(host, port))
        sock = self._transport.get_extra_info("sockname")
        self.address = (sock[0], sock[1])
        return self.address

    def send(self, addr: Address, msg: Message) -> None:
        if self._transport is None or self._closed:
            raise TransportError("transport is not running")
        data = msg.encode()
        if len(data) > self.max_message_size:
            raise TransportError(
                f"{msg.type.value} is {len(data)} bytes, over the "
                f"{self.max_message_size}-byte UDP limit")
        self._transport.sendto(data, addr)
        self.stats["sent"] += 1

    async def request(self, addr: Address, msg: Message,
                      timeout: Optional[float] = None) -> Message:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[msg.msg_id] = fut
        try:
            self.send(addr, msg)
            return await asyncio.wait_for(fut, timeout or self.default_timeout)
        except asyncio.TimeoutError:
            self.stats["timeouts"] += 1
            raise RequestTimeout(
                f"no reply to {msg.type.value} from {addr[0]}:{addr[1]}") from None
        finally:
            self._pending.pop(msg.msg_id, None)

    async def close(self) -> None:
        self._closed = True
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(TransportError("transport closed"))
        self._pending.clear()
        for task in list(self._inflight):
            task.cancel()
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    # -- asyncio.DatagramProtocol callbacks ---------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Address) -> None:
        if self._closed:
            return
        self.stats["received"] += 1
        task = asyncio.get_running_loop().create_task(self._handle(data, addr))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    def error_received(self, exc: Exception) -> None:
        # On Windows a datagram sent to a dead port comes back as an ICMP error
        # here. The request that sent it will simply time out.
        self.log.debug("udp error: %s", exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._transport = None

    # -- internals -----------------------------------------------------------

    async def _handle(self, data: bytes, addr: Address) -> None:
        reply = await self.dispatcher.dispatch_bytes(data, addr)
        if reply is not None and self._transport is not None and not self._closed:
            self._transport.sendto(reply, addr)
            self.stats["sent"] += 1

    def _on_reply(self, msg: Message) -> bool:
        fut = self._pending.get(msg.reply_to or "")
        if fut is None or fut.done():
            return False
        fut.set_result(msg)
        return True
