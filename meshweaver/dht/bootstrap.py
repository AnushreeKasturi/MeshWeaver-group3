
"""Seed bootstrap, UDP multicast discovery and periodic DHT refresh."""

import asyncio
import json
import socket
import struct

from ..protocol import (
    MessageType,
    Ping,
    PeerInfo,
    node_id_from_hex,
    node_id_to_hex,
)

from .lookup import iterative_find_node


async def bootstrap(dht):
    """Connect to configured seed nodes, then perform a normal DHT lookup."""

    seeds = dht.node.config.seeds

    for host, port in seeds:
        try:
            response = await dht.node.request(
                (host, port),
                MessageType.PING,
                Ping(port=dht.node.config.port),
                timeout=dht.node.config.rpc_timeout,
            )

            peer_id = node_id_from_hex(response.sender_id)

            peer = PeerInfo(
                node_id=node_id_to_hex(peer_id),
                host=host,
                port=port,
            )

            dht.add_peer(peer)

            dht.log.info("Connected to seed %s:%s", host, port)

        except Exception as error:
            dht.log.warning(
                "Could not connect to seed %s:%s: %s",
                host,
                port,
                error,
            )

    await iterative_find_node(dht, dht.node.node_id)


def _get_advertised_host(dht, multicast_group, multicast_port):
    """Choose an address other nodes can use to contact this node."""

    address = dht.node.address
    if address is None:
        raise RuntimeError("Node transport has not started")

    host, _port = address

    # A specific bind address can be announced directly.
    if host not in ("0.0.0.0", "::", ""):
        return host

    # If bound to all interfaces, find the outbound IPv4 interface.
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((multicast_group, multicast_port))
        return probe.getsockname()[0]
    finally:
        probe.close()


async def multicast_discovery(dht):
    """Discover peers by exchanging UDP multicast announcements."""

    config = dht.node.config
    group = config.multicast_group
    multicast_port = config.multicast_port
    interval = config.multicast_interval

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # Allow multiple nodes on the same computer to join this port.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Listen for multicast announcements.
        sock.bind(("", multicast_port))

        membership = struct.pack(
            "4s4s",
            socket.inet_aton(group),
            socket.inet_aton("0.0.0.0"),
        )
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            membership,
        )

        # Keep multicast traffic on the local network.
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        sock.setblocking(False)

        # Save the socket so DHT.stop() can close it.
        dht._multicast_socket = sock

        advertised_host = _get_advertised_host(
            dht,
            group,
            multicast_port,
        )

        actual_address = dht.node.address
        if actual_address is None:
            raise RuntimeError("Node transport has not started")

        announcement = {
            "node_id": dht.node.id_hex,
            "host": advertised_host,
            "port": actual_address[1],
        }

        message = json.dumps(announcement).encode("utf-8")
        destination = (group, multicast_port)
        loop = asyncio.get_running_loop()

        dht.log.info(
            "Multicast discovery started on %s:%s",
            group,
            multicast_port,
        )

        # Announce immediately, then continue receiving announcements.
        next_announcement = 0.0

        while True:
            now = loop.time()

            if now >= next_announcement:
                await loop.sock_sendto(sock, message, destination)
                next_announcement = loop.time() + interval

            timeout = max(0.1, next_announcement - loop.time())

            try:
                data, _sender_address = await asyncio.wait_for(
                    loop.sock_recvfrom(sock, 4096),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                continue

            try:
                announcement = json.loads(data.decode("utf-8"))

                peer_id_hex = announcement["node_id"]
                peer_host = announcement["host"]
                peer_port = int(announcement["port"])

                peer_id = node_id_from_hex(peer_id_hex)

                if peer_id == dht.node.node_id:
                    continue

                if not peer_host or not 0 < peer_port < 65536:
                    continue

                peer = PeerInfo(
                    node_id=node_id_to_hex(peer_id),
                    host=peer_host,
                    port=peer_port,
                )

                dht.add_peer(peer)

                dht.log.info(
                    "Discovered peer %s at %s:%s",
                    peer_id_hex,
                    peer_host,
                    peer_port,
                )

                # Use the normal DHT lookup to discover additional peers.
                await iterative_find_node(dht, dht.node.node_id)

            except (KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
                dht.log.warning(
                    "Ignoring invalid multicast announcement: %s",
                    error,
                )

            except asyncio.CancelledError:
                raise

            except Exception as error:
                dht.log.warning(
                    "Multicast peer discovery failed: %s",
                    error,
                )

    finally:
        if getattr(dht, "_multicast_socket", None) is sock:
            dht._multicast_socket = None

        sock.close()


async def refresh_loop(dht):
    """Refresh the routing table periodically."""

    while True:
        await asyncio.sleep(dht.node.config.peer_refresh_interval)

        try:
            await iterative_find_node(dht, dht.node.node_id)

        except asyncio.CancelledError:
            raise

        except Exception as error:
            dht.log.warning("DHT peer refresh failed: %s", error)





        
