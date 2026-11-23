
"""Bootstrap and periodic peer refresh for the DHT."""

import asyncio

from ..protocol import (
    MessageType,
    Ping,
    PeerInfo,
    node_id_from_hex,
    node_id_to_hex,
)

from .lookup import iterative_find_node


async def bootstrap(dht):
    """Connect to the seed nodes from NodeConfig."""

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

            dht.log.info(
                "Connected to seed %s:%s",
                host,
                port,
            )

        except Exception as error:
            dht.log.warning(
                "Could not connect to seed %s:%s: %s",
                host,
                port,
                error,
            )

    await iterative_find_node(
        dht,
        dht.node.node_id,
    )


async def refresh_loop(dht):
    """Refresh the routing table periodically."""

    while True:
        await asyncio.sleep(
            dht.node.config.peer_refresh_interval
        )

        try:
            await iterative_find_node(
                dht,
                dht.node.node_id,
            )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            dht.log.warning(
                "DHT peer refresh failed: %s",
                error,
            )


            
             




        
