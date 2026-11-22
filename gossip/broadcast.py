"""Gossip broadcasting and receiving module."""

import asyncio

from meshweaver.node import Module
from meshweaver.protocol import (
    LoadReport,
    MessageType,
)

from .collector import collect_system_load
from .peer_register import get_nearest_neighbours
from .peer_store import PeerLoadStore


class GossipModule(Module):
    """Collect, broadcast and receive peer load information."""

    name = "gossip"

    def __init__(self):
        super().__init__()

        self.peer_store = PeerLoadStore()

    def handlers(self):
        """Register incoming gossip message handlers."""

        return {
            MessageType.GOSSIP_LOAD: self.on_load,
        }

    async def start(self):
        """Start the background gossip process."""

        self.peer_store = PeerLoadStore(
            stale_after=self.node.config.stale_after
        )

        self.spawn(
            self._gossip_loop(),
            name="gossip-broadcast",
        )

    async def _gossip_loop(self):
        """Periodically broadcast local system load."""

        while True:

            try:
                await self.broadcast_load()

                self.peer_store.remove_stale_peers()

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self.log.event(
                    "gossip.error",
                    detail=str(exc),
                )

            await asyncio.sleep(
                self.node.config.gossip_interval
            )

    async def broadcast_load(self):
        """Send local load information to nearest connected peers."""

        load_info = await asyncio.to_thread(
            collect_system_load
        )

        report = LoadReport(
            node_id=self.node.id_hex,
            cpu_usage=load_info["cpu_usage"],
            free_ram_gb=load_info["free_ram_gb"],
        )

        membership = self.node.module("membership")

        if membership is None:
            self.log.event(
                "gossip.warning",
                detail="Membership module not available",
            )
            return

        connected_peers = membership.peer_list()

        nearest_peers = get_nearest_neighbours(
            self.node.node_id,
            connected_peers,
            self.node.config.gossip_fanout,
        )

        sent_count = 0

        for peer in nearest_peers:

            self.node.send(
                peer.address,
                MessageType.GOSSIP_LOAD,
                report,
            )

            sent_count += 1

        self.log.event(
            "gossip.broadcast",
            peers=sent_count,
            cpu_usage=load_info["cpu_usage"],
            free_ram_gb=load_info["free_ram_gb"],
        )

    async def on_load(self, msg, addr):
        """Handle a load report received from another node."""

        report = msg.parsed()

        self.peer_store.update_peer(
            msg.to_dict()
        )

        self.log.event(
            "gossip.received",
            peer=report.node_id,
            cpu_usage=report.cpu_usage,
            free_ram_gb=report.free_ram_gb,
        )

        # Gossip is one-way, so no acknowledgement is returned.
        return None

    def get_peer_loads(self):
        """Return fresh load reports from connected peers."""

        return self.peer_store.get_fresh_peers()

    def status(self):
        """Return the current Gossip module status."""

        return {
            "known_load_reports": len(
                self.peer_store.get_fresh_peers()
            )
        }
