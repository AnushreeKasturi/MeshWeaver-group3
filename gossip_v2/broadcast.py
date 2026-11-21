
import asyncio
import logging
import time

from gossip.collector import collect_system_load

from net.message import create_message
from net.transport import send_rpc

from gosip_v2.peer_register import get_nearest_neighbours


GOSSIP_INTERVAL = 5
MAX_NEIGHBOURS = 3


class GossipBroadcaster:

    def __init__(self, dht_node):

        self.dht_node = dht_node

        self.task = None
        self.running = False

    async def send_load(self):

        # Get nearest peers from Member C's DHT
        neighbours = get_nearest_neighbours(
            local_node_id=self.dht_node.node_id,
            routing_table=self.dht_node.routing_table,
            peer_directory=self.dht_node.peer_directory,
            count=MAX_NEIGHBOURS
        )

        if not neighbours:
            print("No neighbours available for gossip")
            return

       
        load_info = collect_system_load()

        # Include local node information so receivers can learn us
        sender_info = {
            "node_id": self.dht_node.node_id,
            "ip_address": self.dht_node.host,
            "port": self.dht_node.port
        }

        payload = {
            "cpu_usage": load_info["cpu_usage"],
            "free_ram_gb": load_info["free_ram_gb"],
            "timestamp": time.time(),
            "sender": sender_info
        }

        async def send_to_peer(peer):

            message = create_message(
                "GOSSIP_LOAD",
                self.dht_node.node_id,
                payload
            )

            try:

                response = await send_rpc(
                    peer,
                    message
                )

                print(
                    "Gossip sent to:",
                    peer["node_id"],
                    "| Response:",
                    response["type"]
                )

            except Exception as error:

                logging.warning(
                    "Gossip failed for peer %s: %s",
                    peer["node_id"],
                    error
                )


        await asyncio.gather(
            *[
                send_to_peer(peer)
                for peer in neighbours
            ]
        )

    async def run(self):

        self.running = True

        print("Gossip broadcaster started")

        try:

            while self.running:

                await self.send_load()

                await asyncio.sleep(GOSSIP_INTERVAL)

        except asyncio.CancelledError:

            print("Gossip broadcaster stopped")

            raise

    def start(self):

        if self.task is None or self.task.done():

            self.task = asyncio.create_task(
                self.run()
            )

    async def stop(self):

        self.running = False

        if self.task is not None:

            self.task.cancel()

            try:
                await self.task

            except asyncio.CancelledError:
                pass

            self.task = None
