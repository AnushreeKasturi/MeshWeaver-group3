from net.message import create_message
from net.server import NetworkServer

from dht.routing import RoutingTable


class DHTNode:

    def __init__(self, node_info):

        self.node_info = node_info

        self.node_id = node_info["node_id"]
        self.host = node_info["ip_address"]
        self.port = node_info["port"]

        # Member C: DHT routing table
        self.routing_table = RoutingTable(self.node_id)

        # Store complete peer information for Member D
        self.peer_directory = {}

        # Member D: Gossip message handler
        self.gossip_handler = None

        self.data_store = {}

        # Member B: Networking server
        self.server = NetworkServer(
            self.host,
            self.port,
            self.handle_message
        )

    # Register a discovered peer
    def add_peer(self, peer):

        if peer["node_id"] == self.node_id:
            return False

        # Store complete peer details
        self.peer_directory[peer["node_id"]] = peer

        # Routing table stores only peer IDs
        return self.routing_table.add_peer(
            peer["node_id"]
        )

    # Get nearest peers with complete connection details
    def get_nearest_peer_infos(self, target_id, count=20):

        peer_ids = self.routing_table.get_all_peers()

        available_peers = []

        for peer_id in peer_ids:

            peer = self.peer_directory.get(peer_id)

            if peer is not None:
                available_peers.append(peer)

        nearest = sorted(
            available_peers,
            key=lambda peer: (
                int(target_id, 16) ^
                int(peer["node_id"], 16)
            )
        )

        return nearest[:count]

    # Handle incoming network messages
    async def handle_message(self, message):

        message_type = message["type"]
        sender_id = message["sender_id"]
        payload = message["payload"]
        msg_id = message["msg_id"]

        # Learn sender's address
        sender = payload.get("sender")

        if sender:

            if sender.get("node_id") == sender_id:
                self.add_peer(sender)

        # PING
        if message_type == "PING":

            response = create_message(
                "PONG",
                self.node_id,
                {"status": "alive"}
            )

        # STORE
        elif message_type == "STORE":

            key = payload["key"]
            value = payload["value"]

            self.data_store[key] = value

            response = create_message(
                "STORE_RESPONSE",
                self.node_id,
                {"stored": True}
            )

        # FIND_NODE
        elif message_type == "FIND_NODE":

            target_id = payload["target_node_id"]

            peers = self.get_nearest_peer_infos(
                target_id=target_id,
                count=20
            )

            response = create_message(
                "FIND_NODE_RESPONSE",
                self.node_id,
                {"nodes": peers}
            )

        # FIND_VALUE
        elif message_type == "FIND_VALUE":

            key = payload["key"]

            if key in self.data_store:

                response = create_message(
                    "FIND_VALUE_RESPONSE",
                    self.node_id,
                    {
                        "found": True,
                        "value": self.data_store[key]
                    }
                )

            else:

                import hashlib

                target_id = hashlib.sha1(
                    key.encode("utf-8")
                ).hexdigest()

                peers = self.get_nearest_peer_infos(
                    target_id=target_id,
                    count=20
                )

                response = create_message(
                    "FIND_VALUE_RESPONSE",
                    self.node_id,
                    {
                        "found": False,
                        "nodes": peers
                    }
                )

        # MEMBER D: GOSSIP LOAD
        elif message_type == "GOSSIP_LOAD":

            if self.gossip_handler is not None:

                self.gossip_handler(message)

            response = create_message(
                "GOSSIP_ACK",
                self.node_id,
                {"received": True}
            )

        # Unknown message
        else:

            response = create_message(
                "ERROR",
                self.node_id,
                {"error": "Unknown message type"}
            )

        # Preserve original message ID for Member B
        response["msg_id"] = msg_id

        return response

    # Start network server
    async def start(self):

        await self.server.start()

    # Stop network server
    async def stop(self):

        await self.server.stop()
