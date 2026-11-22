import asyncio
import json
import socket
import struct


MULTICAST_GROUP = "239.1.2.3"
BOOTSTRAP_PORT = 9999


class BootstrapProtocol(asyncio.DatagramProtocol):

    def __init__(self, node_info, peer_callback):

        self.node_info = node_info
        self.peer_callback = peer_callback
        self.transport = None

    def connection_made(self, transport):

        self.transport = transport

        print("Bootstrap listener started")

    def datagram_received(self, data, address):

        try:

            message = json.loads(
                data.decode("utf-8")
            )

            message_type = message.get("type")

            # DISCOVER
            if message_type == "DISCOVER":

                if message.get("node_id") == self.node_info["node_id"]:
                    return

                # Register announcing peer
                peer = {
                    "node_id": message["node_id"],
                    "ip_address": message["ip_address"],
                    "port": message["port"]
                }

                self.peer_callback(peer)

                # Send discovery response
                response = {
                    "type": "DISCOVER_RESPONSE",
                    "node_id": self.node_info["node_id"],
                    "ip_address": self.node_info["ip_address"],
                    "port": self.node_info["port"]
                }

                self.transport.sendto(
                    json.dumps(response).encode("utf-8"),
                    address
                )

            # DISCOVER RESPONSE
            elif message_type == "DISCOVER_RESPONSE":

                peer = {
                    "node_id": message["node_id"],
                    "ip_address": message["ip_address"],
                    "port": message["port"]
                }

                if peer["node_id"] != self.node_info["node_id"]:

                    self.peer_callback(peer)

                    print(
                        "Discovered peer:",
                        peer
                    )

        except (
            ValueError,
            KeyError,
            UnicodeDecodeError,
            TypeError
        ) as error:

            print(
                "Invalid bootstrap message:",
                error
            )

    def error_received(self, error):

        print("Bootstrap UDP error:", error)

    def connection_lost(self, error):

        print("Bootstrap listener closed")


class BootstrapService:

    def __init__(self, node_info, peer_callback):

        self.node_info = node_info
        self.peer_callback = peer_callback
        self.transport = None

    async def start_listener(self):

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP
        )

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        sock.bind(("", BOOTSTRAP_PORT))

        membership = struct.pack(
            "4s4s",
            socket.inet_aton(MULTICAST_GROUP),
            socket.inet_aton("0.0.0.0")
        )

        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            membership
        )

        sock.setblocking(False)

        loop = asyncio.get_running_loop()

        self.transport, _ = await loop.create_datagram_endpoint(

            lambda: BootstrapProtocol(
                self.node_info,
                self.peer_callback
            ),

            sock=sock
        )

        print("Listening for multicast discovery")

    async def discover(self, duration=5):

        if self.transport is None:

            raise RuntimeError(
                "Start bootstrap listener first"
            )

        announcement = {
            "type": "DISCOVER",
            "node_id": self.node_info["node_id"],
            "ip_address": self.node_info["ip_address"],
            "port": self.node_info["port"]
        }

        self.transport.sendto(

            json.dumps(announcement).encode("utf-8"),

            (MULTICAST_GROUP, BOOTSTRAP_PORT)
        )

        print("Searching for MeshWeaver peers...")

        await asyncio.sleep(duration)

    def stop(self):

        if self.transport:

            self.transport.close()

            self.transport = None
