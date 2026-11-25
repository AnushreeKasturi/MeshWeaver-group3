"""MeshWeaver: a pure-Python, serverless task queue.

Nodes find each other through a Kademlia DHT, share CPU and RAM load by
gossip, and send each task to the least-loaded peer. See docs/architecture.md.
"""

__version__ = "0.1.0"
