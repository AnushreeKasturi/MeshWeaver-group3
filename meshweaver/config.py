"""Node configuration: ports, gossip interval and timeouts.

Values are resolved in this order, later sources winning:

1. defaults in :class:`NodeConfig`
2. a JSON file (``--config node.json``)
3. environment variables named ``MESHWEAVER_<FIELD>`` (e.g. ``MESHWEAVER_PORT=9005``)
4. explicit overrides (command-line flags)

Every module reads its settings from the one ``NodeConfig`` the node runtime
creates, so intervals and timeouts are changed in a single place.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Mapping, Optional

ENV_PREFIX = "MESHWEAVER_"

Address = tuple[str, int]


def parse_address(text: str) -> Address:
    """Parse ``"host:port"`` (or just ``"port"``, meaning localhost)."""
    text = text.strip()
    host, sep, port = text.rpartition(":")
    if not sep:
        host, port = "127.0.0.1", text
    try:
        port_num = int(port)
    except ValueError:
        raise ValueError(f"bad address {text!r}: port must be a number") from None
    if not 0 < port_num < 65536:
        raise ValueError(f"bad address {text!r}: port out of range")
    return (host or "127.0.0.1", port_num)


@dataclass
class NodeConfig:
    # Identity and network
    name: str = ""                       # human-readable; also seeds the node ID
    host: str = "127.0.0.1"
    port: int = 9000                     # 0 = let the OS pick a free port
    seeds: list = field(default_factory=list)  # [(host, port), ...] to join through

    # Transport (member B)
    max_message_size: int = 60_000       # bytes; larger payloads need TCP
    rpc_timeout: float = 2.0             # seconds to wait for a reply

    # Membership / DHT (member C)
    k: int = 20                          # k-bucket size and FIND_NODE reply size
    peer_refresh_interval: float = 2.0   # seconds between peer pings
    peer_max_misses: int = 3             # missed pings before a peer is dropped

    # Gossip and routing (member D)
    gossip_interval: float = 5.0         # seconds between load broadcasts
    gossip_fanout: int = 3               # nearest neighbours to gossip to
    stale_after: float = 15.0            # seconds before a load entry is stale

    # Tasks (member E)
    heartbeat_interval: float = 2.0
    heartbeat_max_misses: int = 3
    task_timeout: float = 60.0

    # Runtime and logging (member A)
    shutdown_timeout: float = 5.0        # seconds to wait for modules to stop
    log_level: str = "INFO"
    log_json: bool = False               # JSON lines instead of readable text
    log_file: Optional[str] = None

    def __post_init__(self) -> None:
        self.seeds = [s if isinstance(s, tuple) else _coerce_seed(s) for s in self.seeds]
        self.validate()

    def validate(self) -> None:
        if not 0 <= self.port < 65536:
            raise ValueError(f"port out of range: {self.port}")
        positive = (
            "max_message_size", "rpc_timeout", "k", "peer_refresh_interval",
            "peer_max_misses", "gossip_interval", "gossip_fanout", "stale_after",
            "heartbeat_interval", "heartbeat_max_misses", "task_timeout",
            "shutdown_timeout",
        )
        for name in positive:
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")
        if self.stale_after <= self.gossip_interval:
            raise ValueError("stale_after must be longer than gossip_interval")
        if self.log_level.upper() not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise ValueError(f"unknown log level {self.log_level!r}")

    @property
    def address(self) -> Address:
        return (self.host, self.port)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["seeds"] = [f"{h}:{p}" for h, p in self.seeds]
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NodeConfig":
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        return cls(**dict(data))

    def replace(self, **changes: Any) -> "NodeConfig":
        data = asdict(self)
        data.update({k: v for k, v in changes.items() if v is not None})
        return NodeConfig.from_dict(data)


def _coerce_seed(value: Any) -> Address:
    if isinstance(value, str):
        return parse_address(value)
    host, port = value
    return (str(host), int(port))


def _from_env(env: Mapping[str, str]) -> dict:
    """Read ``MESHWEAVER_*`` variables, converting each to its field's type."""
    out: dict[str, Any] = {}
    for f in fields(NodeConfig):
        raw = env.get(ENV_PREFIX + f.name.upper())
        if raw is None:
            continue
        default = getattr(NodeConfig(), f.name)
        if f.name == "seeds":
            out[f.name] = [parse_address(s) for s in raw.split(",") if s.strip()]
        elif isinstance(default, bool):
            out[f.name] = raw.strip().lower() in {"1", "true", "yes", "on"}
        elif isinstance(default, int):
            out[f.name] = int(raw)
        elif isinstance(default, float):
            out[f.name] = float(raw)
        else:
            out[f.name] = raw
    return out


def load_config(
    path: Optional[str | os.PathLike] = None,
    env: Optional[Mapping[str, str]] = None,
    **overrides: Any,
) -> NodeConfig:
    """Build a config from defaults, an optional JSON file, env vars and overrides."""
    data: dict[str, Any] = asdict(NodeConfig())
    if path is not None:
        loaded = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError(f"{path}: config file must hold a JSON object")
        data.update(loaded)
    data.update(_from_env(os.environ if env is None else env))
    data.update({k: v for k, v in overrides.items() if v is not None})
    return NodeConfig.from_dict(data)
