"""Structured logging for MeshWeaver.

Every important moment is logged as a named **event** with key/value fields,
so a 10-node run can be followed or filtered later:

    node.start   node.ready   node.stop    module.start   module.stop
    peer.join    peer.leave   gossip.send  gossip.recv
    task.submit  task.assign  task.done    task.failed    task.reroute

Readable form (default)::

    14:02:11.204 INFO  node-3   peer.join      peer=1f3a9c0b host=127.0.0.1 port=9001

JSON-lines form (``log_json=True``), one object per line::

    {"ts": "...", "level": "INFO", "node": "node-3", "event": "peer.join", "peer": "1f3a9c0b", ...}

Usage inside a module::

    self.log.event("gossip.send", peers=3, cpu=12.5)
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional

ROOT_LOGGER = "meshweaver"


class NodeLogger(logging.LoggerAdapter):
    """Logger that stamps every record with the node name and supports events."""

    def __init__(self, node: str, name: str = ROOT_LOGGER) -> None:
        super().__init__(logging.getLogger(name), {"node": node})

    def process(self, msg: Any, kwargs: dict) -> tuple[Any, dict]:
        extra = dict(self.extra)
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        return msg, kwargs

    def child(self, suffix: str) -> "NodeLogger":
        return NodeLogger(self.extra["node"], f"{self.logger.name}.{suffix}")

    def event(self, event: str, level: int = logging.INFO, **fields: Any) -> None:
        """Log a named event with structured fields."""
        self.log(level, event, extra={"event": event, "fields": fields})


def _short(value: Any) -> str:
    text = str(value)
    # 40-char hex node IDs are unreadable in a terminal; keep the first 8.
    if len(text) == 40 and all(c in "0123456789abcdef" for c in text):
        return text[:8]
    return text


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S.%f")[:-3]
        node = getattr(record, "node", "-")
        event = getattr(record, "event", None)
        fields = getattr(record, "fields", None) or {}
        if event:
            body = f"{event:<14} " + " ".join(f"{k}={_short(v)}" for k, v in fields.items())
        else:
            body = record.getMessage()
        line = f"{ts} {record.levelname:<5} {node:<8} {body.rstrip()}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "node": getattr(record, "node", None),
        }
        event = getattr(record, "event", None)
        if event:
            data["event"] = event
            for key, value in (getattr(record, "fields", None) or {}).items():
                data.setdefault(key, value)
        else:
            data["msg"] = record.getMessage()
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str)


def setup_logging(level: str = "INFO", json_lines: bool = False,
                  log_file: Optional[str] = None) -> None:
    """Configure the ``meshweaver`` logger once per process."""
    root = logging.getLogger(ROOT_LOGGER)
    root.setLevel(level.upper())
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    formatter: logging.Formatter = JsonFormatter() if json_lines else TextFormatter()
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    for handler in handlers:
        handler.setFormatter(formatter)
        root.addHandler(handler)
    root.propagate = False
