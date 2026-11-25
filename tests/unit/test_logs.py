import json
import logging

from meshweaver.logs import JsonFormatter, NodeLogger, TextFormatter, setup_logging


def _capture(formatter):
    records = []

    class Keep(logging.Handler):
        def emit(self, record):
            records.append(self.format(record))

    handler = Keep()
    handler.setFormatter(formatter)
    root = logging.getLogger("meshweaver")
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    return records, lambda: root.removeHandler(handler)


def test_json_event_has_node_event_and_fields():
    records, done = _capture(JsonFormatter())
    try:
        NodeLogger("node-3").child("membership").event(
            "peer.join", peer="a" * 40, port=9001)
    finally:
        done()
    data = json.loads(records[-1])
    assert data["node"] == "node-3"
    assert data["event"] == "peer.join"
    assert data["peer"] == "a" * 40 and data["port"] == 9001
    assert data["logger"] == "meshweaver.membership"


def test_text_event_shortens_node_ids():
    records, done = _capture(TextFormatter())
    try:
        NodeLogger("node-1").event("peer.leave", peer="b" * 40, reason="no reply")
    finally:
        done()
    line = records[-1]
    assert "node-1" in line and "peer.leave" in line
    assert "peer=bbbbbbbb " in line and "b" * 9 not in line
    assert "reason=no reply" in line


def test_plain_messages_still_work():
    records, done = _capture(JsonFormatter())
    try:
        NodeLogger("n").warning("disk %s", "full")
    finally:
        done()
    data = json.loads(records[-1])
    assert data["msg"] == "disk full" and data["level"] == "WARNING"


def test_setup_logging_writes_file(tmp_path):
    path = tmp_path / "node.log"
    setup_logging("INFO", json_lines=True, log_file=str(path))
    try:
        NodeLogger("n").event("node.ready", port=1)
        NodeLogger("n").debug("hidden")
    finally:
        for h in list(logging.getLogger("meshweaver").handlers):
            h.close()
            logging.getLogger("meshweaver").removeHandler(h)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["event"] == "node.ready"
