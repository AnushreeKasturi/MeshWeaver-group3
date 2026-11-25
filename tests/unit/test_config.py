import json

import pytest

from meshweaver.config import NodeConfig, load_config, parse_address


def test_defaults_match_the_work_plan():
    c = NodeConfig()
    assert c.gossip_interval == 5.0
    assert c.gossip_fanout == 3
    assert c.stale_after == 15.0
    assert c.heartbeat_interval == 2.0
    assert c.heartbeat_max_misses == 3
    assert c.k == 20


@pytest.mark.parametrize("text, expected", [
    ("127.0.0.1:9000", ("127.0.0.1", 9000)),
    ("9001", ("127.0.0.1", 9001)),
    (" 10.0.0.5:80 ", ("10.0.0.5", 80)),
    (":9002", ("127.0.0.1", 9002)),
])
def test_parse_address(text, expected):
    assert parse_address(text) == expected


@pytest.mark.parametrize("text", ["host:abc", "host:0", "host:70000", ""])
def test_parse_address_rejects_bad_input(text):
    with pytest.raises(ValueError):
        parse_address(text)


def test_seeds_accept_strings_and_tuples():
    c = NodeConfig(seeds=["127.0.0.1:9000", ("10.0.0.1", 9001), ["h", "9002"]])
    assert c.seeds == [("127.0.0.1", 9000), ("10.0.0.1", 9001), ("h", 9002)]


@pytest.mark.parametrize("changes", [
    {"port": 70000},
    {"rpc_timeout": 0},
    {"gossip_fanout": -1},
    {"gossip_interval": 20.0},     # not shorter than stale_after
    {"log_level": "LOUD"},
])
def test_validation(changes):
    with pytest.raises(ValueError):
        NodeConfig(**changes)


def test_unknown_keys_rejected():
    with pytest.raises(ValueError, match="unknown config keys"):
        NodeConfig.from_dict({"prot": 9000})


def test_load_order_file_then_env_then_overrides(tmp_path):
    path = tmp_path / "node.json"
    path.write_text(json.dumps({"port": 9100, "gossip_interval": 4.0,
                                "seeds": ["127.0.0.1:9000"], "name": "from-file"}))
    env = {"MESHWEAVER_PORT": "9200", "MESHWEAVER_LOG_JSON": "true",
           "MESHWEAVER_RPC_TIMEOUT": "1.5", "UNRELATED": "x"}
    c = load_config(path, env=env, name="from-cli", port=None)
    assert c.name == "from-cli"         # override wins
    assert c.port == 9200               # env beats file; None override ignored
    assert c.gossip_interval == 4.0     # file beats default
    assert c.log_json is True
    assert c.rpc_timeout == 1.5
    assert c.seeds == [("127.0.0.1", 9000)]


def test_env_seed_list():
    c = load_config(env={"MESHWEAVER_SEEDS": "127.0.0.1:9000, 127.0.0.1:9001"})
    assert c.seeds == [("127.0.0.1", 9000), ("127.0.0.1", 9001)]


def test_config_file_must_be_object(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("[1, 2]")
    with pytest.raises(ValueError):
        load_config(path, env={})


def test_replace_and_round_trip():
    c = NodeConfig(name="a", seeds=["127.0.0.1:9000"])
    d = c.replace(port=9005, name=None)
    assert d.port == 9005 and d.name == "a"
    assert NodeConfig.from_dict(c.to_dict()) == c


def test_multicast_is_off_by_default_and_seeds_still_work():
    c = NodeConfig(seeds=["127.0.0.1:9000"])
    assert c.multicast_enabled is False
    assert c.multicast_group == "239.255.42.99"
    assert c.multicast_port == 9999
    assert c.multicast_interval == 5.0
    assert c.seeds == [("127.0.0.1", 9000)]


def test_multicast_settings_from_env_and_overrides():
    env = {"MESHWEAVER_MULTICAST_ENABLED": "true",
           "MESHWEAVER_MULTICAST_GROUP": "239.1.2.3",
           "MESHWEAVER_MULTICAST_PORT": "9998"}
    c = load_config(env=env, multicast_interval=2.0)
    assert c.multicast_enabled is True
    assert (c.multicast_group, c.multicast_port, c.multicast_interval) == ("239.1.2.3", 9998, 2.0)


@pytest.mark.parametrize("changes", [
    {"multicast_group": "192.168.1.10"},   # a normal address, not multicast
    {"multicast_group": "not-an-ip"},
    {"multicast_port": 0},
    {"multicast_port": 70000},
    {"multicast_interval": 0},
])
def test_multicast_validation(changes):
    with pytest.raises(ValueError):
        NodeConfig(**changes)
