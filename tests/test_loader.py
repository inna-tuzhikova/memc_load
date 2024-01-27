from pathlib import Path

from memcache import Client

from memc_load.loader import appsinstalled_pb2
from memc_load.loader.loader import MemcLoader


def test_idfa_key(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    test_log: Path
):
    loader.load([test_log])
    client = memcached_clients['idfa']
    res = client.get_stats('items')
    assert len(res) > 0
    cached = client.get('idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c')
    assert cached is not None


def test_gaid_key(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    test_log: Path
):
    loader.load([test_log])
    client = memcached_clients['gaid']
    cached = client.get('gaid:3261cf44cbe6a00839c574336fdf49f6')
    assert cached is not None


def test_adid_key(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    test_log: Path
):
    loader.load([test_log])
    client = memcached_clients['adid']
    cached = client.get('adid:ca468fbb41ae6bd0b75fde1246a89bd1')
    assert cached is not None


def test_dvid_key(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    test_log: Path
):
    loader.load([test_log])
    client = memcached_clients['dvid']
    cached = client.get('dvid:94584df26efb6afd43b30609328f3d75')
    assert cached is not None


def test_log_renamed(
    loader: MemcLoader,
    test_log: Path
):
    loader.load([test_log])
    assert not test_log.is_file()
    renamed = test_log.with_name('.' + test_log.name)
    assert renamed.is_file()


def test_serialization(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    test_log: Path
):
    loader.load([test_log])
    client = memcached_clients['dvid']
    cached = client.get('dvid:94584df26efb6afd43b30609328f3d75')
    unpacked = appsinstalled_pb2.UserApps()
    unpacked.ParseFromString(cached)
    assert unpacked.lat == 165.364801883
    assert unpacked.lon == -67.9991374849
    assert unpacked.apps == [
        4046, 5305, 7503, 1192, 1354, 4875, 6216, 529,
        4067, 7003, 8900, 3945, 9099, 9054, 9322, 173,
        739, 3807, 5133, 8353, 4358, 2781, 6015, 5538
    ]


def test_invalid_log_format(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    invalid_fmt: Path
):
    loader.load([invalid_fmt])
    client = memcached_clients['idfa']
    cached = client.get('idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c')
    assert cached is None


def test_invalid_latitude(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    invalid_latitude: Path
):
    loader.load([invalid_latitude])
    client = memcached_clients['idfa']
    cached = client.get('idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c')
    assert cached is None


def test_invalid_longitude(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    invalid_longitude: Path
):
    loader.load([invalid_longitude])
    client = memcached_clients['idfa']
    cached = client.get('idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c')
    assert cached is None


def test_invalid_apps(
    memcached_clients: dict[str, Client],
    loader: MemcLoader,
    invalid_apps: Path
):
    loader.load([invalid_apps])
    client = memcached_clients['idfa']
    cached = client.get('idfa:e7e1a50c0ec2747ca56cd9e1558c0d7c')
    unpacked = appsinstalled_pb2.UserApps()
    unpacked.ParseFromString(cached)
    assert len(unpacked.apps) == 0
