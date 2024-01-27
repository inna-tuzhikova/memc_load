import shutil
from pathlib import Path
from typing import Generator

import pytest
from memcache import Client

from memc_load.loader.loader import MemcLoader


@pytest.fixture(scope='function')
def test_log() -> Generator[Path, None, None]:
    tmp_path = get_tmp_test_file_by_name('test.tsv.gz')
    yield tmp_path
    tmp_path.unlink(missing_ok=True)


@pytest.fixture(scope='function')
def invalid_apps() -> Generator[Path, None, None]:
    tmp_path = get_tmp_test_file_by_name('invalid_apps.tsv.gz')
    yield tmp_path
    tmp_path.unlink(missing_ok=True)


@pytest.fixture(scope='function')
def invalid_fmt() -> Generator[Path, None, None]:
    tmp_path = get_tmp_test_file_by_name('invalid_fmt.tsv.gz')
    yield tmp_path
    tmp_path.unlink(missing_ok=True)


@pytest.fixture(scope='function')
def invalid_latitude() -> Generator[Path, None, None]:
    tmp_path = get_tmp_test_file_by_name('invalid_latitude.tsv.gz')
    yield tmp_path
    tmp_path.unlink(missing_ok=True)


@pytest.fixture(scope='function')
def invalid_longitude() -> Generator[Path, None, None]:
    tmp_path = get_tmp_test_file_by_name('invalid_longitude.tsv.gz')
    yield tmp_path
    tmp_path.unlink(missing_ok=True)


@pytest.fixture(scope='function')
def invalid_device_type() -> Generator[Path, None, None]:
    tmp_path = get_tmp_test_file_by_name('invalid_device_type.tsv.gz')
    yield tmp_path
    tmp_path.unlink(missing_ok=True)


def get_tmp_test_file_by_name(filename: str):
    path = (Path(__file__).parent / 'data' / filename).absolute()
    tmp_path = path.with_name('tmp_' + path.name)
    shutil.copy(str(path), str(tmp_path))
    return tmp_path


@pytest.fixture(scope='function')
def memcached_urls() -> dict[str, str]:
    return dict(
        idfa='memcached_idfa:11211',
        gaid='memcached_gaid:11211',
        adid='memcached_adid:11211',
        dvid='memcached_dvid:11211',
    )


@pytest.fixture(scope='function')
def memcached_clients(
    memcached_urls: dict[str, str]
) -> Generator[dict[str, Client], None, None]:
    clients = {
        k: Client([v])
        for k, v in memcached_urls.items()
    }
    for client in clients.values():
        client.flush_all()
    yield clients
    for client in clients.values():
        client.flush_all()


@pytest.fixture(scope='function')
def loader(memcached_urls: dict[str, str]) -> MemcLoader:
    return MemcLoader(memcached_urls)
