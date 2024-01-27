import gzip
import logging
from collections import namedtuple
from multiprocessing import Pool
from pathlib import Path

import memcache

from memc_load.loader import appsinstalled_pb2
from memc_load.loader.util import dot_rename

logger = logging.getLogger(__name__)
AppsInstalled = namedtuple(
    'AppsInstalled',
    ['dev_type', 'dev_id', 'lat', 'lon', 'apps']
)


class MemcLoader:
    _NORMAL_ERR_RATE = 0.01

    def __init__(self, device_memc: dict[str, str]):
        self._device_memc = device_memc
        self._memc_clients = None

    def load(self, logs: list[Path]) -> None:
        """Uploads files to Memcached with parallel processes"""
        with Pool() as pool:
            for path in pool.imap(self._load_file, sorted(logs)):
                dot_rename(path)
                logger.info('Renamed %s', path.name)

    def _load_file(self, path: Path) -> None:
        self._prep_clients()
        processed = 0
        errors = 0
        logger.info('Processing %s', path)
        gzipped_log = gzip.open(path, 'rt')
        for idx, line in enumerate(gzipped_log):
            line = line.strip()
            if not line:
                continue
            appsinstalled = self._parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            client = self._memc_clients.get(appsinstalled.dev_type)
            if not client:
                errors += 1
                logger.error(
                    'Unknown device type: %s',
                    appsinstalled.dev_type
                )
                continue

            success = self._insert_appsinstalled(client, appsinstalled)
            if success:
                processed += 1
            else:
                errors += 1
        if processed:
            err_rate = float(errors) / processed
            if err_rate < self._NORMAL_ERR_RATE:
                logger.info(
                    'Success %s: acceptable error rate (%s).',
                    path.name, err_rate
                )
            else:
                logger.error(
                    'Fail %s: high error rate (%s > %s).',
                    path, err_rate, self._NORMAL_ERR_RATE
                )
        gzipped_log.close()
        return path

    def _parse_appsinstalled(self, line: str) -> AppsInstalled | None:
        line_parts = line.strip().split('\t')
        if len(line_parts) < 5:
            return
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            return
        try:
            apps = [int(a.strip()) for a in raw_apps.split(',')]
        except ValueError:
            apps = [int(a.strip()) for a in raw_apps.split(',') if a.isdigit()]
            logging.info('Not all user apps are digits: `%s`', line)
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info('Invalid geo coordinates: `%s`', line)
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)

    def _insert_appsinstalled(
        self,
        client: memcache.Client,
        appsinstalled: AppsInstalled
    ) -> bool:
        success = False
        ua = appsinstalled_pb2.UserApps()
        try:
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            ua.apps.extend(appsinstalled.apps)
        except TypeError:
            return success
        key = '%s:%s' % (appsinstalled.dev_type, appsinstalled.dev_id)
        packed = ua.SerializeToString()
        try:
            logging.debug(
                '%s -> %s -> %s',
                client.servers[0].address, key, str(ua).replace('\n', ' ')
            )
            success = bool(client.set(key, packed))
        except Exception as e:
            logging.exception('Cannot write to memc %s', e)
            return success
        return success

    def _prep_clients(self) -> None:
        self._memc_clients = {
            k: memcache.Client([v], socket_timeout=5)
            for k, v in self._device_memc.items()
        }
