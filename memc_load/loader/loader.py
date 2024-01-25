from collections import namedtuple
import glob
import gzip
import logging

import memcache
from loader import appsinstalled_pb2

from loader.util import dot_rename


logger = logging.getLogger(__name__)
AppsInstalled = namedtuple(
    'AppsInstalled',
    ['dev_type', 'dev_id', 'lat', 'lon', 'apps']
)


class MemcLoader:
    _NORMAL_ERR_RATE = 0.01

    def __init__(self, pattern: str, device_memc: dict[str, str]):
        self._pattern = pattern
        self._device_memc = device_memc

    def load(self):
        for fn in sorted(glob.glob(self._pattern)):
            processed = 0
            errors = 0
            logger.info('Processing %s', fn)
            gzipped_log = gzip.open(fn, 'rt')
            for idx, line in enumerate(gzipped_log):
                line = line.strip()
                if not line:
                    continue
                appsinstalled = self._parse_appsinstalled(line)
                if not appsinstalled:
                    errors += 1
                    continue
                memc_addr = self._device_memc.get(appsinstalled.dev_type)
                if not memc_addr:
                    errors += 1
                    logger.error(
                        'Unknown device type: %s',
                        appsinstalled.dev_type
                    )
                    continue

                ok = self._insert_appsinstalled(memc_addr, appsinstalled)
                if ok:
                    processed += 1
                else:
                    errors += 1
            if not processed:
                gzipped_log.close()
                dot_rename(fn)
                continue

            err_rate = float(errors) / processed
            if err_rate < self._NORMAL_ERR_RATE:
                logger.info('Success: acceptable error rate (%s).', err_rate)
            else:
                logger.error(
                    'Fail: high error rate (%s > %s).',
                    err_rate, self._NORMAL_ERR_RATE
                )
            gzipped_log.close()
            dot_rename(fn)

    def _parse_appsinstalled(self, line):
        line_parts = line.strip().split('\t')
        if len(line_parts) < 5:
            return
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            return
        try:
            apps = [int(a.strip()) for a in raw_apps.split(',')]
        except ValueError:
            apps = [int(a.strip()) for a in raw_apps.split(',') if a.isidigit()]
            logging.info('Not all user apps are digits: `%s`', line)
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info('Invalid geo coordinates: `%s`', line)
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)

    def _insert_appsinstalled(self, memc_addr, appsinstalled):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = '%s:%s' % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        # @TODO persistent connection
        # @TODO retry and timeouts!
        try:
            logging.debug(
                '%s - %s -> %s',
                memc_addr, key, str(ua).replace('\n', ' ')
            )
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
        except Exception as e:
            logging.exception('Cannot write to memc %s: %s', memc_addr, e)
            return False
        return True
