import logging
import sys
import time
from pathlib import Path

from loader.logger import init_logging
from loader.loader import MemcLoader


def run_loader(
    log: Path | None,
    dry: bool,
    pattern: str,
    idfa: str,
    gaid: str,
    adid: str,
    dvid: str
):
    init_logging(log, dry)
    info_logger = logging.getLogger(__name__)
    info_logger.info(
        'MemcLoad started with options: '
        'log=%s, dry=%s, pattern=%s, idfa=%s, gaid=%s, adid=%s, dvid=%s',
        log, dry, pattern, idfa, gaid, adid, dvid
    )
    device_memc = dict(idfa=idfa, gaid=gaid, adid=adid, dvid=dvid)
    begin = time.time()
    memc_loader = MemcLoader(pattern, device_memc)
    try:
        memc_loader.load()
    except Exception as e:
        info_logger.exception('Unexpected error: %s', e)
        sys.exit(1)
    else:
        elapsed = time.time() - begin
        info_logger.info('Elapsed time: %0.3f sec', elapsed)
