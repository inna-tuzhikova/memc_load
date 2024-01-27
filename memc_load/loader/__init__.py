import glob
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
    logs = list(Path(p) for p in glob.glob(pattern))
    begin = time.time()
    memc_loader = MemcLoader(device_memc)
    try:
        memc_loader.load(logs)
        # memc_loader.load()  # 6.212 sec

        # memc_loader.load_pp_proc_fn()  # 1.897 sec
        # memc_loader.load_pp_proc_fn_tpe()  # 2.557 sec
        # memc_loader.load_pp_proc_fn_t()  # 7.786 sec

        # memc_loader.load_ppe_proc_fn()  # 1.942 sec
        # memc_loader.load_ppe_proc_fn_tpe()  # 2.580 sec
        # memc_loader.load_ppe_proc_fn_t()  # 8.815 sec
    except Exception as e:
        info_logger.exception('Unexpected error: %s', e)
        sys.exit(1)
    else:
        elapsed = time.time() - begin
        info_logger.info('Elapsed time: %0.3f sec', elapsed)
