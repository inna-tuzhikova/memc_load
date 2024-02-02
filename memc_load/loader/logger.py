import logging
from pathlib import Path


def init_logging(log: Path | None, dry: bool) -> None:
    """Sets basic logging settings

    Args:
        log (Path | None): path to log file. If is None, stdout is used
        dry (bool): if True sets log level to DEBUG, INFO otherwise
    """
    logging.basicConfig(
        filename=log,
        level=logging.DEBUG if dry else logging.INFO,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
    )
