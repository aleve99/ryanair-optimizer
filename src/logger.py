import logging, sys

__all__ = []

def _init_logger():
    logger = logging.getLogger('ryanair')
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s'
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

_init_logger()