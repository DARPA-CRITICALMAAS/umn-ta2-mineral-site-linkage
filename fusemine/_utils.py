import os
import logging
from datetime import datetime

class DefaultLogger:
    def __init__(self):
        self.logger = logging.getLogger('FuseMine')

    def configure(self, level):
        self.set_level(level)
        self._add_handler()

    def set_level(self, level):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if level in levels:
            self.logger.setLevel(level)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def _add_handler(self):
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        self.logger.addHandler(sh)

        fh = logging.FileHandler(f'~/{os.path.dirname(os.path.realpath(__file__))}/logs/fusemine_{datetime.timestamp(datetime.now())}.log')
        fh.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        self.logger.addHandler(fh)