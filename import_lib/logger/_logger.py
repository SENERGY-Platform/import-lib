"""
   Copyright 2020 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

__all__ = ('get_logger', 'init_logging')

import logging.handlers
import threading

lock = threading.RLock()

logging_levels = {
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'debug': logging.DEBUG
}

color_code = {
    'CRITICAL': '\033[41m',
    'ERROR': '\033[31m',
    'WARNING': '\033[33m',
    'DEBUG': '\033[94m',
}


class LoggerError(Exception):
    pass


class ColorFormatter(logging.Formatter):
    def format(self, record):
        s = super().format(record)
        if record.levelname in color_code:
            return '{}{}{}'.format(color_code[record.levelname], s, '\033[0m')
        return s


msg_fmt = '%(asctime)s - %(levelname)s: [%(name)s] %(message)s'
date_fmt = '%m.%d.%Y %I:%M:%S %p'
standard_formatter = logging.Formatter(fmt=msg_fmt, datefmt=date_fmt)
color_formatter = ColorFormatter(fmt=msg_fmt, datefmt=date_fmt)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(standard_formatter)

logger = logging.getLogger('import')
logger.propagate = False
logger.addHandler(stream_handler)


def init_logging(level: str, colored: bool = True) -> None:
    if not level in logging_levels.keys():
        err = "unknown log level '{}'".format(level)
        raise LoggerError(err)
    if colored:
        stream_handler.setFormatter(color_formatter)
    logger.setLevel(logging_levels[level])


def get_logger(name: str) -> logging.Logger:
    return logger.getChild(name)
