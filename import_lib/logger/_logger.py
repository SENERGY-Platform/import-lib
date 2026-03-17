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

import logging
import threading
import structlog

lock = threading.RLock()

logging_levels = {
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'debug': logging.DEBUG
}

class LoggerError(Exception):
    pass

logging.setLoggerClass(structlog.Logger)
logger = logging.getLogger('import')


def init_logging(level: str, project_name: str) -> None:
    logger.configure(organization_name='github.com/SENERGY-Platform', project_name=project_name, time_utc=True, logger_name=True)
    if not level in logging_levels.keys():
        err = "unknown log level '{}'".format(level)
        raise LoggerError(err)

    logger.setLevel(logging_levels[level])


def get_logger(name: str) -> logging.Logger:
    return logger.getChild(name)
