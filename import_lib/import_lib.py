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
import datetime
import json
import logging
import os
import time
from typing import Any

from confluent_kafka import Producer, KafkaException
from rfc3339 import rfc3339

from .logger._logger import get_logger, init_logging


class ImportLib:
    def __init__(self):
        if os.getenv("DEBUG", "False").lower() == 'true':
            level = 'debug'
        else:
            level = 'info'
        init_logging(level)
        self.__logger = get_logger(__name__.rsplit(".", 1)[-1].replace("_", ""))
        self.__logger.info("Log Level: " + level)

        self.__import_id = os.getenv("IMPORT_ID", "unknown")
        self.__logger.info("Import ID: " + self.__import_id)

        self.__kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost")
        self.__logger.info("Kafka Bootstrap: " + self.__kafka_bootstrap)

        self.__kafka_topic = os.getenv("KAFKA_TOPIC", "import-lib")
        self.__logger.info("Kafka Topic: " + self.__kafka_topic)

        self.__config = json.loads(
            os.getenv("CONFIG", '{}'))
        self.__logger.info("Config: " + str(self.__config))

        self.__producer = Producer({'bootstrap.servers': self.__kafka_bootstrap})

    def put(self, date_time: datetime, value: dict) -> None:
        '''
        Import a single data point. Writes asynchronously, unless the write queue is full.
        :param date_time: Datetime of the data in UTC
        :param value: value to import
        :return: None
        '''
        data = {
            "import_id": self.__import_id,
            "time": rfc3339(date_time, utc=True, use_system_timezone=False),
            "value": value
        }
        queued = False
        while not queued:
            try:
                self.__producer.produce(self.__kafka_topic, key=self.__import_id, value=json.dumps(data))
                queued = True
            except (KafkaException, BufferError) as e:
                self.__logger.warning("Could not queue kafka message, will retry in 1s. Error: " + str(e))
                time.sleep(1)


    def get_config(self, key: str, default: Any) -> Any:
        '''
        Get a config value by providing a key
        :param key: config value identifier
        :param default: default value, in case the config is not set
        :return: the config value or the default value
        '''
        if key in self.__config:
            return self.__config[key]
        return default

    def get_logger(self, name: str) -> logging.Logger:
        return get_logger(name.rsplit(".", 1)[-1].replace("_", ""))
