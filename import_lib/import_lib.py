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
from typing import Any, Optional, Dict, Tuple

from confluent_kafka import Producer, Consumer, KafkaException, cimpl
from rfc3339 import rfc3339

from .logger._logger import get_logger as internal_get_logger, init_logging


def get_logger(name: str) -> logging.Logger:
    return internal_get_logger(name.rsplit(".", 1)[-1].replace("_", ""))


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
                self.__logger.warning("Could not queue kafka message, flushing and retrying in 1s. Error: " + str(e))
                self.__producer.flush()
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

    def get_last_published_datetime(self) -> Tuple[Optional[datetime.datetime], Optional[Dict]]:
        '''
        Returns the last published timestamp and message or None, if no message has been published yet. This will
        only be the least old timestamp, if you strictly publish in order! If you published multiple values with the
        same timestamp, there are no guarantees for which message you will receive. You should publish in order and
        check this timestamp before importing historic data to prevent re-imports.
        :return: Latest known timestamp
        '''
        consumer = Consumer(
            {'bootstrap.servers': self.__kafka_bootstrap, 'group.id': self.__import_id})
        partitions = consumer.list_topics(topic=self.__kafka_topic).topics[self.__kafka_topic].partitions.keys()
        self.__logger.debug("Found " + str(len(partitions)) + " partition(s) of topic " + self.__kafka_topic)
        num_messages = 0
        topic_partitions = []
        for partition in partitions:
            high_offset = consumer.get_watermark_offsets(cimpl.TopicPartition(self.__kafka_topic, partition=partition))[
                1]
            self.__logger.debug("Largest offset of partition " + str(partition) + " is " + str(high_offset))
            if high_offset > 0:  # Ignore partitions without data
                topic_partitions.append(
                    cimpl.TopicPartition(self.__kafka_topic, partition=partition, offset=high_offset - 1))
                num_messages += 1

        consumer.assign(topic_partitions)

        self.__logger.debug("Consuming last " + str(num_messages) + " message(s) (1 from each active partition)")
        msgs = consumer.consume(num_messages=num_messages)  # Get last messages from all partitions
        consumer.close()

        greatest_datetime = datetime.datetime.fromtimestamp(0)
        matching_value = {}
        for msg in msgs:
            value = json.loads(msg.value())
            if 'time' not in value:
                self.__logger.warning("time field missing in message, is someone else using this topic? Ignoring "
                                      "message")
                continue
            if 'value' not in value or not isinstance(value['value'], Dict):
                self.__logger.warning("value field missing or malformed in message, is someone else using this topic? "
                                      "Ignoring message")
                continue

            self.__logger.debug("Got timestamp: " + str(value["time"]))
            try:
                date_time = datetime.datetime.strptime(value["time"], "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                self.__logger.warning("time field not in rfc3339 format, is someone else using this topic? Ignoring "
                                      "message")
                continue
            if date_time > greatest_datetime:
                greatest_datetime = date_time
                matching_value = value["value"]

        if greatest_datetime.timestamp() > 0:
            return greatest_datetime, matching_value
        return None, None
