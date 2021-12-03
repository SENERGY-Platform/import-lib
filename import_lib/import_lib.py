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
from typing import Any, Optional, Dict, Tuple, List

from confluent_kafka import Producer, Consumer, KafkaException, cimpl
from confluent_kafka.admin import AdminClient, ConfigResource
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

        self.__configure_topic()

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
                self.__producer.produce(self.__kafka_topic, key=self.__import_id, value=json.dumps(data, ensure_ascii=False))
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

        :return: Latest known timestamp and message
        '''
        last = self.get_last_n_messages(1)
        if last is None:
            return None, None
        if len(last) == 1:
            return last[0]

        greatest_datetime = datetime.datetime.fromtimestamp(0)
        matching_value = {}
        for date_time, value in last:
            if date_time > greatest_datetime:
                greatest_datetime = date_time
                matching_value = value
        return greatest_datetime, matching_value

    def get_last_n_messages(self, n: int) -> Optional[List[Tuple[datetime.datetime, Dict]]]:
        '''
        Returns the last n published timestamps and messages or None, if no message has been published yet.
        If the configured topic has more than one partition, you will receive more messages than requested
        (at most partitions * n). You might receive less messages than requested, if the broker has cleared messages.

        :return: List of tuples with timestamp and message or None if no message has been published yet
        '''

        consumer = Consumer(
            {'bootstrap.servers': self.__kafka_bootstrap, 'group.id': self.__import_id})
        partitions = consumer.list_topics(topic=self.__kafka_topic).topics[self.__kafka_topic].partitions.keys()
        self.__logger.debug("Found " + str(len(partitions)) + " partition(s) of topic " + self.__kafka_topic)
        num_messages = 0
        topic_partitions = []
        for partition in partitions:
            high_low_offset = consumer.get_watermark_offsets(
                cimpl.TopicPartition(self.__kafka_topic, partition=partition))
            high_offset = high_low_offset[1]
            low_offset = high_low_offset[0]
            available_messages = high_offset - low_offset
            self.__logger.debug(
                "Low/High offset of partition " + str(partition) + " is " + str(low_offset) + "/" + str(high_offset))
            if high_offset > 0:  # Ignore partitions without data
                if available_messages >= n:
                    offset = high_offset - n
                    num_messages += n
                else:
                    offset = low_offset
                    num_messages += available_messages
                partition = cimpl.TopicPartition(self.__kafka_topic, partition=partition, offset=offset)
                topic_partitions.append(partition)
                self.__logger.debug("Setting offset of partition " + str(partition))

        if len(topic_partitions) == 0:  # No partition has any data
            return None

        consumer.assign(topic_partitions)
        consumer.commit(offsets=topic_partitions)
        tuples = []
        consumed_messages = 0
        batch_size = 10000
        self.__logger.debug("Consuming last " + str(num_messages) + " message(s)")

        while consumed_messages < num_messages:
            if consumed_messages + batch_size <= num_messages:
                to_consume = batch_size
            else:
                to_consume = num_messages - consumed_messages

            consumed_messages += to_consume
            self.__logger.debug("Consuming batch of " + str(to_consume) + " messages")
            msgs = consumer.consume(num_messages=to_consume, timeout=30)

            for msg in msgs:
                value = json.loads(msg.value())
                if 'time' not in value:
                    self.__logger.warning("time field missing in message, is someone else using this topic? Ignoring "
                                          "message")
                    continue
                if 'value' not in value or not isinstance(value['value'], Dict):
                    self.__logger.warning(
                        "value field missing or malformed in message, is someone else using this topic? "
                        "Ignoring message")
                    continue

                try:
                    date_time = datetime.datetime.strptime(value["time"], "%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    self.__logger.warning(
                        "time field not in rfc3339 format, is someone else using this topic? Ignoring "
                        "message")
                    continue
                tuples.append((date_time, value["value"]))

        consumer.close()
        return tuples

    def __configure_topic(self):
        admin = AdminClient({'bootstrap.servers': self.__kafka_bootstrap})
        admin.create_topics([cimpl.NewTopic(topic=self.__kafka_topic, num_partitions=1)])
        admin.alter_configs([ConfigResource(restype=ConfigResource.Type.TOPIC, name=self.__kafka_topic, set_config={
            "retention.bytes": 10000000,
            "retention.ms": -1,
        })])
