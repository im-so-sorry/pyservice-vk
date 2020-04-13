import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Tuple

import ujson as json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.fetcher import ConsumerRecord

from vk_service import settings

system_logger = logging.getLogger("system")


@dataclass
class KafkaMessage:
    content: Dict = field(default=dict)
    meta: Dict = field(default=dict)

    @classmethod
    def from_dict(cls, data: Dict) -> "KafkaMessage":
        return cls(**data)

    def get_dict(self) -> Dict:
        return {
            "content": self.content,
            "meta": self.meta,
        }


class KafkaClient:
    producer = None
    consumer = None

    def __init__(
        self,
        topics: List[str] = None,
        *,
        bootstrap_servers: List[str] = None,
        group_id: str = None,
        auto_offset_reset: str = "earliest",
        session_timeout_ms: int = 6000,
        result_topics: List[str] = ("result_topics",),
        reproduce_topics: List[str] = None,
        produce_timeout: int = 60,
        enable_auto_commit: bool = False,
        enable_reproduce: bool = True,
    ):

        if reproduce_topics is None:
            """
            If reproduce_topics not defined, return message to topics
            """
            reproduce_topics = topics

        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_HOSTS
        self.group_id = group_id or settings.KAFKA_GROUP_ID or "client"
        self.topics = topics
        self.auto_offset_reset = auto_offset_reset
        self.session_timeout_ms = session_timeout_ms

        self._result_topic_id = 0
        self.result_topics = result_topics
        self._reproduce_topic_id = 0
        self.reproduce_topics = reproduce_topics

        self.produce_timeout = produce_timeout
        self.enable_auto_commit = enable_auto_commit
        self.enable_reproduce = enable_reproduce

    def close(self):
        if self.producer is not None:
            try:
                self.producer.close()
            except:
                pass
        if self.consumer is not None:
            try:
                self.consumer.close()
            except:
                pass

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @staticmethod
    def get_value_from_headers(headers: List, key: str, default: Any = None) -> Optional[Any]:
        for h in headers:
            if h[0] == key:
                return h[1]

        return default

    def get_producer(self) -> KafkaProducer:
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
        return self.producer

    def get_consumer(self) -> KafkaConsumer:
        if self.consumer is None:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                session_timeout_ms=self.session_timeout_ms,
                enable_auto_commit=self.enable_auto_commit,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )

        return self.consumer

    def get_result_topic(self):
        topic = self.result_topics[self._result_topic_id % len(self.result_topics)]
        self._result_topic_id = (self._result_topic_id + 1) % len(self.result_topics)
        return topic

    def send(self, data: Dict, topic: str = None, headers=None) -> Optional[bool]:

        if topic is None:
            topic = self.get_result_topic()

        producer = self.get_producer()

        try:
            future = producer.send(topic, value=data, headers=headers)
            result = future.get(timeout=self.produce_timeout)
            logging.info(f"Success message sending: {data}")
        except Exception as e:
            logging.error(f"Error on sending message to {topic}: {e}")
            return False
        return True

    def success_result(self, msg, consumer, result):
        """
         :param msg: ConsumerRecord
         :param consumer: KafkaConsumer
         :param result: Unwrapperd success result
         :return:
         """
        if not self.enable_auto_commit:
            options = {TopicPartition(msg.topic, msg.partition): OffsetAndMetadata(msg.offset + 1, None)}
            consumer.commit(options)

    def failure_result(self, msg, consumer, result):
        pass

    def listen(self, message_handler: Callable[[ConsumerRecord], Tuple[Any, bool]] = None, rise_exception=False):
        """
        Start topic listening
        :param message_handler:
        :return:
        """
        system_logger.info(f"Start listen")
        system_logger.info(f"Get consumer")

        consumer = self.get_consumer()

        if message_handler is None:
            message_handler = self.process_message

        system_logger.info(f"Start msg in consumer")
        for msg in consumer:
            try:
                result, ok = message_handler(msg)

                if ok:
                    self.success_result(msg, consumer, result)
                else:
                    self.failure_result(msg, consumer, result)
            except Exception as e:
                if rise_exception:
                    raise e
                else:
                    logging.error(str(e))

    def process_message(self, msg: ConsumerRecord) -> Tuple[Any, bool]:
        raise NotImplementedError
