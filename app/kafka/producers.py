import asyncio
import threading
from abc import abstractmethod, ABC

from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

from app.models import UserAuthTransfer
import app.settings as config


class GenericProducer(ABC):
    bootstrap_servers = config.broker_settings.broker
    schema_registry_conf = {'url': config.broker_settings.schema_registry}

    # bootstrap_servers = 'localhost:9092'
    # schema_registry_conf = {'url': 'http://localhost:8081'}

    @abstractmethod
    def model_to_dict(self, obj, ctx):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def produce(self, id, value, headers) -> asyncio.Future:
        ...

    def _produce_data(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def produce_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None, client=None):
        schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        json_serializer = JSONSerializer(self.schema, schema_registry_client, to_dict=self.model_to_dict)

        producer_conf = {'bootstrap.servers': self.bootstrap_servers,
                         'key.serializer': StringSerializer('utf_8'),
                         'value.serializer': json_serializer
                         }
        self._loop = loop or asyncio.get_event_loop()
        self._producer = SerializingProducer(producer_conf)
        self._polling_thread = threading.Thread(target=self._produce_data)
        self._cancelled = False
        self.client = client


class UserAuthProducer(GenericProducer):
    topic = 'user-auth-reply'

    def model_to_dict(self, obj: UserAuthTransfer, ctx):
        if obj is None:
            return None

        return obj.dict()

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Auth reply",
  "description": "User Auth kafka reply",
  "type": "object",
  "properties": {
    "username": {
      "description": "Discord username",
      "type": "string"
    },
    "user_id": {
      "description": "Discord User identifier",
      "type": "string"
    },
    "authorized": {
      "description": "Whether the user is authorized",
      "type": "boolean"
    }
  }
}"""

    def produce(self, id, value, headers) -> asyncio.Future:
        result_fut = self._loop.create_future()

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
                self._loop.call_soon_threadsafe(result_fut.set_exception, KafkaException(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
                self._loop.call_soon_threadsafe(result_fut.set_result, msg)

        self._producer.produce(topic=self.topic, key=id, value=value, on_delivery=delivery_report, headers=headers)
        return result_fut


user_auth_producer: UserAuthProducer


def init_producers(client=None):
    global user_auth_producer
    user_auth_producer = UserAuthProducer(asyncio.get_running_loop(), client)
    user_auth_producer.produce_data()


def close_producers():
    user_auth_producer.close()
