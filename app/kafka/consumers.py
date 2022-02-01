import asyncio
import datetime
import logging
import threading
from abc import abstractmethod, ABC
from confluent_kafka import DeserializingConsumer, Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
from fastapi.encoders import jsonable_encoder

import app.db_utils.mongo_utils as database
import app.kafka.producers as producers
import app.settings as config
from app.db_utils.advanced_scheduler import async_repeat_deco
from app.models import UserAuthTransfer, User, UserAuthTransferReply


class GenericConsumer(ABC):
    bootstrap_servers = config.broker_settings.broker

    @property
    @abstractmethod
    def group_id(self):
        ...

    @property
    @abstractmethod
    def auto_offset_reset(self):
        ...

    @property
    @abstractmethod
    def auto_commit(self):
        ...

    @property
    @abstractmethod
    def topic(self):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def dict_to_model(self, map, ctx):
        ...

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def consume_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    @abstractmethod
    def _consume_data(self):
        ...

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None, normal_consumer=False):
        if not normal_consumer:
            json_deserializer = JSONDeserializer(self.schema,
                                                 from_dict=self.dict_to_model)
            string_deserializer = StringDeserializer('utf_8')

            consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'key.deserializer': string_deserializer,
                             'value.deserializer': json_deserializer,
                             'group.id': self.group_id,
                             'auto.offset.reset': self.auto_offset_reset,
                             'enable.auto.commit': self.auto_commit,
                             'allow.auto.create.topics': True}
        else:
            consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'group.id': self.group_id,
                             'auto.offset.reset': self.auto_offset_reset,
                             'enable.auto.commit': self.auto_commit,
                             'allow.auto.create.topics': True}

        self._loop = loop or asyncio.get_event_loop()
        if not normal_consumer:
            self._consumer = DeserializingConsumer(consumer_conf)
        else:
            self._consumer = Consumer(consumer_conf)
        self._cancelled = False
        self._consumer.subscribe([self.topic])
        self._polling_thread = threading.Thread(target=self._consume_data)


class UserAuthConsumer(GenericConsumer):
    @property
    def group_id(self):
        return 'my_group_user'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'user-auth'

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Auth Request",
  "description": "User Auth request data",
  "type": "object",
  "properties": {
    "user_id": {
      "description": "User's Discord id",
      "type": "string"
    },
    "username": {
      "description": "User's nick",
      "type": "string"
    }
  },
  "required": [
    "user_id",
    "username"
  ]
}"""

    def dict_to_model(self, map, ctx):
        if map is None:
            return None

        return UserAuthTransfer.parse_obj(map)

    def _rollback_data(self, id):
        pass

    async def _check_existing_user(self, user_id):
        return await database.mongo.db[User.collection_name].find_one({'user_id': user_id})

    async def _create_new_user(self, user_model: User):
        await database.mongo.db[User.collection_name].insert_one(jsonable_encoder(user_model))

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                # headers: [0] channel_id, [1] web_site, [2] category
                user_auth: UserAuthTransfer = msg.value()
                if user_auth is not None:
                    existing = asyncio.run_coroutine_threadsafe(self._check_existing_user(user_auth.user_id), self._loop).result(20)
                    if existing:
                        existing_model = User.parse_obj(existing)
                        authorized = True
                        if existing_model.ban_period is not None and existing_model.ban_period.astimezone(
                                datetime.timezone.utc) > datetime.datetime.now(datetime.timezone.utc):

                            authorized = False

                            user_auth_transfer_reply = UserAuthTransferReply(**existing_model.dict(),
                                                                             authorized=authorized)
                            producers.user_auth_producer.produce(msg.key(), user_auth_transfer_reply, msg.headers())
                            self._consumer.commit(msg)
                        else:
                            producers.user_limit_auth_producer.produce(msg.key(), user_auth, msg.headers())
                            self._consumer.commit(msg)
                    else:
                        new_user_model = User(**user_auth.dict())
                        asyncio.run_coroutine_threadsafe(
                            self._create_new_user(new_user_model), self._loop).result(20)

                        user_auth_transfer_reply = UserAuthTransferReply(**new_user_model.dict(), authorized=True)
                        producers.user_auth_producer.produce(msg.key(), user_auth_transfer_reply, headers=msg.headers())
                        self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                except:
                    pass

                # break

        self._consumer.close()


class UserLimitAuthReplyConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group_user'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'user-limit-auth-reply'

    @property
    def schema(self):
        return None

    def dict_to_model(self, map, ctx):
        return None

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                # headers: [0] channel_id, [1] web_site, [2] category
                search_count = int(msg.value())

                async def evaluate_user_limit_threshold():
                    try:
                        user = await database.mongo.db[User.collection_name].find_one({'user_id': msg.headers()[4][1].decode('utf-8')})

                        if user is not None:
                            user_model = User.parse_obj(user)
                            if search_count is not None and user.get('max_research') is not None and user.get(
                                    'max_research') < search_count:
                                producers.user_auth_producer.produce(msg.key().decode('utf-8'),
                                                                     UserAuthTransferReply(**user_model.dict(),
                                                                                           authorized=False), msg.headers())
                            else:
                                producers.user_auth_producer.produce(msg.key().decode('utf-8'),
                                                                     UserAuthTransferReply(**user_model.dict(),
                                                                                           authorized=True), msg.headers())
                    except Exception as exc:
                        logging.exception('')

                asyncio.run_coroutine_threadsafe(evaluate_user_limit_threshold(), loop=self._loop).result(10)
                self._consumer.commit(msg)

            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                except:
                    pass

                # break

        self._consumer.close()


user_auth_consumer: UserAuthConsumer
user_auth_limit_reply_consumer: UserLimitAuthReplyConsumer

def init_consumers():
    @async_repeat_deco(3, 3, always_reschedule=True, store='alternative')
    async def init_user_auth_consumer(_):
        global user_auth_consumer
        user_auth_consumer = UserAuthConsumer(asyncio.get_running_loop())
        user_auth_consumer.consume_data()

    @async_repeat_deco(3, 3, always_reschedule=True, store='alternative')
    async def init_user_auth_limit_reply_consumer(_):
        global user_auth_limit_reply_consumer
        user_auth_limit_reply_consumer = UserLimitAuthReplyConsumer(asyncio.get_running_loop(), normal_consumer=True)
        user_auth_limit_reply_consumer.consume_data()

    asyncio.run_coroutine_threadsafe(init_user_auth_consumer('user_auth_consumer'), loop=asyncio.get_running_loop())
    asyncio.run_coroutine_threadsafe(init_user_auth_limit_reply_consumer('user_auth_limit_reply_consumer'), loop=asyncio.get_running_loop())


def close_consumers():
    user_auth_consumer.close()
    user_auth_limit_reply_consumer.close()

