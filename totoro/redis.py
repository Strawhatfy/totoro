# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
from datetime import timedelta
from functools import partial

from tornado import gen
from tornado import ioloop
from tornadoredis import Client, ConnectionPool
from tornadoredis.exceptions import ResponseError
from tornadoredis.pubsub import SocketIOSubscriber
from redis import StrictRedis

from totoro.base import TaskConsumerBase, WaitForResultTimeoutError


LOGGER = logging.getLogger(__name__)


class RedisClient(Client):
    @gen.engine
    def _consume_bulk(self, tail, callback=None):
        response = yield gen.Task(self.connection.read, int(tail) + 2)
        if isinstance(response, Exception):
            raise response
        if not response:
            raise ResponseError('EmptyResponse')
        else:
            # We don't cast try to convert to unicode here as the response
            # may not be utf-8 encoded, for example if using msgpack as a
            # serializer
            # response = to_unicode(response)
            response = response[:-2]
        callback(response)


class RedisConsumeDelegate(object):
    def __init__(self, key, callback, subscriber, io_loop=None, oneshot=True):
        self._key = key
        self._callback = callback
        self._subscriber = subscriber
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._oneshot = oneshot
        self._subscribed = False
        self._consume_callbacks = list()

    @property
    def subscribed(self):
        return self._subscribed

    def _update_subscribe_state(self, future):
        self._subscribed = future.result()

    def consume(self, callback=None):
        if self._subscribed:
            callback()
            return
        if callback:
            self._consume_callbacks.append(callback)
        self._io_loop.add_future(
            gen.Task(self._subscriber.subscribe, self._key, self),
            lambda future: self._update_subscribe_state)

    def cancel(self):
        del self._consume_callbacks[:]
        self._subscriber.unsubscribe(self._key, self)
        self._subscribed = False

    def on_message(self, msg):
        try:
            if self._consume_callbacks:
                [cb() for cb in self._consume_callbacks]
                del self._consume_callbacks[:]
            if self._callback:
                self._callback(msg)
        finally:
            if self._oneshot:
                self.cancel()


class RedisConsumer(TaskConsumerBase):
    def __init__(self, **kwargs):
        super(RedisConsumer, self).__init__(**kwargs)
        self.redis_client = StrictRedis(
            host=self.backend.connparams['host'],
            port=self.backend.connparams['port'],
            password=self.backend.connparams['password'],
            db=self.backend.connparams['db'])
        self.subscriber = self.create_redis_subscriber()

    def _wait_for(self, task_id, callback, wait_timeout):
        key = self.backend.get_key_for_task(task_id)
        consume_delegate = RedisConsumeDelegate(key, callback, self.subscriber, self.io_loop)
        if wait_timeout:
            timeout = self.io_loop.add_timeout(
                timedelta(milliseconds=wait_timeout),
                partial(consume_delegate.on_message, WaitForResultTimeoutError(wait_timeout)))
            consume_delegate.consume(callback=lambda: self.io_loop.remove_timeout(timeout))

        # try to get the result before subscribed
        def _check_subscribed():
            result = self.redis_client.get(key)
            if result:
                LOGGER.info('Get the result before redis subscribed: key={0}.'.format(key))
                consume_delegate.on_message(result)
            elif not consume_delegate.subscribed:
                self.io_loop.add_timeout(timedelta(seconds=1), _check_subscribed)
        self.io_loop.add_callback(_check_subscribed)

    def create_redis_subscriber(self):
        connection_pool = ConnectionPool(
            max_connections=1024,
            host=self.backend.connparams['host'],
            port=self.backend.connparams['port'],
            io_loop=self.io_loop)
        redis_client = RedisClient(
            password=self.backend.connparams['password'],
            selected_db=self.backend.connparams['db'],
            connection_pool=connection_pool,
            io_loop=self.io_loop)
        return SocketIOSubscriber(redis_client)
