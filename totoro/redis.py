# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
from datetime import timedelta
from functools import partial

from tornado import gen
from tornado import ioloop
from tornadoredis import Client
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
    SUBSCRIBER_INIT = 0
    SUBSCRIBER_SUBSCRIBING = 1
    SUBSCRIBER_SUBSCRIBED = 2

    def __init__(self, key, callback, subscriber, io_loop=None, oneshot=True):
        self._key = key
        self._callback = callback
        self._subscriber = subscriber
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._oneshot = oneshot
        self._subscriber_state = self.SUBSCRIBER_INIT
        self._consume_callbacks = list()

    @property
    def is_subscribed(self):
        return self._subscriber_state == self.SUBSCRIBER_SUBSCRIBED

    @property
    def is_subscribing(self):
        return self._subscriber_state == self.SUBSCRIBER_SUBSCRIBING

    def _set_subscriber_state(self, subscriber_state):
        self._subscriber_state = subscriber_state

    def _update_subscribe_state(self, future):
        if future.result():
            if self.is_subscribing:
                LOGGER.debug('The redis subscription success: {0}.'.format(self._key))
                self._set_subscriber_state(self.SUBSCRIBER_SUBSCRIBED)
            else:
                LOGGER.info('The redis subscription have been cancelled before '
                            'seeing success: {0}.'.format(self._key))
        else:
            LOGGER.warning('The redis subscription fails: {0}.'.format(self._key))

    def add_consume_callback(self, callback, *args, **kwargs):
        if callback:
            self._consume_callbacks.append(partial(callback, *args, **kwargs))

    def consume(self):
        if self._subscriber_state == self.SUBSCRIBER_INIT:
            LOGGER.debug('Start a redis subscription: {0}.'.format(self._key))
            self._set_subscriber_state(self.SUBSCRIBER_SUBSCRIBING)
            self._io_loop.add_future(
                gen.Task(self._subscriber.subscribe, self._key, self),
                self._update_subscribe_state)

    def cancel(self, delete_consume_callbacks=True):
        if delete_consume_callbacks:
            del self._consume_callbacks[:]
        self._subscriber.unsubscribe(self._key, self)
        self._set_subscriber_state(self.SUBSCRIBER_INIT)

    def on_message(self, msg):
        try:
            if self._consume_callbacks:
                [cb() for cb in self._consume_callbacks]
            if self._callback:
                self._callback(msg)
                LOGGER.debug('Consumed a message: {0}'.format(self._key))
        finally:
            if self._oneshot:
                self.cancel()


class RedisConsumer(TaskConsumerBase):
    def __init__(self, **kwargs):
        super(RedisConsumer, self).__init__(**kwargs)
        self.host = self.backend.connparams['host']
        self.port = self.backend.connparams['port']
        self.password = self.backend.connparams['password']
        self.selected_db = self.backend.connparams['db']
        self.redis_client = StrictRedis(self.host, self.port, self.selected_db, self.password)
        self.subscriber = self.create_redis_subscriber()

    def _wait_for(self, task_id, callback, wait_timeout):
        key = self.backend.get_key_for_task(task_id)
        consume_delegate = RedisConsumeDelegate(key, callback, self.subscriber, self.io_loop)
        if wait_timeout:
            timeout = self.io_loop.add_timeout(
                timedelta(milliseconds=wait_timeout),
                partial(consume_delegate.on_message, WaitForResultTimeoutError(wait_timeout)))
            consume_delegate.add_consume_callback(self.io_loop.remove_timeout, timeout)
        consume_delegate.add_consume_callback(self.redis_client.delete, key)
        consume_delegate.consume()

        # try to get the result before subscribed
        def _check_subscribed():
            result = self.redis_client.get(key)
            if result:
                LOGGER.info('Get the result before redis subscribed: {0}.'.format(key))
                consume_delegate.on_message(result)
            elif consume_delegate.is_subscribing:
                LOGGER.debug('Continue to check the subscriber state: {0}.'.format(key))
                # self.io_loop.add_callback(_check_subscribed)
                self.io_loop.add_timeout(timedelta(seconds=1), _check_subscribed)
            else:
                LOGGER.info('Stop checking the subscriber state: {0}.'.format(key))
        self.io_loop.add_callback(_check_subscribed)

    def create_redis_subscriber(self):
        redis_client = RedisClient(
            host=self.host,
            port=self.port,
            password=self.password,
            selected_db=self.selected_db,
            io_loop=self.io_loop)
        return SocketIOSubscriber(redis_client)
