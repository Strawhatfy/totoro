# -*- coding: utf-8 -*-

from __future__ import absolute_import

import sys
import logging
import time
import uuid
import traceback
from collections import deque
from datetime import timedelta
from functools import partial
try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote     # noqa

import pika
import pika.adapters
from tornado import ioloop, gen
from tornado.concurrent import Future

from totoro.base import (
    TaskProducerAdapter,
    TaskPublishDelegate,
    TaskConsumerBase,
    WaitForResultTimeoutError)

LOGGER = logging.getLogger(__name__)


def generate_consumer_tag():
    return 'totoro_tag_0.{0}'.format(uuid.uuid4().get_hex())


class Connection(object):
    """"""
    def __init__(self, url, io_loop=None):
        self._url = url
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection = None
        self._channel = None
        self._connected_time = None
        self._consumers = dict()
        self._waiting_callers = list()
        self._connected_future = None
        self._closed_future = Future()
        self._closed_future.set_result(self)

    @property
    def connected_time(self):
        return self._connected_time

    @property
    def is_ready(self):
        return self._channel and self._channel.is_open

    @property
    def is_idle(self):
        return len(self._consumers) == 0 and len(self._waiting_callers) == 0

    def queue_declare(self, callback, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        if callback and nowait is True:
            raise ValueError('Can not pass a callback if nowait is True')

        if self.is_ready:
            LOGGER.debug('queue_declare: queue={0}'.format(queue))
            self._channel.queue_declare(callback, queue, passive, durable, exclusive,
                                        auto_delete, nowait, arguments)
        else:
            self._waiting_callers.append(partial(self.queue_declare, callback, queue, passive,
                                                 durable, exclusive, auto_delete, nowait, arguments))

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        if self.is_ready:
            LOGGER.debug('basic_publish: exchange={0} routing_key={1}'.format(exchange, routing_key))
            self._channel.basic_publish(exchange, routing_key, body,
                                        properties, mandatory, immediate)
        else:
            self._waiting_callers.append(partial(self.basic_publish, exchange, routing_key, body, properties,
                                                 mandatory, immediate))

    def basic_consume(self, consumer_callback, queue='', no_ack=False,
                      exclusive=False, consumer_tag=None, arguments=None):
        consumer_tag = consumer_tag or generate_consumer_tag()
        if not self.is_ready:
            self._waiting_callers.append(partial(self.basic_consume, consumer_callback, queue, no_ack,
                                                 exclusive, consumer_tag, arguments))
        else:
            LOGGER.debug('basic_consume: queue={0} consumer_tag={1}'.format(queue, consumer_tag))
            consumer_tag = self._channel.basic_consume(
                consumer_callback, queue, no_ack,
                exclusive, consumer_tag, arguments)
            self._consumers[consumer_tag] = dict(
                consumer_callback=consumer_callback,
                queue=queue, no_ack=no_ack,
                exclusive=exclusive, arguments=arguments)
        return consumer_tag

    def basic_cancel(self, callback=None, consumer_tag='', nowait=False):
        LOGGER.debug('enter basic_cancel: consumer_tag={0}'.format(consumer_tag))
        if callback and nowait is True:
            raise ValueError('Can not pass a callback if nowait is True')

        if not self.is_ready:
            self._waiting_callers.append(partial(self.basic_cancel, callback, consumer_tag, nowait))
            return
        if consumer_tag not in self._consumers:
            LOGGER.info('Invalid consumer_tag:{0}'.format(consumer_tag))
            return
        cb = callback
        if nowait:
            del self._consumers[consumer_tag]
        else:
            def callback_wrapper(method_frame):
                del self._consumers[consumer_tag]
                if callback:
                    callback(method_frame)
            cb = callback_wrapper
        self._channel.basic_cancel(cb, consumer_tag, nowait)

    def connect(self):
        if self._connected_future is None:
            self._connected_future = Future()
            self._connect()
        return self._connected_future

    def close(self):
        if self._closed_future is None:
            self._closed_future = Future()
            self._close()
        return self._closed_future

    def _close(self):
        LOGGER.info('Closing connection.')
        self._connection.close()

    def _connect(self):
        LOGGER.info('Connecting to %s.', self._url)
        self._connection = pika.adapters.TornadoConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self._on_connection_open,
            custom_ioloop=self._io_loop)

    def _on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened.')
        LOGGER.info('Adding connection close callback.')
        self._connection.add_on_close_callback(self._on_connection_closed)

        LOGGER.info('Creating a new channel.')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        self._connection = None
        self._channel = None
        if self._closed_future is None:
            LOGGER.warning('Connection closed, reopening in 2 seconds: (%s) %s.', reply_code, reply_text)
            self._io_loop.add_timeout(2, self._connect())
        else:
            LOGGER.info('Connection closed, setting the result of a `self._closed_future`.')
            self._closed_future.set_result(self)
            self._connected_future = None

    def _on_channel_open(self, channel):
        LOGGER.info('Channel opened.')
        self._channel = channel
        self._connected_time = time.time()

        LOGGER.info('Adding channel close callback.')
        self._channel.add_on_close_callback(self._on_channel_closed)

        LOGGER.info('Try to restore previous channel state.')
        self._restore()

        if self._connected_future:
            LOGGER.info('Channel opened, setting the result of a `self._connected_future`.')
            self._connected_future.set_result(self)
            self._closed_future = None

    def _on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning('Channel %i was closed: (%s) %s.', channel, reply_code, reply_text)
        self._channel = None
        self._connection.close()

    def _restore(self):
        if self._waiting_callers:
            LOGGER.info('restore waiting callers: {0}'.format(len(self._waiting_callers)))
            waiting_callers = self._waiting_callers
            self._waiting_callers = list()
            for caller in waiting_callers:
                try:
                    caller()
                except:
                    tp, value, tb = sys.exc_info()
                    LOGGER.warning(''.join([line.decode("unicode-escape")
                                            for line in traceback.format_exception(tp, value, tb)]))

        if self._consumers:
            LOGGER.info('restore consumers: {0} - [{1}]'.format(
                len(self._consumers), ','.join(self._consumers.keys())))
            consumers = self._consumers
            self._consumers = dict()
            for consumer_tag, consumer_args in consumers.items():
                try:
                    self.basic_consume(consumer_tag=consumer_tag, **consumer_args)
                except:
                    LOGGER.warning('restore: call basic_consume(consumer_tag={0}) fails.'.format(consumer_tag))
                    tp, value, tb = sys.exc_info()
                    LOGGER.warning(''.join([line.decode("unicode-escape")
                                            for line in traceback.format_exception(tp, value, tb)]))


class ConnectionPool(object):
    """"""
    CONN_OPTIONS_NAMES = (
        'max_idle_connections',
        'max_recycle_sec',
        'max_open_connections')

    @classmethod
    def get_conn_options(cls, **kwargs):
        conn_options = TaskProducerAdapter.app.conf.get('TOTORO_AMQP_CONNECTION_POOL', dict())
        current_conn_options = dict()
        if isinstance(conn_options, dict):
            for n in cls.CONN_OPTIONS_NAMES:
                if n in conn_options:
                    current_conn_options[n] = int(conn_options[n])
                elif n in kwargs:
                    current_conn_options[n] = kwargs[n]
        else:
            LOGGER.warning('Invalid conn_options: {0}'.format(conn_options))
        LOGGER.info('ConnectionPool - current_conn_options: {0}'.format(
            ', '.join(['{0}={1}'.format(k, v) for k, v in current_conn_options.items()])))
        return current_conn_options

    @staticmethod
    def get_url():
        parts = list(TaskProducerAdapter.app.connection().as_uri(
            include_password=True).partition('://'))
        parts.extend(parts.pop(-1).partition('/'))
        parts[-1] = quote(parts[-1], safe='')
        return ''.join(str(part) for part in parts if part)

    @staticmethod
    def instance():
        if not hasattr(ConnectionPool, '_instance'):
            ConnectionPool._instance = ConnectionPool(
                ConnectionPool.get_url(),
                **ConnectionPool.get_conn_options(max_idle_connections=3, max_open_connections=10))
        return ConnectionPool._instance

    def __init__(self, url,
                 max_idle_connections=1,
                 max_recycle_sec=3600,
                 max_open_connections=0,
                 io_loop=None):
        self.url = url
        self.max_idle = max_idle_connections
        self.max_open = max_open_connections
        self.max_recycle_sec = max_recycle_sec
        self.io_loop = io_loop or ioloop.IOLoop.instance()

        self._opened_conns = 0
        self._free_conn = deque()
        self._waitings = deque()

    def stat(self):
        """Returns (opened connections, free connections, waiters)"""
        return self._opened_conns, len(self._free_conn), len(self._waitings)

    def get_connection(self):
        now = self.io_loop.time()

        # Try to reuse in free pool
        while self._free_conn:
            conn = self._free_conn.popleft()
            if conn.is_idle and (now - conn.connected_time) > self.max_recycle_sec:
                self._close_async(conn)
                continue
            LOGGER.debug("Reusing connection from pool: %s", self.stat())
            future = Future()
            future.set_result(conn)
            return future

        # Open new connection
        if self.max_open == 0 or self._opened_conns < self.max_open:
            self._opened_conns += 1
            LOGGER.info("Creating new connection: %s", self.stat())
            conn = Connection(self.url, self.io_loop)
            return conn.connect()

        # Wait to other connection is released.
        future = Future()
        self._waitings.append(future)
        return future

    def put_connection(self, conn):
        if (not conn.is_idle
            or (len(self._free_conn) < self.max_idle
                and (self.io_loop.time() - conn.connected_time) < self.max_recycle_sec)):
            if self._waitings:
                fut = self._waitings.popleft()
                fut.set_result(conn)
                LOGGER.debug("Passing returned connection to waiter: %s", self.stat())
            else:
                LOGGER.info("Add connection to free pool: %s", self.stat())
                self._free_conn.append(conn)
                max_close = len(self._free_conn) - self.max_idle
                if max_close > 0:
                    for _ in xrange(0, len(self._free_conn)):
                        conn = self._free_conn.popleft()
                        if conn.is_idle:
                            self._close_async(conn)
                            max_close -= 1
                            if max_close <= 0:
                                break
                        else:
                            self._free_conn.append(conn)
        else:
            self._close_async(conn)

    def _close_async(self, conn):
        self.io_loop.add_future(conn.close(), callback=self._after_close)

    def _after_close(self, future):
        if self._waitings:
            conn = future.result()
            future = self._waitings.popleft()
            self.io_loop.add_future(conn.connect(), callback=lambda f: future.set_result(conn))
        else:
            self._opened_conns -= 1
        LOGGER.info("Connection closed: %s", self.stat())


class AMQPConsumer(TaskConsumerBase):
    """"""
    def __init__(self, **kwargs):
        super(AMQPConsumer, self).__init__(**kwargs)
        self._connection_pool = ConnectionPool.instance()

    @gen.coroutine
    def _consume(self, task_id, callback, wait_timeout):
        conn = yield self._connection_pool.get_connection()
        timeout = None
        consumer_tag = generate_consumer_tag()
        consume_future = Future()

        def _basic_cancel():
            conn.basic_cancel(consumer_tag=consumer_tag)
            consume_future.set_result(None)

        if wait_timeout:
            def _on_timeout():
                _basic_cancel()
                callback(WaitForResultTimeoutError(wait_timeout))
            timeout = self.io_loop.add_timeout(timedelta(milliseconds=wait_timeout), _on_timeout)

        try:
            def _on_result(reply):
                if timeout:
                    self.io_loop.remove_timeout(timeout)
                _basic_cancel()
                callback(reply)

            name = self.backend.rkey(task_id)
            queue_declare_future = Future()
            conn.queue_declare(lambda method_frame: queue_declare_future.set_result(None),
                               queue=name,
                               auto_delete=self.backend.auto_delete,
                               durable=self.backend.persistent,
                               arguments=self.backend.queue_arguments)
            yield queue_declare_future
            conn.basic_consume(
                consumer_callback=lambda channel, deliver, properties, reply: _on_result(reply),
                queue=name, consumer_tag=consumer_tag)
        finally:
            self._connection_pool.put_connection(conn)
            yield consume_future

    def _wait_for(self, task_id, callback, wait_timeout):
        def tracking(fut=None):
            if fut:
                fut.result()
            LOGGER.debug('Task({0}) is completed.'.format(task_id))
        future = self._consume(task_id, callback, wait_timeout)
        if not future.done():
            self.io_loop.add_future(future, lambda f: tracking(f))
        else:
            tracking()


class AMQPTaskPublishDelegate(TaskPublishDelegate):
    """"""
    def __init__(self, task_producer):
        super(AMQPTaskPublishDelegate, self).__init__(task_producer)
        self._connection_pool = ConnectionPool.instance()

    @gen.coroutine
    def _publish(self, body, priority, content_type, content_encoding,
                 headers, routing_key, mandatory, immediate, exchange,
                 retry, retry_policy, **properties):
        conn = yield self._connection_pool.get_connection()
        try:
            properties = pika.BasicProperties(
                content_type=content_type,
                content_encoding=content_encoding,
                headers=headers,
                priority=priority,
                **properties
            )
            conn.basic_publish(
                exchange=exchange, routing_key=routing_key,
                body=body, properties=properties,
                mandatory=mandatory, immediate=immediate)
        finally:
            self._connection_pool.put_connection(conn)

    def publish(self, task_id, body, priority, content_type, content_encoding,
                headers, routing_key, mandatory, immediate, exchange,
                declare, retry, retry_policy, **properties):
        if declare:
            maybe_declare = self.task_producer.maybe_declare
            [maybe_declare(entity) for entity in declare]

        future = self._publish(body, priority, content_type, content_encoding,
                               headers, routing_key, mandatory, immediate, exchange,
                               retry, retry_policy, **properties)

        def tracking(fut=None):
            if fut:
                fut.result()
            LOGGER.debug('Task({0}) is published.'.format(task_id))
        if not future.done():
            self.task_producer.io_loop.add_future(future, lambda f: tracking(f))
        else:
            tracking()
