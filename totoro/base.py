# -*- coding: utf-8 -*-

from __future__ import absolute_import

import numbers
import inspect
import logging
from functools import partial

import celery.result
from kombu.utils import cached_property
from kombu.entity import Exchange, DELIVERY_MODES
from celery.app.amqp import TaskProducer
from celery.states import EXCEPTION_STATES
from celery.exceptions import TimeoutError
from celery.backends.base import BaseBackend

LOGGER = logging.getLogger(__name__)


class WaitForResultTimeoutError(TimeoutError):
    def __init__(self, wait_timeout, *args, **kwargs):
        super(WaitForResultTimeoutError, self).__init__(
            'Wait timeout of {0}ms expired.'.format(wait_timeout), *args, **kwargs)


class AsyncResult(celery.result.AsyncResult):
    """"""
    def __init__(self, task_id, **kwargs):
        self._wait_timeout_error = kwargs.pop('wait_timeout_error', None)
        self._status = kwargs.pop('status', None)
        self._traceback = kwargs.pop('traceback', None)
        self._result = kwargs.pop('result', None)
        super(AsyncResult, self).__init__(task_id)

    @property
    def status(self):
        return self._status or super(AsyncResult, self).status
    state = status

    @property
    def traceback(self):
        return self._traceback or super(AsyncResult, self).traceback

    @property
    def result(self):
        if self._wait_timeout_error:
            raise self._wait_timeout_error
        if self.status in EXCEPTION_STATES:
            raise self._result
        return self._result or super(AsyncResult, self).result


class TaskConsumerBase(object):
    """"""
    def __init__(self, io_loop, app, result_cls):
        self.io_loop = io_loop
        self.app = app
        self.backend = self.app.backend
        self.result_cls = result_cls

    def _wait_for(self, task_id, callback, wait_timeout):
        raise NotImplementedError()

    def wait_for(self, task_id, callback, wait_timeout):
        callback = partial(self.on_result, task_id, callback)
        return self._wait_for(task_id, callback, wait_timeout)

    def on_result(self, task_id, callback, reply):
        if isinstance(reply, WaitForResultTimeoutError):
            reply = dict(wait_timeout_error=reply)
        else:
            reply = self.backend.decode_result(reply)
        reply['task_id'] = task_id
        result = self.result_cls(**reply)
        callback(result)


class TaskPublishDelegate(object):
    """"""
    def __init__(self, task_producer):
        self.task_producer = task_producer

    def publish(self, task_id, body, priority, content_type, content_encoding,
                headers, routing_key, mandatory, immediate, exchange,
                declare, retry, retry_policy, **properties):
        raise NotImplementedError()


class TaskProducerAdapter(TaskProducer):
    """"""
    _CONSUMERS = dict()
    task_publish_delegate = None
    result_cls = AsyncResult
    io_loop = None

    @staticmethod
    def register_consumer(backend_type, consumer_type):
        if not inspect.isclass(backend_type) or not issubclass(backend_type, BaseBackend):
            raise ValueError('`backend_type` must be a subclass of BaseBackend.')

        if not inspect.isclass(consumer_type) or not issubclass(consumer_type, TaskConsumerBase):
            raise ValueError('`consumer_type` must be a subclass of TaskConsumerBase.')

        TaskProducerAdapter._CONSUMERS.setdefault(backend_type, consumer_type)

    def __init__(self, channel=None, *args, **kwargs):
        super(TaskProducerAdapter, self).__init__(channel, *args, **kwargs)

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=None, expiration=None, **properties):
        headers = {} if headers is None else headers
        retry_policy = {} if retry_policy is None else retry_policy
        routing_key = self.routing_key if routing_key is None else routing_key
        compression = self.compression if compression is None else compression
        exchange = exchange or self.exchange
        declare = declare or []

        callback = properties.pop('callback', None)
        wait_timeout = int(properties.pop('wait_timeout', 86400)) * 1000
        task_id = body['id']

        if callback and not callable(callback):
            raise ValueError('Callback should be callable.')

        if callback and not isinstance(self.app.backend, tuple(self._CONSUMERS.keys())):
            raise ValueError('Callback can be used only with [{0}] backend.'.format(
                ','.join(map(lambda x: x.__name__, self._CONSUMERS.keys()))))

        if isinstance(exchange, Exchange):
            delivery_mode = delivery_mode or exchange.delivery_mode
            exchange = exchange.name
        else:
            delivery_mode = delivery_mode or self.exchange.delivery_mode
        if not isinstance(delivery_mode, numbers.Integral):
            delivery_mode = DELIVERY_MODES[delivery_mode]
        properties['delivery_mode'] = delivery_mode
        if expiration is not None:
            properties['expiration'] = str(int(expiration*1000))

        body, content_type, content_encoding = self._prepare(
            body, serializer, content_type, content_encoding,
            compression, headers)

        publish = self.publish_delegate
        result = publish(task_id, body, priority, content_type, content_encoding, headers,
                         routing_key, mandatory, immediate, exchange, declare,
                         retry, retry_policy, **properties)

        if callback:
            self.consumer.wait_for(
                task_id=task_id,
                callback=callback,
                wait_timeout=wait_timeout)
        return result

    def _publish_default(self, task_id, body, priority, content_type, content_encoding,
                         headers, routing_key, mandatory, immediate, exchange,
                         declare, retry, retry_policy, **properties):
        LOGGER.debug('Enter _publish_default() method: {0}'.format(task_id))
        publish = self._publish
        if retry:
            publish = self.connection.ensure(self, publish, **retry_policy)
        return publish(body, priority, content_type, content_encoding,
                       headers, properties, routing_key, mandatory,
                       immediate, exchange, declare)

    @cached_property
    def consumer(self):
        LOGGER.debug('The current backend is {0}.'.format(type(self.app.backend).__name__))
        consumer = self._CONSUMERS.get(type(self.app.backend), None)
        if not consumer:
            raise ValueError('Registered customers can\'t support the backend: {0}.'.format(
                type(self.app.backend).__name__))
        return consumer(io_loop=self.io_loop, app=self.app, result_cls=self.result_cls)

    @cached_property
    def publish_delegate(self):
        if self.task_publish_delegate is None:
            LOGGER.debug('Using default publish delegate.')
            publish = self._publish_default
        else:
            LOGGER.debug('Using the custom publish delegate: {0}.'.format(self.task_publish_delegate.__name__))
            publish = self.task_publish_delegate(self).publish
        return publish