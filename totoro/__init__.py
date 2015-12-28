# -*- coding: utf-8 -*-

from __future__ import absolute_import
import logging

import tornado.ioloop
import celery.app.amqp
from celery.backends.amqp import AMQPBackend
from celery.backends.redis import RedisBackend

import totoro.base
import totoro.amqp
import totoro.redis

__version__ = '0.1.0'
VERSION = tuple(map(int, __version__.split('.')))

try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

register_consumer = totoro.base.TaskProducerAdapter.register_consumer


def _setup_producer(celery_app, io_loop, task_publish_delegate, result_cls):
    producer_cls = totoro.base.TaskProducerAdapter
    producer_cls.app = celery_app
    producer_cls.io_loop = io_loop
    producer_cls.task_publish_delegate = task_publish_delegate
    producer_cls.result_cls = result_cls

    producer_cls.register_consumer(AMQPBackend, totoro.amqp.AMQPConsumer)
    producer_cls.register_consumer(RedisBackend, totoro.redis.RedisConsumer)

    celery.app.amqp.AMQP.producer_cls = producer_cls


def setup_producer(celery_app=None, io_loop=None, result_cls=None, task_publish_delegate=None):
    celery_app = celery_app or celery.current_app
    io_loop = io_loop or tornado.ioloop.IOLoop.instance()
    result_cls = result_cls or totoro.base.AsyncResult

    if (celery_app.conf.BROKER_URL[0:3] == 'redis'
            and celery_app.conf.CELERY_RESULT_BACKEND[0:3] != 'redis'):
        raise ValueError('Redis broker only supports the redis result backend.')

    if (celery_app.conf.BROKER_URL
            and celery_app.conf.BROKER_URL[0:4] == 'amqp'
            and task_publish_delegate is None):
        task_publish_delegate = totoro.amqp.AMQPTaskPublishDelegate

    _setup_producer(celery_app, io_loop, task_publish_delegate, result_cls)