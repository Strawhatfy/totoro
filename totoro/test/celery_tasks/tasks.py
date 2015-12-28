# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
import time

from celery import Celery


celery = Celery("totoro_celery_tasks")
celery.conf.update(
    BROKER_URL=os.environ.get('BROKER_URL', 'amqp://guest:guest@localhost:5672/%2F'),
    CELERY_RESULT_BACKEND=os.environ.get('CELERY_RESULT_BACKEND', 'amqp://guest:guest@localhost:5672/%2F'),
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_TIMEZONE='Europe/Oslo',
    CELERY_ENABLE_UTC=True,
)


@celery.task
def add(x, y=1):
    return int(x) + int(y)


@celery.task
def sleep(seconds):
    time.sleep(float(seconds))
    return seconds


class TaskExecError(Exception):
    pass


@celery.task
def error(msg):
    raise TaskExecError(msg)