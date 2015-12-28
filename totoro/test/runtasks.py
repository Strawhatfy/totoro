# -*- coding: utf-8 -*-

from __future__ import absolute_import

from totoro.test.celery_tasks import tasks

# CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/1 python -m totoro.test.runtasks worker -l INFO
if __name__ == '__main__':
    tasks.celery.start()
