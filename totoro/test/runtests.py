from __future__ import absolute_import, print_function

import unittest

from tornado import gen
from tornado import ioloop
from totoro.test.celery_tasks import tasks
import totoro


class TestCase(unittest.TestCase):
    def setUp(self):
        totoro.setup_producer()

    def test_args(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            self.assertEquals(3, response.result)
        future = gen.Task(tasks.add.apply_async, args=[1, 2])
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_kwargs(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            self.assertEquals(3, response.result)
        future = gen.Task(tasks.add.apply_async, kwargs=dict(x=1, y=2))
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_args_and_kwargs(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            self.assertEquals(3, response.result)
        future = gen.Task(tasks.add.apply_async, args=[1], kwargs=dict(y=2))
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_eta(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            self.assertEquals(3, response.result)
        future = gen.Task(tasks.add.apply_async,
                          args=[1, 2],
                          **{'countdown': 3})
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_raise_task_exec_exception(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            self.assertRaises(tasks.TaskExecError, lambda: response.result)
        future = gen.Task(tasks.error.apply_async, args=['TaskExecError'])
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_raise_wait_timeout_error(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            from celery.exceptions import TimeoutError
            self.assertRaises(TimeoutError, lambda: response.result)
        future = gen.Task(tasks.sleep.apply_async,
                          args=[2],
                          **{'wait_timeout': 1})
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_raise_revoke_error(self):
        io_loop = ioloop.IOLoop.instance()

        def done(response):
            io_loop.stop()
            from celery.exceptions import TaskRevokedError
            self.assertRaises(TaskRevokedError, lambda: response.result)
        future = gen.Task(tasks.add.apply_async,
                          args=[1, 2],
                          **{'expires': 1, 'countdown': 2})
        io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

    def test_multiple_task(self):
        io_loop = ioloop.IOLoop.instance()
        total_number = 100
        current_count = [0]

        def done(response):
            current_count[0] += 1
            if current_count[0] == total_number:
                io_loop.stop()
            self.assertEqual(3, response.result)

        for _ in xrange(0, total_number):
            future = gen.Task(tasks.add.apply_async, args=[1, 2])
            io_loop.add_future(future, lambda f: done(f.result()))
        io_loop.start()

# CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/1 python -m totoro.test.runtests -l WARNING
if __name__ == "__main__":
    import sys
    import logging

    LOG_LEVELS = dict()
    LOG_LEVELS.setdefault('DEBUG', logging.DEBUG)
    LOG_LEVELS.setdefault('INFO', logging.INFO)
    LOG_LEVELS.setdefault('WARN', logging.WARNING)
    LOG_LEVELS.setdefault('WARNING', logging.WARNING)
    LOG_LEVELS.setdefault('ERROR', logging.ERROR)
    LOG_LEVELS.setdefault('FATAL', logging.CRITICAL)
    LOG_LEVELS.setdefault('CRITICAL', logging.CRITICAL)
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -20s %(funcName) '
                  '-25s %(lineno) -5d: %(message)s')

    if len(sys.argv) > 1 and sys.argv[1] in ('-l', '--level'):
        del sys.argv[1]
        level = sys.argv.pop(1)
    else:
        level = 'WARNING'

    logging.basicConfig(format=LOG_FORMAT)
    logging.getLogger('totoro').setLevel(LOG_LEVELS.get(level, logging.WARNING))

    unittest.main()