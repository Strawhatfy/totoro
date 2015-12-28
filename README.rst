Totoro
======
.. image:: https://img.shields.io/pypi/v/totoro.svg
    :target: https://pypi.python.org/pypi/totoro

.. image:: https://img.shields.io/pypi/dm/totoro.svg
    :target: https://pypi.python.org/pypi/totoro

Celery integration with Tornado.

Installation
------------

You can install Totoro either via the Python Package Index (PyPI) or from source.

To install using pip, simply:

.. code-block:: bash

    $ sudo pip install totoro

or alternatively (you really should be using pip though):

.. code-block:: bash

    $ sudo easy_install totoro

or from source:

.. code-block:: bash

    $ sudo python setup.py install

Totoro can only support AMQP by default. To use the redis, you can specify the requirements on the pip comand-line by using brackets.

.. code-block:: bash

    $ sudo pip install totoro[redis]

Hello, world
------------

Here is a simple "Hello, world!" example for calling celery tasks from Tornado RequestHandler:

.. code-block:: python

    #!/usr/bin/env python
    
    import tornado.httpserver
    import tornado.ioloop
    import tornado.web
    
    from tornado import gen
    import totoro
    from totoro.test.celery_tasks import tasks
    
    
    class MainHandler(tornado.web.RequestHandler):
        @gen.coroutine
        def get(self):
            response = yield gen.Task(tasks.echo.apply_async, args=['Hello world!'])
            self.write(response.result)
    
    
    def main():
        totoro.setup_producer()
        application = tornado.web.Application([
            (r"/", MainHandler),
        ])
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(8888)
        tornado.ioloop.IOLoop.instance().start()
    
    
    if __name__ == "__main__":
        main()

To run celery worker for the example:

.. code-block:: bash

    $ python -m totoro.test.runtasks worker -l INFO


Tests
-----

To run the tests for the AMQP(broker/backend):

.. code-block:: bash

    $ python -m totoro.test.runtasks worker -l INFO
    $ python -m totoro.test.runtests

To run the tests for the AMQP broker with the Redis backend:

.. code-block:: bash

    $ CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/0  python -m totoro.test.runtasks worker -l INFO
    $ CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/0  python -m totoro.test.runtests

To run the tests for the Redis(broker/backend):

.. code-block:: bash

    $ BROKER_URL=redis://127.0.0.1:6379/0  CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/0 python -m totoro.test.runtasks worker -l INFO
    $ BROKER_URL=redis://127.0.0.1:6379/0  CELERY_RESULT_BACKEND=redis://127.0.0.1:6379/0 python -m totoro.test.runtests

